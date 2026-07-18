import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import { createClient } from '@supabase/supabase-js';
import { MercadoPagoConfig, Preference, Payment, WebhookSignatureValidator } from 'mercadopago';
import multer from 'multer';
import { PDFDocument } from 'pdf-lib';
import { exec } from 'child_process';
import { promisify } from 'util';
import { readFile, rm } from 'fs/promises';
import { mkdtempSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';
import PQueue from 'p-queue';

const execAsync = promisify(exec);
// soffice en PATH en Linux/Docker; en Windows ajustar si es necesario
const SOFFICE = process.platform === 'win32'
  ? '"C:\\Program Files\\LibreOffice\\program\\soffice.exe"'
  : 'soffice';

// Cola de conversión — concurrency: 1 hoy, listo para escalar (solo cambiar el número).
// El timeout lo maneja execAsync (mata el proceso hijo con SIGKILL),
// no p-queue: así un soffice colgado no acumula procesos zombie en background.
// Si una tarea falla o hace timeout, p-queue libera el slot y la cola sigue sola.
const CONVERSION_TIMEOUT_MS = 60_000;
const conversionQueue = new PQueue({ concurrency: 1 });

// diskStorage: escribe el archivo directamente a disco en vez de mantenerlo en RAM.
// Necesario para archivos grandes (PDFs con imágenes, presentaciones, etc.).
const upload = multer({
  storage: multer.diskStorage({
    destination(req, file, cb) {
      try {
        const dir = mkdtempSync(join(tmpdir(), 'pc-'));
        req._uploadTmpDir = dir; // lo usamos en el handler para cleanup y conversión
        cb(null, dir);
      } catch (err) { cb(err); }
    },
    filename(req, file, cb) { cb(null, file.originalname); },
  }),
  limits: { fileSize: 300 * 1024 * 1024 },
});

const app = express();
app.use(express.json());
// origin: '*' es suficiente para desarrollo local (archivo HTML abierto con file://)
// En producción reemplazar con el dominio real del sitio
app.use(cors({ origin: '*' }));

// service_role bypasea RLS — nunca expongas esta key al cliente
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// En producción usar el access token de producción (no el de prueba)
const mpClient = new MercadoPagoConfig({
  accessToken: process.env.MP_ACCESS_TOKEN,
});
const preferenceClient = new Preference(mpClient);
const paymentClient = new Payment(mpClient);

// ── Health check ──────────────────────────────────────────────────────────────
app.get('/health', (_req, res) => res.json({ ok: true }));

// ── Procesar archivo: convertir a PDF (LibreOffice) + contar páginas + guardar en Storage ──
const IMAGENES = ['jpg','jpeg','png','webp','gif','bmp','tiff'];
const BUCKET   = 'archivos-pedidos';

function sanitizeKey(name) {
  return name
    .normalize('NFD')
    .replace(/[^\x00-\x7F]/g, '')      // elimina tildes/diacríticos (no-ASCII post-NFD)
    .replace(/[^a-zA-Z0-9.\-_]/g, '_') // espacios y otros chars inválidos → _
    .replace(/_+/g, '_')               // colapsar guiones bajos consecutivos
    .replace(/^_|_$/g, '');            // trim
}

app.post('/procesar-archivo', (req, res) => {
  upload.single('archivo')(req, res, async (err) => {
    if (err) {
      const msg = err.code === 'LIMIT_FILE_SIZE'
        ? 'El archivo es demasiado grande (máximo 150 MB)'
        : (err.message || 'Error al recibir el archivo');
      return res.status(400).json({ error: msg });
    }
    return procesarArchivoHandler(req, res);
  });
});

async function procesarArchivoHandler(req, res) {
  if (!req.file) return res.status(400).json({ error: 'No se recibió ningún archivo' });
  const { originalname, path: inputPath } = req.file;
  const pedidoId = req.body.pedidoId;
  if (!pedidoId) return res.status(400).json({ error: 'pedidoId requerido' });

  const ext    = originalname.split('.').pop().toLowerCase();
  const isPDF  = ext === 'pdf';
  const tmpDir = req._uploadTmpDir; // creado por diskStorage destination

  try {
    let pdfBuffer, pages, convertedName, pdfBase64;

    if (isPDF) {
      // PDF: leer del disco para contar páginas y subir tal cual
      pdfBuffer     = await readFile(inputPath);
      convertedName = originalname;
      const doc     = await PDFDocument.load(pdfBuffer, { ignoreEncryption: true });
      pages         = doc.getPageCount();
    } else {
      // docx, xlsx, pptx, imágenes → LibreOffice convierte a PDF via cola de conversión.
      // La cola garantiza que un fallo o timeout no bloquea las siguientes conversiones.
      convertedName = originalname.replace(/\.[^.]+$/, '.pdf');
      const outPath = join(tmpDir, convertedName);
      await conversionQueue.add(async () => {
        const loProfile = join(tmpDir, 'lo-profile').replace(/\\/g, '/');
        // timeout en execAsync mata el proceso soffice hijo con SIGKILL si se agota
        await execAsync(
          `${SOFFICE} --headless --norestore -env:UserInstallation=file:///${loProfile} --convert-to pdf --outdir "${tmpDir}" "${inputPath}"`,
          { timeout: CONVERSION_TIMEOUT_MS }
        );
      });
      pdfBuffer = await readFile(outPath);
      const doc = await PDFDocument.load(pdfBuffer, { ignoreEncryption: true });
      pages     = doc.getPageCount();
      // Devolver el PDF al cliente para la vista previa
      pdfBase64 = pdfBuffer.toString('base64');
    }

    // Subir a Supabase Storage — el path debe ser URL-safe
    const storagePath = `${pedidoId}/${sanitizeKey(convertedName)}`;
    const { error: uploadErr } = await supabase.storage
      .from(BUCKET)
      .upload(storagePath, pdfBuffer, { contentType: 'application/pdf', upsert: true });
    if (uploadErr) {
      console.error('Storage upload error:', uploadErr);
      return res.status(500).json({ error: 'No se pudo guardar el archivo' });
    }

    const response = { ok: true, pages, storageUrl: storagePath, convertedName };
    if (pdfBase64) response.pdfBase64 = pdfBase64;
    return res.json(response);

  } catch (err) {
    console.error('procesar-archivo error:', err);
    return res.status(500).json({ error: err.message || 'Error al procesar el archivo' });
  } finally {
    rm(tmpDir, { recursive: true, force: true }).catch(() => {});
  }
}

// ── Borrar archivo de Storage (llamado desde removeFile() — fire-and-forget) ──
app.delete('/procesar-archivo', async (req, res) => {
  const { path: storagePath } = req.body;
  if (!storagePath) return res.status(400).json({ error: 'path requerido' });
  const { error } = await supabase.storage.from(BUCKET).remove([storagePath]);
  if (error) return res.status(500).json({ error: error.message });
  return res.json({ ok: true });
});

// ── Limpieza de archivos huérfanos en Storage (llamado por cron externo) ─────
const ORPHAN_MAX_AGE_MS = 48 * 60 * 60 * 1000;

// storage.list() pagina de a `limit` — hay que recorrer todas las páginas,
// nunca asumir que el bucket entero entra en una sola llamada.
async function listAllStorage(path) {
  const all = [];
  const pageSize = 100;
  let offset = 0;
  while (true) {
    const { data, error } = await supabase.storage
      .from(BUCKET)
      .list(path, { limit: pageSize, offset });
    if (error) throw error;
    all.push(...data);
    if (data.length < pageSize) break;
    offset += pageSize;
  }
  return all;
}

app.post('/admin/limpiar-huerfanos', async (req, res) => {
  const secret = req.headers['x-cron-secret'];
  if (!secret || secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'No autorizado' });
  }

  const resultado = { carpetasRevisadas: 0, carpetasEliminadas: 0, archivosEliminados: 0, errores: [] };

  try {
    const carpetas = await listAllStorage(''); // cada entrada es un pedidoId (carpeta de primer nivel)

    for (const carpeta of carpetas) {
      const pedidoId = carpeta.name;
      resultado.carpetasRevisadas++;

      try {
        const { data: pedido, error: pedidoErr } = await supabase
          .from('pedidos')
          .select('estado, created_at')
          .eq('pedido_id', pedidoId)
          .maybeSingle();
        if (pedidoErr) throw pedidoErr;

        const esHuerfano = !pedido
          || (pedido.estado === 'pendiente' && (Date.now() - new Date(pedido.created_at).getTime()) > ORPHAN_MAX_AGE_MS);
        if (!esHuerfano) continue;

        const archivos = await listAllStorage(pedidoId);
        if (archivos.length === 0) continue;

        const rutas = archivos.map(a => `${pedidoId}/${a.name}`);
        const { error: removeErr } = await supabase.storage.from(BUCKET).remove(rutas);
        if (removeErr) throw removeErr;

        resultado.carpetasEliminadas++;
        resultado.archivosEliminados += rutas.length;
      } catch (err) {
        resultado.errores.push({ pedidoId, error: err.message || String(err) });
      }
    }

    return res.json(resultado);
  } catch (err) {
    console.error('limpiar-huerfanos error:', err);
    return res.status(500).json({ error: err.message || 'Error al limpiar huérfanos', ...resultado });
  }
});

// ── Panel de administración: ver pedidos y archivos, marcar completados ─────
async function requireAdmin(req, res, next) {
  const authHeader = req.headers['authorization'] || '';
  const token = authHeader.replace('Bearer ', '').trim();
  if (!token) return res.status(401).json({ error: 'Se requiere sesión' });

  const { data: { user }, error: authErr } = await supabase.auth.getUser(token);
  if (authErr || !user) return res.status(401).json({ error: 'Sesión inválida' });

  const { data: profile, error: profileErr } = await supabase
    .from('profiles')
    .select('is_admin')
    .eq('id', user.id)
    .single();
  if (profileErr || !profile?.is_admin) return res.status(403).json({ error: 'No autorizado' });

  next();
}

app.get('/admin/pedidos', requireAdmin, async (req, res) => {
  const { data: pedidos, error } = await supabase
    .from('pedidos')
    .select('*')
    .order('created_at', { ascending: false });
  if (error) return res.status(500).json({ error: error.message });

  const conArchivos = await Promise.all(pedidos.map(async (p) => {
    let archivosStorage = [];
    try {
      const files = await listAllStorage(p.pedido_id);
      archivosStorage = await Promise.all(files.map(async (f) => {
        const path = `${p.pedido_id}/${f.name}`;
        const { data: signed } = await supabase.storage.from(BUCKET).createSignedUrl(path, 3600);
        return { nombre: f.name, url: signed?.signedUrl || null };
      }));
    } catch (err) {
      console.error(`Error listando archivos de ${p.pedido_id}:`, err.message);
    }
    return { ...p, archivosStorage };
  }));

  return res.json({ pedidos: conArchivos });
});

app.post('/admin/pedidos/:pedidoId/completar', requireAdmin, async (req, res) => {
  const { pedidoId } = req.params;
  const { error } = await supabase
    .from('pedidos')
    .update({ estado: 'completado' })
    .eq('pedido_id', pedidoId);
  if (error) return res.status(500).json({ error: error.message });
  return res.json({ ok: true });
});

// ── Crear preferencia MP + registrar pedido en Supabase ──────────────────────
// Máximo de pedidos (ítems) que se pueden combinar en un solo carrito/pago.
// No corresponde a un límite documentado de Mercado Pago — es un resguardo
// propio de tamaño de payload/UX.
const MAX_ITEMS_CARRITO = 10;

// Reparte un descuento entre varios ítems, proporcional al peso de cada
// uno en el subtotal combinado. El ÚLTIMO ítem absorbe el resto exacto
// (evita drift de redondeo: la suma siempre da subtotalImpTotal - descuento).
// Cada precio queda como mínimo en 1 — Mercado Pago rechaza unit_price <= 0
// (confirmado: "invalid_items · unit_price invalid").
function distribuirDescuento(pedidos, descuentoTotal, subtotalImpTotal) {
  if (descuentoTotal <= 0) return pedidos.map(p => p.subtotalImp);
  let acumulado = 0;
  return pedidos.map((p, i) => {
    if (i === pedidos.length - 1) {
      return Math.max(1, p.subtotalImp - (descuentoTotal - acumulado));
    }
    const share = Math.round(descuentoTotal * (p.subtotalImp / subtotalImpTotal));
    acumulado += share;
    return Math.max(1, p.subtotalImp - share);
  });
}

app.post('/checkout', async (req, res) => {
  const { pedidoGrupoId, pedidos, zona, direccion, costoEnvio, email, whatsapp, codigo } = req.body;

  if (!pedidoGrupoId || !email) {
    return res.status(400).json({ error: 'pedidoGrupoId y email son obligatorios' });
  }
  if (!Array.isArray(pedidos) || pedidos.length === 0) {
    return res.status(400).json({ error: 'El carrito no tiene ningún pedido' });
  }
  if (pedidos.length > MAX_ITEMS_CARRITO) {
    return res.status(400).json({ error: `Un mismo pago admite hasta ${MAX_ITEMS_CARRITO} pedidos` });
  }
  for (const p of pedidos) {
    if (!p.pedidoId || typeof p.subtotalImp !== 'number' || p.subtotalImp <= 0) {
      return res.status(400).json({ error: 'Uno de los pedidos del carrito es inválido' });
    }
  }
  const envio = typeof costoEnvio === 'number' && costoEnvio >= 0 ? costoEnvio : 0;
  const subtotalImpTotal = pedidos.reduce((acc, p) => acc + p.subtotalImp, 0);
  const totalBase = subtotalImpTotal + envio;

  // ── Código promocional: validar y calcular descuento SERVER-SIDE ─────────
  // Nunca se confía en ningún total/descuento que mande el cliente. El
  // descuento se aplica UNA sola vez sobre el subtotal de impresión
  // COMBINADO de todos los ítems del carrito (nunca sobre el envío, y
  // nunca por ítem individual).
  let descuentoServer = 0;
  let userId = null;

  if (codigo) {
    const authHeader = req.headers['authorization'] || '';
    const token = authHeader.replace('Bearer ', '').trim();
    if (!token) return res.status(401).json({ error: 'Se requiere sesión para usar un código' });

    const { data: { user }, error: authErr } = await supabase.auth.getUser(token);
    if (authErr || !user) return res.status(401).json({ error: 'Sesión inválida' });
    userId = user.id;

    const { data: codigoData, error: codigoErr } = await supabase
      .from('codigos_promocionales')
      .select('tipo, valor')
      .eq('codigo', codigo)
      .eq('activo', true)
      .single();

    if (codigoErr || !codigoData) {
      return res.status(400).json({ error: 'Código inválido o inactivo' });
    }

    if (codigoData.tipo === 'porcentaje') {
      descuentoServer = Math.round(subtotalImpTotal * codigoData.valor / 100);
    } else {
      descuentoServer = Math.min(Number(codigoData.valor), subtotalImpTotal);
    }

    // Registrar uso ANTES de crear la preferencia MP, UNA sola vez para
    // todo el carrito (pedido_id = pedidoGrupoId, no un ítem individual).
    // Si la restricción UNIQUE (user_id, codigo) falla → código ya usado → rechazar.
    const { error: usoErr } = await supabase
      .from('codigos_usados')
      .insert({ user_id: userId, codigo, pedido_id: pedidoGrupoId });

    if (usoErr) {
      const yaUsado = usoErr.code === '23505'; // unique_violation
      return res.status(409).json({
        error: yaUsado ? 'Este código ya fue utilizado en otro pedido' : 'No se pudo registrar el código',
      });
    }

    console.log(`🏷️  Código ${codigo} aplicado | descuento server-side: $${descuentoServer}`);
  }

  const totalParaPago = Math.max(0, totalBase - descuentoServer);
  const preciosConDescuento = distribuirDescuento(pedidos, descuentoServer, subtotalImpTotal);

  const baseUrl = process.env.APP_BASE_URL;
  console.log(`📦 Creando preferencia MP | pedidoGrupoId=${pedidoGrupoId} | ${pedidos.length} ítem(s) | total=${totalParaPago}`);

  // Crear preferencia en Mercado Pago — un item por cada pedido del carrito
  // (MP soporta items[] múltiples nativamente) + uno de envío si corresponde.
  let mpPreference;
  try {
    const mpItems = pedidos.map((p, i) => ({
      id:          p.pedidoId,
      title:       pedidos.length > 1 ? `Impresión ${i + 1}/${pedidos.length} — Punto Color` : 'Impresión en Punto Color',
      description: `${p.hojas ?? '?'} hoja(s) · ${p.config?.tinta ?? ''} · ${p.config?.acabado ?? ''}`,
      quantity:    1,
      unit_price:  preciosConDescuento[i],
      currency_id: 'ARS',
    }));
    if (envio > 0) {
      mpItems.push({
        id: 'envio', title: 'Envío', quantity: 1, unit_price: envio, currency_id: 'ARS',
      });
    }

    mpPreference = await preferenceClient.create({
      body: {
        external_reference: pedidoGrupoId,
        items: mpItems,
        payer: { email },
        back_urls: {
          success: `${baseUrl}/pago/success`,
          failure: `${baseUrl}/pago/failure`,
          pending: `${baseUrl}/pago/pending`,
        },
        auto_return:      'approved',
        notification_url: `${baseUrl}/webhook`,
      },
    });
    console.log(`✅ Preferencia creada | id=${mpPreference.id}`);
  } catch (mpError) {
    console.error('MP preference error:', mpError);
    return res.status(502).json({ error: 'No se pudo crear la preferencia de pago' });
  }

  // Guardar los pedidos en Supabase — una fila por ítem del carrito, todas
  // compartiendo pedido_grupo_id. UN SOLO insert con el array completo:
  // es una única sentencia SQL (INSERT ... VALUES (...),(...),(...)),
  // atómica de por sí — si una fila falla, no se inserta ninguna.
  const filas = pedidos.map(p => ({
    pedido_id:        p.pedidoId,
    pedido_grupo_id:  pedidoGrupoId,
    estado:           'pendiente',
    mp_preference_id: mpPreference.id,
    total:            totalParaPago,   // total del GRUPO, repetido en cada fila
    descuento:        descuentoServer > 0 ? descuentoServer : null, // ídem
    subtotal_imp:     p.subtotalImp,
    subtotal_env:     envio,           // ídem — el envío es único para todo el grupo
    copies:           p.copies         ?? null,
    pages:            p.pages          ?? null,
    hojas:            p.hojas          ?? null,
    precio_por_hoja:  p.precioPorHoja  ?? null,
    acab_price:       p.acabPrice      ?? null,
    caras_impresas:   p.carasImpresas  ?? null,
    zona:             zona             ?? null,
    config:           p.config         ?? null,
    direccion:        direccion        ?? null,
    archivos:         p.archivos       ?? null,
    email,
    whatsapp:         whatsapp         ?? null,
    codigo_promo:     codigo           ?? null,
  }));

  const { error: dbError } = await supabase.from('pedidos').insert(filas);

  if (dbError) {
    console.error('Supabase insert error:', dbError);
    return res.status(500).json({ error: 'Pedido creado en MP pero no se pudo guardar en la base de datos' });
  }

  return res.status(201).json({
    ok:          true,
    checkoutUrl: mpPreference.init_point,
  });
});

// ── Webhook de Mercado Pago ───────────────────────────────────────────────────
// MP llama acá cada vez que un pago cambia de estado. Puede llamar varias
// veces para el mismo pago (reintentos) — el handler es idempotente.
app.post('/webhook', async (req, res) => {
  // Responder 200 de inmediato para que MP no reintente por timeout
  res.sendStatus(200);

  const { type, data } = req.body;
  if (type !== 'payment' || !data?.id) return;

  // Validar la firma cuando se pueda — pero SOLO avisar si falla, nunca
  // bloquear. La suscripción "Pagos (legacy)" del dashboard de MP no
  // soporta x-signature (confirmado: el secreto de esa sección no está
  // atado al notification_url que seteamos por preferencia), así que hoy
  // el mismatch es esperado, no un ataque. Si algún día se resuelve el
  // secreto correcto, esto ya está listo para endurecerse a bloqueante.
  if (process.env.MP_WEBHOOK_SECRET) {
    try {
      WebhookSignatureValidator.validate({
        xSignature: req.headers['x-signature'],
        xRequestId: req.headers['x-request-id'],
        dataId: req.query['data.id'],
        secret: process.env.MP_WEBHOOK_SECRET,
      });
    } catch (err) {
      console.warn('⚠️  Firma de webhook no validada (no bloquea) ->', err.message);
    }
  } else {
    console.warn('⚠️  MP_WEBHOOK_SECRET no configurado — el webhook no está validando firma');
  }

  try {
    const payment = await paymentClient.get({ id: data.id });
    console.log(`Webhook payment ${data.id}: status=${payment.status}, ref=${payment.external_reference}`);

    if (payment.status === 'approved') {
      const ref = payment.external_reference || '';
      // pedidoGrupoId siempre empieza con 'PG-' (ver /checkout) — cualquier
      // otro valor es un pedido_id suelto (formato viejo, 'PC-', de antes
      // del carrito). Chequeo excluyente por prefijo, no un OR genérico:
      // evita cualquier ambigüedad si algún día un pedido_id y un
      // pedido_grupo_id llegaran a coincidir como string.
      const matchColumn = ref.startsWith('PG-') ? 'pedido_grupo_id' : 'pedido_id';
      const { error } = await supabase
        .from('pedidos')
        .update({
          estado:         'pagado',
          mp_payment_id:  String(data.id),
        })
        .eq(matchColumn, ref);

      if (error) {
        console.error('Webhook Supabase update error:', error);
      } else {
        console.log(`✅ Pedido(s) con ${matchColumn}=${ref} marcado(s) como pagado`);
      }
    }
  } catch (err) {
    console.error('Webhook error:', err);
  }
});

// ── Páginas de retorno de Mercado Pago ───────────────────────────────────────
// Primera pantalla que ve el cliente justo después de pagar — usan la
// identidad visual del sitio (misma paleta y tipografías que el frontend).
const FRONTEND_URL = 'https://www.puntocolorimpresiones.com';

function escapeHtml(str) {
  return String(str).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

function paginaPago({ emoji, titulo, mensaje, colorAccent, pedidoId, extraLinea }) {
  const pedido = pedidoId ? escapeHtml(pedidoId) : '—';
  return `
    <!DOCTYPE html>
    <html lang="es">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Punto Color — ${escapeHtml(titulo)}</title>
      <link rel="preconnect" href="https://fonts.googleapis.com">
      <link href="https://fonts.googleapis.com/css2?family=Fredoka+One&family=Nunito:wght@400;700&display=swap" rel="stylesheet">
      <style>
        body{
          margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;
          background:#f5e8d0;font-family:'Nunito',sans-serif;color:#1a1009;padding:24px;box-sizing:border-box;
        }
        .card{
          background:#fff;border-radius:20px;padding:40px 32px;max-width:420px;width:100%;
          text-align:center;box-shadow:0 12px 40px rgba(26,16,9,.15);
        }
        .logo{font-family:'Fredoka One',sans-serif;font-size:1.4rem;margin-bottom:20px}
        .logo .punto{color:#1a1009}
        .logo .color{color:${colorAccent}}
        .emoji{font-size:3rem;margin-bottom:8px}
        h1{font-family:'Fredoka One',sans-serif;font-size:1.3rem;color:${colorAccent};margin:0 0 12px}
        p{font-size:.95rem;line-height:1.5;margin:6px 0}
        .pedido-box{
          background:#f5e8d0;border-radius:12px;padding:12px 16px;margin:20px 0;
          font-size:.85rem;color:#4a2e0a;
        }
        .pedido-box strong{color:#1a1009}
        a.volver{
          display:inline-block;margin-top:16px;background:#1a1009;color:#f5e8cf;
          font-family:'Fredoka One',sans-serif;font-size:.85rem;text-decoration:none;
          padding:12px 24px;border-radius:30px;
        }
      </style>
    </head>
    <body>
      <div class="card">
        <div class="logo"><span class="punto">Punto</span> <span class="color">Color</span></div>
        <div class="emoji">${emoji}</div>
        <h1>${escapeHtml(titulo)}</h1>
        <p>${escapeHtml(mensaje)}</p>
        <div class="pedido-box">
          Pedido: <strong>${pedido}</strong>
          ${extraLinea ? `<br>${escapeHtml(extraLinea)}` : ''}
        </div>
        <a class="volver" href="${FRONTEND_URL}">Volver a Punto Color</a>
      </div>
    </body>
    </html>
  `;
}

app.get('/pago/success', (req, res) => {
  const { external_reference, payment_id, status } = req.query;
  console.log('MP success redirect:', { external_reference, payment_id, status });
  res.send(paginaPago({
    emoji: '✅',
    titulo: '¡Pago aprobado!',
    mensaje: 'Recibimos tu pago y ya estamos preparando tu pedido.',
    colorAccent: '#2ec4b6',
    pedidoId: external_reference,
    extraLinea: payment_id ? `Número de pago: ${payment_id}` : null,
  }));
});

app.get('/pago/failure', (req, res) => {
  const { external_reference } = req.query;
  console.log('MP failure redirect:', { external_reference });
  res.send(paginaPago({
    emoji: '❌',
    titulo: 'El pago no se pudo procesar',
    mensaje: 'No te preocupes, no se realizó ningún cobro. Podés intentar de nuevo desde el sitio.',
    colorAccent: '#e8453c',
    pedidoId: external_reference,
  }));
});

app.get('/pago/pending', (req, res) => {
  const { external_reference } = req.query;
  console.log('MP pending redirect:', { external_reference });
  res.send(paginaPago({
    emoji: '⏳',
    titulo: 'Pago pendiente',
    mensaje: 'Tu pago está siendo procesado. Te avisaremos apenas se confirme.',
    colorAccent: '#f5a623',
    pedidoId: external_reference,
  }));
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Punto Color server → http://localhost:${PORT}`);
  console.log(`APP_BASE_URL cargada: ${process.env.APP_BASE_URL ?? '⚠️  NO DEFINIDA'}`);
});
