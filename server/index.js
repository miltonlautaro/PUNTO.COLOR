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
import { rateLimit } from 'express-rate-limit';
import { Resend } from 'resend';
import { randomUUID, randomBytes, createHmac, createHash, timingSafeEqual } from 'crypto';

const execAsync = promisify(exec);
// soffice en PATH en Linux/Docker; en Windows ajustar si es necesario
const SOFFICE = process.platform === 'win32'
  ? '"C:\\Program Files\\LibreOffice\\program\\soffice.exe"'
  : 'soffice';

// Cola de conversión — concurrency: 1 hoy, listo para escalar (solo cambiar el número).
// El timeout lo maneja execAsync (mata el proceso hijo con SIGTERM),
// no p-queue: así un soffice colgado no acumula procesos zombie en background.
// Si una tarea falla o hace timeout, p-queue libera el slot y la cola sigue sola.
const CONVERSION_TIMEOUT_MS = 60_000;
const conversionQueue = new PQueue({ concurrency: 1 });

// Mismos formatos que ofrece el <input accept="..."> del frontend — sin
// esto, cualquier extensión se mandaba directo a LibreOffice sin validar
// antes (LibreOffice es muy permisivo: convierte casi cualquier cosa en
// vez de fallar, así que "confiar en que falle solo" no alcanza).
const EXTENSIONES_PERMITIDAS = ['pdf', 'doc', 'docx', 'jpg', 'jpeg', 'png', 'xlsx', 'xls', 'pptx', 'ppt'];

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
    // NUNCA usar file.originalname para el nombre en disco: lo controla
    // 100% quien sube el archivo (viene del multipart) y, sin sanitizar,
    // permite path traversal vía secuencias '../' — multer no lo hace
    // por su cuenta. El nombre original se sigue usando más abajo solo
    // para lo que ve el cliente (Storage, respuesta), nunca para la
    // ruta real en disco.
    filename(req, file, cb) {
      const ext = (file.originalname.split('.').pop() || 'bin')
        .replace(/[^a-zA-Z0-9]/g, '')
        .slice(0, 10);
      cb(null, `${randomUUID()}.${ext}`);
    },
  }),
  limits: { fileSize: 300 * 1024 * 1024 },
  fileFilter(req, file, cb) {
    const ext = file.originalname.split('.').pop().toLowerCase();
    if (!EXTENSIONES_PERMITIDAS.includes(ext)) {
      return cb(new Error(`Formato .${ext} no soportado. Formatos permitidos: PDF, Word, Excel, PowerPoint, JPG o PNG.`));
    }
    cb(null, true);
  },
});

const app = express();
// Railway pone su proxy de borde delante de la app: confiar exactamente
// 1 salto (no 'true', que confiaría en cualquier cantidad y permitiría
// falsificar X-Forwarded-For) — necesario para que express-rate-limit
// identifique la IP real del cliente en vez de tirar un error.
app.set('trust proxy', 1);
app.use(express.json());
// Solo los orígenes reales del sitio — antes era '*' (cualquier origen),
// válido en desarrollo pero no en producción con plata real de por medio.
app.use(cors({
  origin: [
    'https://www.puntocolorimpresiones.com',
    'https://puntocolorimpresiones.com',
    'https://puntocolor.netlify.app',
  ],
}));

// Rate limit para /procesar-archivo: no requiere login (el sitio permite
// compra como invitado, ver /checkout más abajo), así que en vez de exigir
// JWT ahí — lo que rompería esa compra sin cuenta — se limita por IP para
// frenar abuso/spam sin afectar a un cliente real subiendo sus archivos.
const procesarArchivoLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  limit: 20,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Demasiados archivos procesados en poco tiempo. Esperá unos minutos e intentá de nuevo.' },
});

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

// El constructor de Resend tira una excepción SINCRÓNICA si falta la key —
// eso frenaba el arranque de TODO el servidor, no solo el email. Nunca
// debe construirse sin la key presente.
const resend = process.env.RESEND_API_KEY ? new Resend(process.env.RESEND_API_KEY) : null;

// ── Health check ──────────────────────────────────────────────────────────────
app.get('/health', (_req, res) => res.json({ ok: true }));

// ── Procesar archivo: convertir a PDF (LibreOffice) + contar páginas + guardar en Storage ──
const BUCKET = 'archivos-pedidos';

// Firma para poder borrar un archivo de Storage sin exigir login (el
// checkout admite compra como invitado). Sin esto, DELETE /procesar-archivo
// aceptaba cualquier path y borraba archivos de CUALQUIER pedido — quien
// suba un archivo recibe un token firmado junto con el storageUrl, y solo
// ese token (no adivinable) autoriza borrar ESE archivo puntual.
// Fallback: si falta la variable de entorno, se genera una al arrancar —
// los tokens dejan de ser válidos si el proceso reinicia, pero el borrado
// ya es fire-and-forget del lado del cliente (removeFile()), así que un
// archivo que no se pudo borrar por esto queda cubierto igual por la
// limpieza de huérfanos.
const FILE_DELETE_SECRET = process.env.FILE_DELETE_SECRET || randomBytes(32).toString('hex');
if (!process.env.FILE_DELETE_SECRET) {
  console.warn('⚠️  FILE_DELETE_SECRET no configurado — usando uno generado en este arranque');
}
function firmarStoragePath(path) {
  return createHmac('sha256', FILE_DELETE_SECRET).update(path).digest('hex');
}
function tokenValido(path, tokenRecibido) {
  if (!tokenRecibido) return false;
  const esperado = firmarStoragePath(path);
  const a = Buffer.from(esperado);
  const b = Buffer.from(String(tokenRecibido));
  return a.length === b.length && timingSafeEqual(a, b);
}

function sanitizeKey(name) {
  return name
    .normalize('NFD')
    .replace(/[^\x00-\x7F]/g, '')      // elimina tildes/diacríticos (no-ASCII post-NFD)
    .replace(/[^a-zA-Z0-9.\-_]/g, '_') // espacios y otros chars inválidos → _
    .replace(/_+/g, '_')               // colapsar guiones bajos consecutivos
    .replace(/^_|_$/g, '');            // trim
}

app.post('/procesar-archivo', procesarArchivoLimiter, (req, res) => {
  upload.single('archivo')(req, res, async (err) => {
    if (err) {
      const msg = err.code === 'LIMIT_FILE_SIZE'
        ? 'El archivo es demasiado grande (máximo 300 MB)'
        : (err.message || 'Error al recibir el archivo');
      return res.status(400).json({ error: msg });
    }
    return procesarArchivoHandler(req, res);
  });
});

// Envuelve PDFDocument.load con un mensaje claro para el cliente — sin
// esto, un PDF dañado (o un .pdf que en realidad no es un PDF) devolvía
// el mensaje técnico crudo de pdf-lib con un 500 genérico (probado: da
// "Failed to parse PDF document (line:2 col:0 offset=46): No PDF header
// found"). Es el único caso de "archivo con problema" que realmente falla
// de forma confiable — LibreOffice, en cambio, es muy permisivo y convierte
// casi cualquier cosa (probado con archivos vacíos, binarios random y
// texto plano) en vez de tirar error.
async function cargarPDFConMensajeClaro(buffer) {
  let doc;
  try {
    doc = await PDFDocument.load(buffer, { ignoreEncryption: true });
  } catch (err) {
    const claro = new Error('No pudimos leer tu PDF — puede estar dañado o no ser un PDF válido. Probá abrirlo en otro programa, guardalo de nuevo y subilo otra vez.');
    claro.status = 400;
    throw claro;
  }
  // ignoreEncryption:true deja que pdf-lib parsee la ESTRUCTURA de un PDF
  // cifrado (por eso no siempre falla arriba), pero el CONTENIDO puede
  // quedar ilegible — el cliente pagaría por un archivo que en la
  // impresora puede salir en blanco. Rechazarlo acá, explícitamente.
  if (doc.isEncrypted) {
    const err = new Error('Tu PDF tiene contraseña. Quitásela (en el mismo programa donde lo generaste, o con alguna herramienta para eso) y volvé a subirlo.');
    err.status = 400;
    throw err;
  }
  return doc;
}

async function procesarArchivoHandler(req, res) {
  if (!req.file) return res.status(400).json({ error: 'No se recibió ningún archivo' });
  const { originalname, path: inputPath, filename: safeDiskName } = req.file;
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
      const doc     = await cargarPDFConMensajeClaro(pdfBuffer);
      pages         = doc.getPageCount();
    } else {
      // docx, xlsx, pptx, imágenes → LibreOffice convierte a PDF via cola de conversión.
      // La cola garantiza que un fallo o timeout no bloquea las siguientes conversiones.
      convertedName = originalname.replace(/\.[^.]+$/, '.pdf'); // nombre para mostrar / Storage
      // LibreOffice nombra la salida según el archivo de ENTRADA real en
      // disco (safeDiskName, el nombre random generado en diskStorage),
      // no según el nombre original — outPath tiene que seguir a ese.
      const outPath = join(tmpDir, safeDiskName.replace(/\.[^.]+$/, '.pdf'));
      await conversionQueue.add(async () => {
        const loProfile = join(tmpDir, 'lo-profile').replace(/\\/g, '/');
        // timeout en execAsync mata el proceso soffice hijo con SIGTERM si se agota
        await execAsync(
          `${SOFFICE} --headless --norestore -env:UserInstallation=file:///${loProfile} --convert-to pdf --outdir "${tmpDir}" "${inputPath}"`,
          { timeout: CONVERSION_TIMEOUT_MS }
        );
      });
      pdfBuffer = await readFile(outPath);
      const doc = await cargarPDFConMensajeClaro(pdfBuffer);
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

    const response = {
      ok: true, pages, storageUrl: storagePath, convertedName,
      deleteToken: firmarStoragePath(storagePath),
    };
    if (pdfBase64) response.pdfBase64 = pdfBase64;
    return res.json(response);

  } catch (err) {
    console.error('procesar-archivo error:', err);
    return res.status(err.status || 500).json({ error: err.message || 'Error al procesar el archivo' });
  } finally {
    rm(tmpDir, { recursive: true, force: true }).catch(() => {});
  }
}

// ── Borrar archivo de Storage (llamado desde removeFile() — fire-and-forget) ──
app.delete('/procesar-archivo', procesarArchivoLimiter, async (req, res) => {
  const { path: storagePath, deleteToken } = req.body;
  if (!storagePath) return res.status(400).json({ error: 'path requerido' });
  if (!tokenValido(storagePath, deleteToken)) {
    return res.status(403).json({ error: 'No autorizado a borrar este archivo' });
  }
  const { error } = await supabase.storage.from(BUCKET).remove([storagePath]);
  if (error) return res.status(500).json({ error: error.message });
  return res.json({ ok: true });
});

// ── Limpieza de archivos huérfanos en Storage (llamado por cron externo) ─────
const ORPHAN_MAX_AGE_MS = 48 * 60 * 60 * 1000;
// Storage gratis de Supabase es limitado (1GB) y los archivos de pedidos
// completados no se borraban nunca — con volumen real, se llena en
// semanas. 30 días de margen para reimpresiones/reclamos antes de borrar.
const RETENCION_COMPLETADOS_MS = 30 * 24 * 60 * 60 * 1000;

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
          .select('estado, created_at, mp_status, completado_at')
          .eq('pedido_id', pedidoId)
          .maybeSingle();
        if (pedidoErr) throw pedidoErr;

        // pending/in_process (Rapipago, Pago Fácil) pueden tardar días en
        // confirmarse — nunca se consideran huérfanos aunque tengan más
        // de 48hs, para no borrar los archivos de un pago que todavía
        // puede llegar a confirmarse.
        const pagoEnCurso = pedido?.mp_status === 'pending' || pedido?.mp_status === 'in_process';
        const esHuerfanoAbandonado = !pagoEnCurso && (!pedido
          || (pedido.estado === 'pendiente' && (Date.now() - new Date(pedido.created_at).getTime()) > ORPHAN_MAX_AGE_MS));

        // Retención: además de huérfanos abandonados, también se limpian
        // los archivos de pedidos ya 'completado' hace más de 30 días.
        const esCompletadoVencido = pedido?.estado === 'completado' && pedido.completado_at
          && (Date.now() - new Date(pedido.completado_at).getTime()) > RETENCION_COMPLETADOS_MS;

        const debeEliminarse = esHuerfanoAbandonado || esCompletadoVencido;
        if (!debeEliminarse) continue;

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

// ── Autenticación ────────────────────────────────────────────────────────────
// Exige una sesión válida y deja el usuario en req.user. Es el control que le
// faltaba al servidor: hasta ahora solo existía requireAdmin, y el único
// chequeo de "usuario logueado" estaba suelto dentro de /checkout.
async function requireUser(req, res, next) {
  const authHeader = req.headers['authorization'] || '';
  const token = authHeader.replace('Bearer ', '').trim();
  if (!token) return res.status(401).json({ error: 'Se requiere sesión' });

  const { data: { user }, error: authErr } = await supabase.auth.getUser(token);
  if (authErr || !user) return res.status(401).json({ error: 'Sesión inválida' });

  req.user = user;
  next();
}

// Admin = usuario logueado + is_admin en su perfil. La columna is_admin solo
// se puede cambiar desde Supabase: el navegador tiene revocado el UPDATE
// sobre ella (si no, cualquiera se promovía a admin desde la consola).
async function requireAdmin(req, res, next) {
  return requireUser(req, res, async () => {
    const { data: profile, error: profileErr } = await supabase
      .from('profiles')
      .select('is_admin')
      .eq('id', req.user.id)
      .single();
    if (profileErr || !profile?.is_admin) return res.status(403).json({ error: 'No autorizado' });
    next();
  });
}

app.get('/admin/pedidos', requireAdmin, async (req, res) => {
  // Solo las columnas que admin.html realmente pinta (confirmado por
  // grep sobre el archivo) — antes era select('*'), que mandaba de más
  // (mp_preference_id, pedido_grupo_id, descuento, etc.) sin necesidad.
  const { data: pedidos, error } = await supabase
    .from('pedidos')
    .select('pedido_id, pedido_grupo_id, estado, created_at, email, whatsapp, config, copies, pages, hojas, archivos, zona, direccion, total')
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
    // completado_at queda registrado para que la limpieza automática
    // sepa desde cuándo contar los 30 días de retención de archivos.
    .update({ estado: 'completado', completado_at: new Date().toISOString() })
    .eq('pedido_id', pedidoId);
  if (error) return res.status(500).json({ error: error.message });
  return res.json({ ok: true });
});

// ── Constancias de alumno regular ────────────────────────────────────────────
// Bucket propio, separado de archivos-pedidos a propósito: la limpieza de
// huérfanos borra sin margen de edad cualquier carpeta de ese bucket que no
// tenga una fila en `pedidos`, así que una constancia guardada ahí
// desaparecería en la primera corrida del cron.
const BUCKET_CONSTANCIAS = 'constancias';
const RETENCION_CONSTANCIAS_MS = 15 * 24 * 60 * 60 * 1000;
const EXTENSIONES_CONSTANCIA = ['pdf', 'jpg', 'jpeg', 'png'];
const CONTENT_TYPE_CONSTANCIA = {
  pdf: 'application/pdf', jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png',
};

// En memoria y con un límite chico: una constancia son una o dos páginas. No
// pasa por LibreOffice — se guarda tal cual, con su content-type real.
const uploadConstancia = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter(req, file, cb) {
    const ext = (file.originalname.split('.').pop() || '').toLowerCase();
    if (!EXTENSIONES_CONSTANCIA.includes(ext)) {
      return cb(new Error('Solo se acepta PDF, JPG o PNG.'));
    }
    cb(null, true);
  },
});

// El beneficio se renueva todos los abriles: una constancia aprobada vale
// hasta el 31 de marzo siguiente, 23:59:59 hora argentina (UTC-3), que en UTC
// es el 1 de abril a las 02:59:59.
function vencimientoEstudiantil(desde = new Date()) {
  const corteDe = (anio) => Date.UTC(anio, 3, 1, 2, 59, 59);
  const anio = desde.getUTCFullYear();
  return new Date(desde.getTime() <= corteDe(anio) ? corteDe(anio) : corteDe(anio + 1));
}

// ¿Tiene beneficio vigente? Se mira si existe ALGUNA constancia aprobada sin
// vencer, no la última fila: alguien aprobado puede haber subido después una
// constancia nueva que todavía está en revisión o fue rechazada, y eso no
// tiene por qué quitarle el beneficio que ya tenía.
async function beneficioEstudianteVigente(userId) {
  const { data } = await supabase
    .from('constancias')
    .select('vence_at')
    .eq('user_id', userId)
    .eq('estado', 'aprobada')
    .gt('vence_at', new Date().toISOString())
    .order('vence_at', { ascending: false })
    .limit(1)
    .maybeSingle();
  return { activo: !!data, venceAt: data?.vence_at || null };
}

app.post('/constancia', procesarArchivoLimiter, requireUser, (req, res) => {
  uploadConstancia.single('archivo')(req, res, async (err) => {
    if (err) {
      const msg = err.code === 'LIMIT_FILE_SIZE'
        ? 'El archivo es demasiado grande (máximo 10 MB)'
        : (err.message || 'Error al recibir el archivo');
      return res.status(400).json({ error: msg });
    }
    if (!req.file) return res.status(400).json({ error: 'No se recibió ningún archivo' });

    const ext = req.file.originalname.split('.').pop().toLowerCase();
    // La carpeta sale del TOKEN, nunca del body: así nadie puede escribir ni
    // leer en la carpeta de otra persona.
    const storagePath = `${req.user.id}/${randomUUID()}.${ext}`;

    try {
      // Reemplaza la constancia en revisión anterior — la base solo admite una
      // pendiente por persona (índice único parcial).
      const { data: previa } = await supabase
        .from('constancias')
        .select('id, storage_path')
        .eq('user_id', req.user.id)
        .eq('estado', 'pendiente')
        .maybeSingle();
      if (previa) {
        if (previa.storage_path) {
          await supabase.storage.from(BUCKET_CONSTANCIAS).remove([previa.storage_path]);
        }
        await supabase.from('constancias').delete().eq('id', previa.id);
      }

      const { error: upErr } = await supabase.storage
        .from(BUCKET_CONSTANCIAS)
        .upload(storagePath, req.file.buffer, {
          contentType: CONTENT_TYPE_CONSTANCIA[ext], upsert: false,
        });
      if (upErr) throw upErr;

      const { error: insErr } = await supabase.from('constancias').insert({
        user_id: req.user.id,
        storage_path: storagePath,
        nombre_archivo: req.file.originalname.slice(0, 200),
      });
      if (insErr) {
        // No dejar el archivo colgado si la fila no se pudo guardar.
        await supabase.storage.from(BUCKET_CONSTANCIAS).remove([storagePath]);
        throw insErr;
      }

      console.log(`🎓 Constancia recibida | user=${req.user.id}`);
      return res.status(201).json({ ok: true, estado: 'pendiente' });
    } catch (e) {
      console.error('constancia upload error:', e);
      return res.status(500).json({ error: 'No pudimos guardar tu constancia. Probá de nuevo.' });
    }
  });
});

app.get('/constancia/mia', requireUser, async (req, res) => {
  const { data: ultima, error } = await supabase
    .from('constancias')
    .select('estado, motivo_rechazo, vence_at, created_at, revisada_at, nombre_archivo, storage_path')
    .eq('user_id', req.user.id)
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error) return res.status(500).json({ error: error.message });

  const beneficio = await beneficioEstudianteVigente(req.user.id);
  return res.json({
    constancia: ultima ? { ...ultima, archivoBorrado: !ultima.storage_path, storage_path: undefined } : null,
    beneficio,
  });
});

app.get('/admin/constancias', requireAdmin, async (req, res) => {
  const { data: constancias, error } = await supabase
    .from('constancias')
    .select('id, user_id, storage_path, nombre_archivo, estado, motivo_rechazo, vence_at, created_at, revisada_at')
    .order('created_at', { ascending: false });
  if (error) return res.status(500).json({ error: error.message });

  // El email vive en auth.users, no en profiles: se trae todo de una en vez de
  // consultar usuario por usuario.
  const emails = {};
  try {
    const { data: lista } = await supabase.auth.admin.listUsers({ perPage: 1000 });
    (lista?.users || []).forEach(u => { emails[u.id] = u.email; });
  } catch (e) {
    console.error('No se pudieron leer los emails de los usuarios:', e.message);
  }

  const ids = [...new Set(constancias.map(c => c.user_id))];
  const { data: perfiles } = ids.length
    ? await supabase.from('profiles').select('id, nombre, telefono').in('id', ids)
    : { data: [] };
  const porId = {};
  (perfiles || []).forEach(p => { porId[p.id] = p; });

  const conDatos = await Promise.all(constancias.map(async (c) => {
    let url = null;
    if (c.storage_path) {
      // 5 minutos, mucho menos que la hora que usan los pedidos: son
      // documentos con datos personales (nombre, DNI, universidad).
      const { data: signed } = await supabase.storage
        .from(BUCKET_CONSTANCIAS).createSignedUrl(c.storage_path, 300);
      url = signed?.signedUrl || null;
    }
    return {
      ...c,
      storage_path: undefined,
      archivoBorrado: !c.storage_path,
      url,
      nombre:   porId[c.user_id]?.nombre || null,
      telefono: porId[c.user_id]?.telefono || null,
      email:    emails[c.user_id] || null,
    };
  }));

  return res.json({ constancias: conDatos });
});

app.post('/admin/constancias/:id/aprobar', requireAdmin, async (req, res) => {
  const vence = vencimientoEstudiantil();
  const { data, error } = await supabase
    .from('constancias')
    .update({
      estado: 'aprobada',
      motivo_rechazo: null,
      vence_at: vence.toISOString(),
      revisada_at: new Date().toISOString(),
      revisada_por: req.user.id,
    })
    .eq('id', req.params.id)
    .select()
    .maybeSingle();
  if (error) return res.status(500).json({ error: error.message });
  if (!data) return res.status(404).json({ error: 'No se encontró esa constancia.' });

  console.log(`🎓 Constancia aprobada | id=${data.id} | vence ${vence.toISOString()}`);
  return res.json({ ok: true, constancia: data });
});

app.post('/admin/constancias/:id/rechazar', requireAdmin, async (req, res) => {
  const motivo = String(req.body.motivo || '').trim();
  if (!motivo) return res.status(400).json({ error: 'Hace falta explicar el motivo del rechazo.' });

  const { data, error } = await supabase
    .from('constancias')
    .update({
      estado: 'rechazada',
      motivo_rechazo: motivo.slice(0, 300),
      vence_at: null,
      revisada_at: new Date().toISOString(),
      revisada_por: req.user.id,
    })
    .eq('id', req.params.id)
    .select()
    .maybeSingle();
  if (error) return res.status(500).json({ error: error.message });
  if (!data) return res.status(404).json({ error: 'No se encontró esa constancia.' });

  console.log(`🎓 Constancia rechazada | id=${data.id}`);
  return res.json({ ok: true, constancia: data });
});

// Borra el ARCHIVO a los 15 días pero conserva la fila: el beneficio aprobado
// tiene que seguir valiendo hasta el 31 de marzo, y el motivo de un rechazo
// tiene que poder leerlo el cliente. Lo que se elimina es el documento con
// datos personales, que es lo que no hay que retener.
app.post('/admin/limpiar-constancias', async (req, res) => {
  const secret = req.headers['x-cron-secret'];
  if (!secret || secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'No autorizado' });
  }

  const limite = new Date(Date.now() - RETENCION_CONSTANCIAS_MS).toISOString();
  const resultado = { revisadas: 0, archivosEliminados: 0, errores: [] };

  try {
    const { data: viejas, error } = await supabase
      .from('constancias')
      .select('id, storage_path')
      .lt('created_at', limite)
      .not('storage_path', 'is', null);
    if (error) throw error;

    resultado.revisadas = viejas.length;
    for (const c of viejas) {
      try {
        const { error: rmErr } = await supabase.storage
          .from(BUCKET_CONSTANCIAS).remove([c.storage_path]);
        if (rmErr) throw rmErr;
        const { error: updErr } = await supabase
          .from('constancias').update({ storage_path: null }).eq('id', c.id);
        if (updErr) throw updErr;
        resultado.archivosEliminados++;
      } catch (e) {
        resultado.errores.push({ id: c.id, error: e.message || String(e) });
      }
    }
    return res.json(resultado);
  } catch (err) {
    console.error('limpiar-constancias error:', err);
    return res.status(500).json({ error: err.message || 'Error al limpiar constancias', ...resultado });
  }
});

// ── Códigos promocionales: administración desde el panel ─────────────────────
// Se valida con rigor al crear porque /checkout aplica el porcentaje sin tope:
// un valor mal cargado (ej. 150%) descuadraría el cobro.
const TIPOS_CODIGO = ['porcentaje', 'monto'];

app.get('/admin/codigos', requireAdmin, async (req, res) => {
  const { data: codigos, error } = await supabase
    .from('codigos_promocionales')
    .select('id, codigo, tipo, valor, activo, descripcion, created_at')
    .order('created_at', { ascending: false });
  if (error) return res.status(500).json({ error: error.message });

  // Usos por código: la tabla es chica, se cuenta en memoria en vez de hacer
  // una consulta por cada código.
  const { data: usos, error: usosErr } = await supabase.from('codigos_usados').select('codigo');
  if (usosErr) return res.status(500).json({ error: usosErr.message });
  const conteo = {};
  usos.forEach(u => { conteo[u.codigo] = (conteo[u.codigo] || 0) + 1; });

  return res.json({ codigos: codigos.map(c => ({ ...c, usos: conteo[c.codigo] || 0 })) });
});

app.post('/admin/codigos', requireAdmin, async (req, res) => {
  const codigo      = String(req.body.codigo || '').trim().toUpperCase();
  const tipo        = String(req.body.tipo || '').trim();
  const valor       = Number(req.body.valor);
  const descripcion = String(req.body.descripcion || '').trim() || null;

  // El cliente escribe el código en mayúsculas y compara exacto (ver
  // aplicarCodigoPromo en index.html), así que se guarda ya normalizado.
  if (!/^[A-Z0-9]{3,32}$/.test(codigo)) {
    return res.status(400).json({ error: 'El código debe tener entre 3 y 32 caracteres, solo letras y números.' });
  }
  if (!TIPOS_CODIGO.includes(tipo)) {
    return res.status(400).json({ error: 'El tipo tiene que ser "porcentaje" o "monto".' });
  }
  if (!Number.isFinite(valor) || valor <= 0) {
    return res.status(400).json({ error: 'El valor tiene que ser un número mayor a 0.' });
  }
  if (tipo === 'porcentaje' && valor > 100) {
    return res.status(400).json({ error: 'Un descuento por porcentaje no puede superar el 100%.' });
  }

  // Chequeo previo de duplicado: no se sabe si la tabla tiene UNIQUE sobre
  // codigo, así que no alcanza con esperar el error de la base.
  const { data: yaExiste } = await supabase
    .from('codigos_promocionales').select('codigo').eq('codigo', codigo).maybeSingle();
  if (yaExiste) return res.status(409).json({ error: `Ya existe un código ${codigo}.` });

  const { data, error } = await supabase
    .from('codigos_promocionales')
    .insert({ codigo, tipo, valor, activo: true, descripcion })
    .select()
    .single();
  if (error) {
    const duplicado = error.code === '23505';
    return res.status(duplicado ? 409 : 500)
      .json({ error: duplicado ? `Ya existe un código ${codigo}.` : error.message });
  }

  console.log(`🏷️  Código creado desde el panel: ${codigo} (${tipo} ${valor})`);
  return res.status(201).json({ ok: true, codigo: data });
});

// Activar/desactivar. NUNCA se borra: codigos_usados no tiene clave foránea
// contra esta tabla, así que un DELETE dejaría registros de uso colgados y, si
// después se recreara el mismo código, quienes ya lo usaron seguirían
// bloqueados por el UNIQUE (user_id, codigo). La validación del checkout ya
// filtra por activo = true, así que desactivar alcanza.
app.post('/admin/codigos/:codigo/estado', requireAdmin, async (req, res) => {
  const codigo = String(req.params.codigo || '').trim().toUpperCase();
  const activo = req.body.activo === true;

  const { data, error } = await supabase
    .from('codigos_promocionales')
    .update({ activo })
    .eq('codigo', codigo)
    .select()
    .maybeSingle();
  if (error) return res.status(500).json({ error: error.message });
  if (!data) return res.status(404).json({ error: 'No se encontró ese código.' });

  console.log(`🏷️  Código ${codigo} ${activo ? 'activado' : 'desactivado'} desde el panel`);
  return res.json({ ok: true, codigo: data });
});

// ── Crear preferencia MP + registrar pedido en Supabase ──────────────────────
// Máximo de pedidos (ítems) que se pueden combinar en un solo carrito/pago.
// No corresponde a un límite documentado de Mercado Pago — es un resguardo
// propio de tamaño de payload/UX.
const MAX_ITEMS_CARRITO = 10;

// ── Precios: fuente de verdad del servidor ───────────────────────────────────
// Espejo de PRECIOS y ZONAS_ENTREGA de index.html. Están duplicados a
// propósito: antes el backend aceptaba el subtotal y el costo de envío que
// mandaba el navegador y solo verificaba que fueran números positivos, así
// que cualquiera podía editar el payload y pagar $1 por un pedido de
// $20.000. Ahora el servidor recalcula ambos y descarta lo que le manda el
// cliente. IMPORTANTE: si se cambia un precio en index.html hay que
// cambiarlo también acá, o el cobro no va a coincidir con lo que ve el
// cliente en pantalla (el servidor manda, así que cobraría el de acá).
const PRECIOS = {
  hoja: {
    bn:    { una: 150, doble: 270 },
    color: { una: 330, doble: 600 },
  },
  acabado: { suelto: 0, abrochado: 120, anillado: 2000, encuadernado: 1800 },
};

const ZONAS_ENTREGA = {
  '1':  { name: 'Zona 1 (Centro)',    precio: 3000 },
  '2':  { name: 'Zona 2',             precio: 3300 },
  '3':  { name: 'Zona 3',             precio: 2500 },
  '4':  { name: 'Zona 4',             precio: 1500 },
  'ov': { name: 'Oro Verde',          precio: 3500 },
  'sb': { name: 'San Benito',         precio: 3300 },
  'ca': { name: 'Colonia Avellaneda', precio: 3700 },
  'pe-terminal': { name: 'Punto de Encuentro — Terminal de Ómnibus', precio: 1500 },
  'pe-uner':     { name: 'Punto de Encuentro — UNER Oro Verde',      precio: 1500 },
  'pe-plaza':    { name: 'Punto de Encuentro — Plaza San Miguel',    precio: 1500 },
};

// Tope defensivo de hojas por ítem. No es un límite de negocio: existe solo
// para que un payload absurdo no genere una preferencia de MP disparatada.
const MAX_HOJAS_POR_ITEM = 5000;

// Recalcula el subtotal de impresión de un ítem con la MISMA fórmula que
// calcularPedido() en el front:  (hojas × precioPorHoja + acabado) × copias
//
// Qué se recalcula y qué no:
//   - precioPorHoja y acabado salen de PRECIOS de acá (el cliente no manda precio).
//   - copies se re-acota a 1..999, igual que el front.
//   - hojas SÍ viene del cliente: depende de la cantidad de páginas de cada
//     PDF, que el server cuenta en /procesar-archivo pero hoy no persiste, así
//     que en /checkout no tiene con qué verificarlo. Se valida que sea un
//     entero razonable. Cerrar del todo ese hueco requiere guardar las páginas
//     por archivo al procesarlo — queda pendiente y está anotado.
//
// Devuelve { ok:true, subtotal, precioPorHoja, acabPrice } o { ok:false, error }.
function calcularImpresionServer(p) {
  const cfg = p && p.config;
  if (!cfg || typeof cfg !== 'object') {
    return { ok: false, error: 'Falta la configuración de uno de los pedidos' };
  }

  const tabla = PRECIOS.hoja[cfg.tinta];
  const precioPorHoja = tabla && tabla[cfg.cara];
  if (typeof precioPorHoja !== 'number') {
    return { ok: false, error: 'La configuración de impresión no es válida' };
  }

  if (!Object.prototype.hasOwnProperty.call(PRECIOS.acabado, cfg.acabado)) {
    return { ok: false, error: 'El acabado elegido no es válido' };
  }

  // Anillado "individual" cobra un anillado por archivo; el resto de los
  // acabados (y el anillado "agrupado") cobran uno solo. Igual que el front.
  let cantidadAnillados = 1;
  if (cfg.acabado === 'anillado' && cfg.encuadernacion === 'individual') {
    cantidadAnillados = Array.isArray(p.archivos) && p.archivos.length > 0
      ? p.archivos.length
      : 1;
  }
  const acabPrice = PRECIOS.acabado[cfg.acabado] * cantidadAnillados;

  const hojas = Number(p.hojas);
  if (!Number.isInteger(hojas) || hojas < 1 || hojas > MAX_HOJAS_POR_ITEM) {
    return { ok: false, error: 'La cantidad de hojas del pedido no es válida' };
  }

  const copiesRaw = parseInt(p.copies, 10);
  const copies = Math.max(1, Math.min(999, Number.isNaN(copiesRaw) ? 1 : copiesRaw));

  return {
    ok: true,
    subtotal: (hojas * precioPorHoja + acabPrice) * copies,
    precioPorHoja,
    acabPrice,
    copies,
  };
}

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
  const { pedidoGrupoId, pedidos, zona, zonaId, direccion, costoEnvio, email, whatsapp, codigo, fbp, fbc } = req.body;

  if (!pedidoGrupoId || !email) {
    return res.status(400).json({ error: 'pedidoGrupoId y email son obligatorios' });
  }
  if (!Array.isArray(pedidos) || pedidos.length === 0) {
    return res.status(400).json({ error: 'El carrito no tiene ningún pedido' });
  }
  if (pedidos.length > MAX_ITEMS_CARRITO) {
    return res.status(400).json({ error: `Un mismo pago admite hasta ${MAX_ITEMS_CARRITO} pedidos` });
  }
  // ── Precios: se recalculan acá, NUNCA se toman del cliente ───────────────
  // El navegador sigue mandando subtotalImp y costoEnvio (los usa para su
  // propia UI), pero acá se descartan y se vuelven a calcular desde PRECIOS
  // y ZONAS_ENTREGA. Si no coinciden, manda el del servidor y se loguea:
  // puede ser un intento de manipular el pago o un precio desincronizado
  // entre index.html y este archivo.
  const subtotalesServer = [];
  for (const p of pedidos) {
    if (!p.pedidoId) {
      return res.status(400).json({ error: 'Uno de los pedidos del carrito es inválido' });
    }
    const calc = calcularImpresionServer(p);
    if (!calc.ok) {
      return res.status(400).json({ error: calc.error });
    }
    if (typeof p.subtotalImp === 'number' && p.subtotalImp !== calc.subtotal) {
      console.warn(
        `⚠️  Subtotal recalculado | pedido=${p.pedidoId} | cliente=${p.subtotalImp} | servidor=${calc.subtotal} — se cobra el del servidor`,
      );
    }
    subtotalesServer.push(calc);
  }
  // Se trabaja sobre una copia con los importes ya corregidos, para que todo
  // lo que sigue (descuento, ítems de MP, filas de la base) use los valores
  // del servidor y no los del navegador.
  const pedidosValidados = pedidos.map((p, i) => ({
    ...p,
    subtotalImp:   subtotalesServer[i].subtotal,
    precioPorHoja: subtotalesServer[i].precioPorHoja,
    acabPrice:     subtotalesServer[i].acabPrice,
    copies:        subtotalesServer[i].copies,
  }));

  // Envío: se resuelve por zonaId contra la tabla del servidor. El fallback
  // por nombre cubre una pestaña vieja abierta antes de este cambio, que
  // todavía no manda zonaId.
  let zonaResuelta = zonaId ? ZONAS_ENTREGA[zonaId] : null;
  if (!zonaResuelta && zona && zona.name) {
    zonaResuelta = Object.values(ZONAS_ENTREGA).find(z => z.name === zona.name) || null;
  }
  if (!zonaResuelta) {
    return res.status(400).json({ error: 'La zona de entrega no es válida' });
  }
  const envio = zonaResuelta.precio;
  if (typeof costoEnvio === 'number' && costoEnvio !== envio) {
    console.warn(
      `⚠️  Envío recalculado | grupo=${pedidoGrupoId} | cliente=${costoEnvio} | servidor=${envio} — se cobra el del servidor`,
    );
  }

  const subtotalImpTotal = subtotalesServer.reduce((acc, c) => acc + c.subtotal, 0);
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
  const preciosConDescuento = distribuirDescuento(pedidosValidados, descuentoServer, subtotalImpTotal);

  const baseUrl = process.env.APP_BASE_URL;
  console.log(`📦 Creando preferencia MP | pedidoGrupoId=${pedidoGrupoId} | ${pedidos.length} ítem(s) | total=${totalParaPago}`);

  // Crear preferencia en Mercado Pago — un item por cada pedido del carrito
  // (MP soporta items[] múltiples nativamente) + uno de envío si corresponde.
  let mpPreference;
  try {
    const mpItems = pedidosValidados.map((p, i) => ({
      id:          p.pedidoId,
      title:       pedidosValidados.length > 1 ? `Impresión ${i + 1}/${pedidosValidados.length} — Punto Color` : 'Impresión en Punto Color',
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
  //
  // La preferencia de Mercado Pago YA existe en este punto — si el
  // insert falla y no se hace nada más, el cliente puede pagar esa
  // preferencia igual y quedar sin ningún registro en Punto Color
  // ("pedido fantasma"). Por eso: reintentar unas veces (cubre fallos
  // momentáneos de Supabase) y, si definitivamente falla, avisar por
  // email con los datos necesarios para conciliar a mano.
  // Datos de matching para la API de Conversiones de Meta (evento Purchase
  // que manda el webhook al confirmarse el pago). Solo strings acotados —
  // vienen del navegador del cliente, no se confía en su formato.
  const limpiarTrack = (v) => (typeof v === 'string' && v.length <= 500) ? v : null;
  const metaTrack = {
    fbp: limpiarTrack(fbp),
    fbc: limpiarTrack(fbc),
    ip:  req.ip || null,
    ua:  limpiarTrack(req.get('user-agent')),
  };

  // pedidosValidados y no pedidos: así subtotal_imp guardado en la base es el
  // que realmente se cobró (el del servidor), no el que mandó el navegador.
  const filas = pedidosValidados.map(p => ({
    pedido_id:        p.pedidoId,
    pedido_grupo_id:  pedidoGrupoId,
    estado:           'pendiente',
    mp_preference_id: mpPreference.id,
    meta_track:       metaTrack,
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

  const { ok: insertOk, error: dbError } = await insertarPedidosConReintento(filas);

  if (!insertOk) {
    console.error('Supabase insert error (definitivo tras reintentos):', dbError);
    await alertarPedidoFantasma({
      pedidoGrupoId, mpPreferenceId: mpPreference.id, email, total: totalParaPago, dbError,
    });
    return res.status(500).json({ error: 'Pedido creado en MP pero no se pudo guardar en la base de datos' });
  }

  return res.status(201).json({
    ok:          true,
    checkoutUrl: mpPreference.init_point,
  });
});

// ── Email de confirmación de pedido (Resend) ─────────────────────────────────
// Se dispara SOLO cuando el webhook confirma un pago (nunca antes) — ver
// el update con .neq('estado','pagado') más abajo, que evita reenviarlo
// en reintentos del webhook para un pedido que ya estaba pagado.
function construirEmailConfirmacion(filas) {
  const primera = filas[0];
  const pedidoRef = primera.pedido_grupo_id || primera.pedido_id;

  const itemsHtml = filas.map((f, i) => {
    const archivos = (f.archivos || []).join(', ') || 'Archivo';
    const specs = [f.config?.tinta, f.config?.cara, f.config?.acabado].filter(Boolean).join(' · ');
    const prefijo = filas.length > 1 ? `Pedido ${i + 1}: ` : '';
    return `
      <tr>
        <td style="padding:10px 0;border-bottom:1px solid #e8d9b8;color:#1a1009;font-size:14px;">
          <strong>${escapeHtml(prefijo)}</strong>${escapeHtml(archivos)}<br>
          <span style="color:#6b5200;font-size:13px;">${escapeHtml(specs)}</span>
        </td>
      </tr>`;
  }).join('');

  const html = `
    <!DOCTYPE html>
    <html lang="es">
    <body style="margin:0;padding:0;background:#f5e8d0;font-family:Arial,Helvetica,sans-serif;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f5e8d0;padding:24px 0;">
        <tr><td align="center">
          <table role="presentation" width="480" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:16px;overflow:hidden;">
            <tr>
              <td style="background:#1a1009;padding:20px 24px;text-align:center;">
                <span style="font-size:20px;font-weight:bold;color:#f5e8cf;">Punto <span style="color:#ff8a75;">Color</span></span>
              </td>
            </tr>
            <tr>
              <td style="padding:28px 24px;">
                <div style="font-size:40px;text-align:center;margin-bottom:8px;">✅</div>
                <h1 style="font-size:20px;color:#2ec4b6;text-align:center;margin:0 0 12px;">¡Pago confirmado!</h1>
                <p style="font-size:14px;color:#1a1009;line-height:1.5;text-align:center;margin:0 0 20px;">
                  Recibimos tu pago y ya estamos preparando tu pedido.
                </p>
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:16px;">
                  ${itemsHtml}
                </table>
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f5e8d0;border-radius:12px;">
                  <tr>
                    <td style="padding:14px 16px;font-size:14px;color:#4a2e0a;">
                      Referencia: <strong>${escapeHtml(pedidoRef)}</strong><br>
                      Total pagado: <strong>$ ${Number(primera.total).toLocaleString('es-AR')}</strong>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
              <td style="padding:16px 24px;text-align:center;background:#f5e8d0;font-size:12px;color:#6b5200;">
                Punto Color · Paraná, Entre Ríos
              </td>
            </tr>
          </table>
        </td></tr>
      </table>
    </body>
    </html>
  `;

  const subject = filas.length > 1
    ? `Confirmamos tu pago — ${filas.length} pedidos en Punto Color`
    : 'Confirmamos tu pago en Punto Color';

  return { subject, html };
}

async function enviarEmailConfirmacion(filas) {
  const email = filas[0]?.email;
  if (!email) return;
  if (!resend) {
    console.warn('⚠️  RESEND_API_KEY no configurado — no se envía email de confirmación');
    return;
  }
  try {
    const { subject, html } = construirEmailConfirmacion(filas);
    await resend.emails.send({
      from: 'Punto Color <pedidos@puntocolorimpresiones.com>',
      to: email,
      subject,
      html,
    });
    console.log(`📧 Email de confirmación enviado a ${email}`);
  } catch (err) {
    // Un fallo acá NUNCA debe afectar el resto del webhook — el pedido
    // ya quedó marcado 'pagado' independientemente de si el email salió.
    console.error('Error enviando email de confirmación:', err);
  }
}

// ── Purchase a la API de Conversiones de Meta ────────────────────────────────
// Se dispara en el mismo punto que el email de confirmación: solo cuando el
// webhook hace la transición real a 'pagado' (nunca en reintentos de MP para
// un pago ya confirmado), así el evento sale una sola vez por pedido.
// Server-side porque el cliente puede no volver nunca al sitio después de
// pagar (Rapipago días después, pestaña cerrada) — el navegador no alcanza.
const META_PIXEL_ID = '1742512147060663';

function hashMeta(str) {
  return createHash('sha256').update(str).digest('hex');
}

async function enviarPurchaseCAPI(filas) {
  if (!process.env.META_CAPI_TOKEN) {
    console.warn('⚠️  META_CAPI_TOKEN no configurado — no se envía Purchase a Meta');
    return;
  }
  try {
    const primera = filas[0];
    const userData = {};

    if (primera.email) userData.em = [hashMeta(primera.email.trim().toLowerCase())];
    if (primera.whatsapp) {
      // Meta espera E.164 en dígitos. El checkout ya valida 8-15 dígitos;
      // si el cliente lo escribió sin código de país, se asume Argentina.
      let ph = String(primera.whatsapp).replace(/\D/g, '');
      if (!ph.startsWith('54')) ph = '54' + ph;
      userData.ph = [hashMeta(ph)];
    }
    const track = primera.meta_track || {};
    if (track.fbp) userData.fbp = track.fbp;
    if (track.fbc) userData.fbc = track.fbc;
    if (track.ip)  userData.client_ip_address = track.ip;
    if (track.ua)  userData.client_user_agent = track.ua;

    const body = {
      data: [{
        event_name:       'Purchase',
        event_time:       Math.floor(Date.now() / 1000),
        event_id:         primera.pedido_grupo_id || primera.pedido_id,
        action_source:    'website',
        event_source_url: FRONTEND_URL,
        user_data:        userData,
        custom_data: {
          value:     Number(primera.total) || 0,
          currency:  'ARS',
          num_items: filas.length,
        },
      }],
    };
    // Con esta variable seteada, el evento aparece en "Probar eventos" del
    // Events Manager en vez de contarse como tráfico real — solo para tests.
    if (process.env.META_TEST_EVENT_CODE) body.test_event_code = process.env.META_TEST_EVENT_CODE;

    const resp = await fetch(
      `https://graph.facebook.com/v21.0/${META_PIXEL_ID}/events?access_token=${process.env.META_CAPI_TOKEN}`,
      { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) },
    );
    const json = await resp.json();
    if (!resp.ok) console.error('Meta CAPI error:', JSON.stringify(json));
    else console.log(`📈 Purchase enviado a Meta | event_id=${body.data[0].event_id} | events_received=${json.events_received}`);
  } catch (err) {
    // Igual que el email: un fallo acá nunca debe afectar el webhook.
    console.error('Error enviando Purchase a Meta CAPI:', err);
  }
}

// ── Notificación al dueño por Telegram ───────────────────────────────────────
// Mismo punto de disparo que el email y el Purchase de Meta: solo en la
// transición real a 'pagado', así llega una sola vez por pedido.
async function notificarPedidoTelegram(filas) {
  if (!process.env.TELEGRAM_BOT_TOKEN || !process.env.TELEGRAM_CHAT_ID) {
    console.warn('⚠️  TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID no configurados — no se notifica el pedido');
    return;
  }
  try {
    const primera = filas[0];
    const ref = primera.pedido_grupo_id || primera.pedido_id;
    const archivos = filas.flatMap(f => f.archivos || []);
    const dir = primera.direccion || {};
    const calle = [dir.calle, dir.altura].filter(Boolean).join(' ');
    const extraDir = [dir.piso && `piso ${dir.piso}`, dir.depto && `depto ${dir.depto}`].filter(Boolean).join(', ');

    const lineas = [
      '🖨️ ¡Nuevo pedido pagado!',
      '',
      `📋 ${ref}`,
      `💰 Total: $ ${Number(primera.total).toLocaleString('es-AR')}`,
      `📄 ${filas.length} ítem(s): ${archivos.join(', ') || 'sin nombres'}`,
      `📍 ${primera.zona?.name || '—'} — ${calle || 'sin dirección'}${extraDir ? ' (' + extraDir + ')' : ''}`,
      `👤 ${primera.email || '—'}${primera.whatsapp ? ' · ' + primera.whatsapp : ''}`,
      '',
      '👉 https://puntocolorimpresiones.com/admin.html',
    ];

    const resp = await fetch(
      `https://api.telegram.org/bot${process.env.TELEGRAM_BOT_TOKEN}/sendMessage`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: process.env.TELEGRAM_CHAT_ID, text: lineas.join('\n') }),
      },
    );
    const json = await resp.json();
    if (!json.ok) console.error('Telegram sendMessage error:', JSON.stringify(json));
    else console.log(`📬 Notificación de Telegram enviada | ${ref}`);
  } catch (err) {
    // Igual que el email y CAPI: un fallo acá nunca debe afectar el webhook.
    console.error('Error notificando pedido por Telegram:', err);
  }
}

// ── "Pedido fantasma": preferencia de MP creada pero insert a Supabase falló ──
// Reintenta el insert unas pocas veces (cubre caídas momentáneas) antes de
// darse por vencido y avisar por email con lo necesario para conciliar a mano.
async function insertarPedidosConReintento(filas, intentos = 3) {
  let ultimoError = null;
  for (let i = 0; i < intentos; i++) {
    const { error } = await supabase.from('pedidos').insert(filas);
    if (!error) return { ok: true };
    ultimoError = error;
    console.error(`Supabase insert error (intento ${i + 1}/${intentos}):`, error);
    if (i < intentos - 1) await new Promise(r => setTimeout(r, 500 * (i + 1)));
  }
  return { ok: false, error: ultimoError };
}

async function alertarPedidoFantasma({ pedidoGrupoId, mpPreferenceId, email, total, dbError }) {
  const detalle = {
    pedidoGrupoId, mpPreferenceId, email, total,
    error: dbError?.message || String(dbError),
  };
  if (!resend) {
    console.error('🚨 PEDIDO FANTASMA (sin alerta por email, RESEND_API_KEY no configurado):', detalle);
    return;
  }
  const destino = process.env.ADMIN_ALERT_EMAIL || 'miltonlautaro@gmail.com';
  try {
    await resend.emails.send({
      from: 'Punto Color <pedidos@puntocolorimpresiones.com>',
      to: destino,
      subject: '🚨 Pedido fantasma — revisar a mano',
      html: `
        <p><strong>Un cliente puede llegar a pagar sin que quede registro en la base de Punto Color.</strong></p>
        <p>
          pedidoGrupoId: ${escapeHtml(pedidoGrupoId)}<br>
          Preferencia de Mercado Pago: ${escapeHtml(mpPreferenceId)}<br>
          Email del cliente: ${escapeHtml(email)}<br>
          Total: $ ${Number(total).toLocaleString('es-AR')}
        </p>
        <p>Buscá esta preferencia en tu panel de Mercado Pago para confirmar si el cliente pagó, y cargá el pedido a mano en Supabase si corresponde.</p>
        <p style="color:#888;font-size:12px">Error técnico: ${escapeHtml(dbError?.message || 'desconocido')}</p>
      `,
    });
    console.log('🚨 Alerta de pedido fantasma enviada a', destino);
  } catch (err) {
    console.error('No se pudo enviar el email de alerta de pedido fantasma:', err, detalle);
  }
}

// ── Si falla la consulta del pago al webhook, reintentar y avisar ────────────
// MP ya recibió el 200 antes de esto (para que no reintente el webhook por
// timeout) — si paymentClient.get() falla y no se hace nada más, el pedido
// nunca se marca 'pagado' aunque el pago haya sido aprobado, sin que nadie
// se entere. Reintenta unas veces (cubre fallos momentáneos de red hacia la
// API de MP) y, si sigue fallando, avisa por email con el id de pago para
// poder resolverlo a mano.
async function consultarPagoConReintento(paymentId, intentos = 3) {
  let ultimoError = null;
  for (let i = 0; i < intentos; i++) {
    try {
      return await paymentClient.get({ id: paymentId });
    } catch (err) {
      ultimoError = err;
      console.error(`paymentClient.get error (intento ${i + 1}/${intentos}):`, err.message || err);
      if (i < intentos - 1) await new Promise(r => setTimeout(r, 500 * (i + 1)));
    }
  }
  throw ultimoError;
}

async function alertarWebhookSinResolver(paymentId, error) {
  const detalle = { paymentId, error: error?.message || String(error) };
  if (!resend) {
    console.error('🚨 WEBHOOK sin resolver (sin alerta por email, RESEND_API_KEY no configurado):', detalle);
    return;
  }
  const destino = process.env.ADMIN_ALERT_EMAIL || 'miltonlautaro@gmail.com';
  try {
    await resend.emails.send({
      from: 'Punto Color <pedidos@puntocolorimpresiones.com>',
      to: destino,
      subject: '🚨 Webhook de Mercado Pago sin resolver',
      html: `
        <p><strong>No se pudo consultar el estado de un pago después de varios intentos — el pedido podría no haberse marcado como pagado aunque el cliente sí haya pagado.</strong></p>
        <p>ID de pago en Mercado Pago: ${escapeHtml(paymentId)}</p>
        <p>Revisá ese pago directamente en tu panel de Mercado Pago y, si corresponde, marcá el pedido como pagado a mano en Supabase.</p>
        <p style="color:#888;font-size:12px">Error técnico: ${escapeHtml(error?.message || 'desconocido')}</p>
      `,
    });
    console.log('🚨 Alerta de webhook sin resolver enviada a', destino);
  } catch (err) {
    console.error('No se pudo enviar el email de alerta de webhook sin resolver:', err, detalle);
  }
}

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
    const payment = await consultarPagoConReintento(data.id);
    console.log(`Webhook payment ${data.id}: status=${payment.status}, ref=${payment.external_reference}`);

    const ref = payment.external_reference || '';
    // pedidoGrupoId siempre empieza con 'PG-' (ver /checkout) — cualquier
    // otro valor es un pedido_id suelto (formato viejo, 'PC-', de antes
    // del carrito). Chequeo excluyente por prefijo, no un OR genérico:
    // evita cualquier ambigüedad si algún día un pedido_id y un
    // pedido_grupo_id llegaran a coincidir como string.
    const matchColumn = ref.startsWith('PG-') ? 'pedido_grupo_id' : 'pedido_id';

    if (payment.status === 'approved') {
      // .neq('estado','pagado') + .select(): solo trae las filas que
      // ESTA llamada realmente hizo pasar a 'pagado'. Si MP reintenta el
      // webhook para un pago ya confirmado antes, no vuelve ninguna fila
      // acá — así el email de confirmación nunca se manda dos veces.
      const { data: filasActualizadas, error } = await supabase
        .from('pedidos')
        .update({
          estado:         'pagado',
          mp_payment_id:  String(data.id),
          mp_status:      payment.status,
        })
        .eq(matchColumn, ref)
        .neq('estado', 'pagado')
        .select();

      if (error) {
        console.error('Webhook Supabase update error:', error);
      } else {
        console.log(`✅ Pedido(s) con ${matchColumn}=${ref} marcado(s) como pagado`);
        if (filasActualizadas && filasActualizadas.length > 0) {
          await enviarEmailConfirmacion(filasActualizadas);
          await enviarPurchaseCAPI(filasActualizadas);
          await notificarPedidoTelegram(filasActualizadas);
        }
      }
    } else {
      // pending / in_process (Rapipago, Pago Fácil pueden tardar días en
      // confirmarse) / rejected / cancelled, etc. — nunca se toca 'estado'
      // acá (solo la rama 'approved' lo hace), pero SÍ se registra el
      // status real de MP en mp_status. Es la pieza clave para que la
      // limpieza de huérfanos pueda excluir un pago en curso aunque
      // tenga más de 48hs (ver /admin/limpiar-huerfanos).
      const { error } = await supabase
        .from('pedidos')
        .update({ mp_status: payment.status })
        .eq(matchColumn, ref);

      if (error) console.error('Webhook Supabase update error (mp_status):', error);
      else console.log(`Pedido(s) con ${matchColumn}=${ref} — status de MP: ${payment.status}`);
    }
  } catch (err) {
    console.error('Webhook error (tras reintentos):', err);
    await alertarWebhookSinResolver(data.id, err);
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
