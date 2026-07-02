FROM node:20-slim

# LibreOffice para conversión server-side de docx/xlsx/pptx → PDF
RUN apt-get update && apt-get install -y \
    libreoffice \
    --no-install-recommends \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY . .

EXPOSE 3001
CMD ["node", "index.js"]
