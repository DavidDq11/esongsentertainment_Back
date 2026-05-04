import { Router } from 'express'
import fs from 'fs'
import path from 'path'
import { createHash } from 'crypto'
import multer from 'multer'
import { PutObjectCommand, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import * as XLSX from 'xlsx'
import pool from '../config/db.js'
import { r2, R2_BUCKET } from '../config/r2.js'
import { requireAdmin, requireAuth } from '../middleware/auth.js'

const logMem = (tag) => {
  const m = process.memoryUsage()
  console.log(
    `[MEM] ${tag} | RSS: ${(m.rss / 1024 / 1024).toFixed(1)}MB` +
    ` | Heap: ${(m.heapUsed / 1024 / 1024).toFixed(1)}MB`
  )
}

const uploadDir = path.resolve('uploads')
fs.mkdirSync(uploadDir, { recursive: true })

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

const upload = multer({
  storage: multer.diskStorage({
    destination: uploadDir,
    filename: (_req, file, cb) => {
      const safeName = file.originalname.replace(/[^a-zA-Z0-9._-]/g, '_')
      cb(null, `${Date.now()}-${safeName}`)
    },
  }),
  limits: {
    fileSize: 20 * 1024 * 1024,
    files: 30,
  },
  fileFilter: (_req, file, cb) => {
    if (file.mimetype.includes('spreadsheet') || file.originalname.endsWith('.xlsx')) {
      cb(null, true)
    } else {
      cb(new Error('Only .xlsx files are allowed'))
    }
  },
})

// Rate limiter
const uploadAttempts = new Map()
const RATE_WINDOW_MS = 60 * 60 * 1000 
const RATE_LIMIT_MAX = 20

function uploadRateLimit(req, res, next) {
  const key = req.user?.id || req.ip
  const now = Date.now()
  let record = uploadAttempts.get(key)
  if (!record || now > record.resetAt) {
    record = { count: 0, resetAt: now + RATE_WINDOW_MS }
  }
  const fileCount = Array.isArray(req.files) ? req.files.length : 1
  record.count += fileCount
  uploadAttempts.set(key, record)
  if (record.count > RATE_LIMIT_MAX) {
    return res.status(429).json({ error: 'Too many upload attempts. Please try again in an hour.' })
  }
  next()
}

const LIMIT_BYTES = 8 * 1024 * 1024 * 1024 

// ---------------------------------------------------------------------------
// PARSE EXCEL OPTIMIZADO (ESTILO STREAM / CELDA A CELDA)
// ---------------------------------------------------------------------------
function parseExcel(buffer, tipoHint) {
  logMem('parseExcel:inicio')

  // Leemos con flags de ahorro de memoria
  const wb = XLSX.read(buffer, { 
    type: 'buffer',
    cellFormula: false,
    cellHTML: false,
    cellText: false,
    cellDates: true,
    cellNF: false, // No cargar formatos de número (ahorra RAM)
    sheets: [0]     // Solo procesar la primera hoja
  })

  const ws = wb.Sheets[wb.SheetNames[0]]
  if (!ws['!ref']) throw new Error("La hoja de cálculo está vacía")

  const range = XLSX.utils.decode_range(ws['!ref'])
  const map = new Map()
  let totalReg = 0

  // 1. Identificar encabezados para mapeo dinámico
  const headers = []
  for (let C = range.s.c; C <= range.e.c; ++C) {
    const cell = ws[XLSX.utils.encode_cell({ r: range.s.r, c: C })]
    headers.push(cell ? String(cell.v).trim().toLowerCase() : `col_${C}`)
  }

  // 2. Detección automática de tipo
  const looksStreaming = headers.some(c => ['total_earned_usd', 'total_earned', 'streams', 'song_title', 'isrc'].includes(c))
  const looksYoutube = headers.some(c => ['total net earnings', 'ad total views', 'asset title'].includes(c))
  let tipoFinal = looksYoutube ? 'youtube' : looksStreaming ? 'streaming' : tipoHint
  const tipoCorregido = (tipoFinal !== tipoHint)

  // 3. Procesamiento fila por fila (Evita crear un JSON gigante en memoria)
  for (let R = range.s.r + 1; R <= range.e.r; ++R) {
    const row = {}
    let hasData = false

    for (let C = range.s.c; C <= range.e.c; ++C) {
      const cell = ws[XLSX.utils.encode_cell({ r: R, c: C })]
      if (cell !== undefined) {
        row[headers[C]] = cell.v
        hasData = true
      }
    }

    if (!hasData) continue

    let earnings = 0, views = 0, titulo = '', artista = '', isrc = ''

    if (tipoFinal === 'streaming') {
      const rawVal = row['payable to labels'] ?? row['total_earned_usd'] ?? row['total_earned'] ?? 0
      earnings = typeof rawVal === 'number' ? rawVal : parseFloat(String(rawVal).replace(',', '.').replace(/[^0-9.-]/g, '')) || 0
      
      views = parseInt(row['streams'] || 0) || 0
      titulo = String(row['song_title'] || row['title'] || '').trim()
      artista = String(row['artist'] || '').trim()
      isrc = String(row['isrc'] || '').trim()
    } else {
      const rawVal = row['total net earnings'] ?? 0
      earnings = typeof rawVal === 'number' ? rawVal : parseFloat(String(rawVal).replace(',', '.')) || 0
      
      views = parseInt(row['ad total views'] || 0) || 0
      titulo = String(row['asset title'] || row['title'] || '').trim()
      artista = String(row['artist'] || '').trim()
      isrc = String(row['isrc'] || row['custom id'] || '').trim()
    }

    // FILTRO CRÍTICO: Omitir filas de totales para no duplicar la suma
    if ((!titulo && !isrc) || titulo.toLowerCase().includes('total') || titulo.toLowerCase().includes('report total')) {
      continue
    }

    totalReg += earnings
    const key = isrc || titulo
    if (key) {
      if (!map.has(key)) {
        map.set(key, { titulo, artista, isrc: isrc || null, reproducciones: 0, regalias: 0 })
      }
      const entry = map.get(key)
      entry.reproducciones += views
      entry.regalias += earnings
    }
  }

  const topSongs = [...map.values()]
    .sort((a, b) => b.regalias - a.regalias)
    .slice(0, 10)
    .map((s, i) => ({ ...s, posicion: i + 1 }))

  totalReg = Math.round(totalReg * 100) / 100
  logMem('parseExcel:fin')
  return { topSongs, totalReg, tipoFinal, tipoCorregido }
}

// ---------------------------------------------------------------------------
// INSERTAR REPORTE (Sin cambios mayores, lógica estable)
// ---------------------------------------------------------------------------
async function insertReporte(client, { sello_id, tipoFinal, nombre_archivo, r2Key, trimestre, anio, totalReg, fileSize, fileHash, topSongs, upsert = true }) {
  let rep
  if (upsert) {
    rep = await client.query(
      `INSERT INTO reportes (sello_id, tipo, nombre_archivo, r2_key, trimestre, anio, total_regalias, file_size, file_hash)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       ON CONFLICT (r2_key) DO UPDATE
         SET nombre_archivo = EXCLUDED.nombre_archivo,
             total_regalias = EXCLUDED.total_regalias,
             file_size      = EXCLUDED.file_size,
             file_hash      = EXCLUDED.file_hash,
             created_at     = NOW()
       RETURNING *`,
      [sello_id, tipoFinal, nombre_archivo, r2Key, trimestre, anio, totalReg, fileSize, fileHash]
    )
  } else {
    rep = await client.query(
      `INSERT INTO reportes (sello_id, tipo, nombre_archivo, r2_key, trimestre, anio, total_regalias, file_size, file_hash)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       RETURNING *`,
      [sello_id, tipoFinal, nombre_archivo, r2Key, trimestre, anio, totalReg, fileSize, fileHash]
    )
  }
  const repId = rep.rows[0].id

  await client.query('DELETE FROM top_canciones WHERE reporte_id = $1', [repId])
  for (const s of topSongs) {
    await client.query(
      `INSERT INTO top_canciones (reporte_id, posicion, titulo, artista, isrc, reproducciones, regalias)
       VALUES ($1,$2,$3,$4,$5,$6,$7)`,
      [repId, s.posicion, s.titulo, s.artista, s.isrc, s.reproducciones, s.regalias]
    )
  }

  // Update de resumen_regalias (maneja tanto sobreescritura como suma en bulk)
  await client.query(
    upsert
      ? `INSERT INTO resumen_regalias (sello_id, trimestre, anio, total_streaming, total_youtube)
         VALUES ($1, $2, $3,
           CASE WHEN $4 = 'streaming' THEN $5::numeric ELSE 0 END,
           CASE WHEN $4 = 'youtube'   THEN $5::numeric ELSE 0 END
         )
         ON CONFLICT (sello_id, trimestre, anio) DO UPDATE SET
           total_streaming = CASE WHEN $4 = 'streaming' THEN $5::numeric ELSE resumen_regalias.total_streaming END,
           total_youtube   = CASE WHEN $4 = 'youtube'   THEN $5::numeric ELSE resumen_regalias.total_youtube   END`
      : `INSERT INTO resumen_regalias (sello_id, trimestre, anio, total_streaming, total_youtube)
         VALUES ($1, $2, $3,
           CASE WHEN $4 = 'streaming' THEN $5::numeric ELSE 0 END,
           CASE WHEN $4 = 'youtube'   THEN $5::numeric ELSE 0 END
         )
         ON CONFLICT (sello_id, trimestre, anio) DO UPDATE SET
           total_streaming = CASE WHEN $4 = 'streaming' THEN resumen_regalias.total_streaming + $5::numeric ELSE resumen_regalias.total_streaming END,
           total_youtube   = CASE WHEN $4 = 'youtube'   THEN resumen_regalias.total_youtube   + $5::numeric ELSE resumen_regalias.total_youtube   END`,
    [sello_id, trimestre, anio, tipoFinal, totalReg]
  )

  return rep.rows[0]
}

// ---------------------------------------------------------------------------
// HELPERS DE NOMBRE Y MATCH
// ---------------------------------------------------------------------------
function parseFilename(filename) {
  const base = filename.replace(/\.xlsx$/i, '')
  const isYoutube = /youtube/i.test(base)
  const tipo = isYoutube ? 'youtube' : 'streaming'
  let selloRaw = '', trimestre = null, anio = null

  if (isYoutube) {
    const match = base.match(/^(\d)Q-(\d{4})\s*Youtube[\s_-]+(.+)/i)
    if (match) {
      trimestre = parseInt(match[1], 10)
      anio = parseInt(match[2], 10)
      selloRaw = match[3].trim()
    } else {
      const selloMatch = base.match(/youtube[\s_-]+(.+)/i)
      selloRaw = selloMatch ? selloMatch[1].trim() : ''
    }
  } else {
    const yearMatch = base.match(/^(\d{4})/)
    if (yearMatch) anio = parseInt(yearMatch[1], 10)
    const trimMatch = base.match(/T(\d)Q(\d)/i)
    if (trimMatch) {
      trimestre = parseInt(trimMatch[1], 10)
    } else {
      const monthMatch = base.match(/^\d{4}-(\d{2})/)
      if (monthMatch) {
        const month = parseInt(monthMatch[1], 10)
        if (month >= 1 && month <= 12) trimestre = Math.ceil(month / 3)
      }
    }
    const parts = base.split(/[_]/)
    selloRaw = parts.length >= 2 ? parts[1].trim().replace(/^[-\s]+/, '') : (base.match(/\d{4}-\d{2}[\s_-]+(.+?)(?:_|$)/i)?.[1].trim() || '')
  }
  return { tipo, selloRaw, trimestre, anio }
}

function matchSello(selloRaw, sellos) {
  const norm = s => s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/\s+/g, ' ').trim()
  const needle = norm(selloRaw)
  return sellos.find(s => {
    const hay = norm(s.nombre)
    return hay === needle || hay.includes(needle) || needle.includes(hay)
  }) ?? null
}

const router = Router()

// ---------------------------------------------------------------------------
// RUTAS API
// ---------------------------------------------------------------------------

router.get('/storage', requireAdmin, async (_req, res, next) => {
  try {
    const result = await pool.query('SELECT COALESCE(SUM(file_size), 0) AS used, COUNT(*) AS total_files FROM reportes')
    const usedBytes = parseInt(result.rows[0].used)
    res.json({
      used_bytes: usedBytes,
      limit_bytes: LIMIT_BYTES,
      used_gb: (usedBytes / (1024 ** 3)).toFixed(3),
      limit_gb: 8,
      pct: Math.min(Math.round((usedBytes / LIMIT_BYTES) * 100), 100),
      total_files: parseInt(result.rows[0].total_files),
    })
  } catch (err) { next(err) }
})

router.post('/', requireAdmin, uploadRateLimit, upload.single('file'), async (req, res, next) => {
  const filePath = req.file?.path
  try {
    const { sello_id, tipo, trimestre, anio } = req.body
    if (!req.file || !sello_id || !tipo || !trimestre || !anio) return res.status(400).json({ error: 'Missing fields' })

    const buf = await fs.promises.readFile(filePath)
    const fileHash = createHash('sha256').update(buf).digest('hex')

    // Parseo optimizado
    const { topSongs, totalReg, tipoFinal, tipoCorregido } = parseExcel(buf, tipo)

    const selloRow = await pool.query('SELECT nombre FROM sellos WHERE id = $1', [sello_id])
    const safeSelloNombre = (selloRow.rows[0]?.nombre || 'Unknown').replace(/[^a-zA-Z0-9]/g, '_')
    const r2Key = `reportes/${anio}/Q${trimestre}/${safeSelloNombre}/${req.file.filename.split('-').slice(1).join('-')}`;

    await r2.send(new PutObjectCommand({
      Bucket: R2_BUCKET,
      Key: r2Key,
      Body: buf,
      ContentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    }))

    const client = await pool.connect()
    try {
      await client.query('BEGIN')
      const row = await insertReporte(client, {
        sello_id, tipoFinal, nombre_archivo: req.file.originalname,
        r2Key, trimestre: parseInt(trimestre), anio: parseInt(anio), 
        totalReg, fileSize: req.file.size, fileHash, topSongs
      })
      await client.query('COMMIT')
      res.status(201).json({ ...row, tipoCorregido: tipoCorregido ? tipoFinal : null })
    } catch (err) {
      await client.query('ROLLBACK')
      throw err
    } finally { client.release() }

  } catch (err) { next(err) }
  finally { if (filePath) await fs.promises.unlink(filePath).catch(() => {}) }
})

router.post('/bulk', requireAdmin, uploadRateLimit, upload.array('files[]', 30), async (req, res, next) => {
  try {
    const { trimestre, anio } = req.body
    const sellos = (await pool.query("SELECT id, nombre FROM sellos WHERE estado = 'activo'")).rows
    const results = []

    for (const file of req.files) {
      try {
        const buf = await fs.promises.readFile(file.path)
        const { tipo, selloRaw, trimestre: fT, anio: fA } = parseFilename(file.originalname)
        const sello = matchSello(selloRaw, sellos)

        if (!sello || fT !== parseInt(trimestre) || fA !== parseInt(anio)) {
          results.push({ filename: file.originalname, status: 'error', error: 'Sello o periodo no coincide' })
          continue
        }

        const { topSongs, totalReg, tipoFinal } = parseExcel(buf, tipo)
        const r2Key = `reportes/${anio}/Q${trimestre}/${sello.nombre.replace(/\s/g,'_')}/${file.originalname}`

        await r2.send(new PutObjectCommand({ Bucket: R2_BUCKET, Key: r2Key, Body: buf }))

        const client = await pool.connect()
        const row = await insertReporte(client, {
          sello_id: sello.id, tipoFinal, nombre_archivo: file.originalname,
          r2Key, trimestre: fT, anio: fA, totalReg,
          fileSize: file.size, fileHash: createHash('sha256').update(buf).digest('hex'), 
          topSongs, upsert: false
        })
        client.release()

        results.push({ filename: file.originalname, status: 'ok', total_regalias: totalReg })
      } catch (e) {
        results.push({ filename: file.originalname, status: 'error', error: e.message })
      } finally {
        await fs.promises.unlink(file.path).catch(() => {})
      }
    }
    res.json({ results })
  } catch (err) { next(err) }
})

// ... El resto de rutas (GET, DELETE) se mantienen igual ...
router.get('/', requireAuth, async (req, res, next) => {
  try {
    let query, params = []
    if (req.user.role === 'admin') {
      const { sello_id, tipo, anio } = req.query
      const conditions = []
      if (sello_id) { params.push(sello_id); conditions.push(`r.sello_id = $${params.length}`) }
      if (tipo) { params.push(tipo); conditions.push(`r.tipo = $${params.length}`) }
      if (anio) { params.push(anio); conditions.push(`r.anio = $${params.length}`) }
      const where = conditions.length ? 'WHERE ' + conditions.join(' AND ') : ''
      query = `SELECT r.*, s.nombre AS sello_nombre FROM reportes r JOIN sellos s ON s.id = r.sello_id ${where} ORDER BY r.created_at DESC`
    } else {
      query = `SELECT r.*, s.nombre AS sello_nombre FROM reportes r JOIN sellos s ON s.id = r.sello_id WHERE r.sello_id = $1 ORDER BY r.created_at DESC`
      params = [req.user.id]
    }
    const result = await pool.query(query, params)
    res.json(result.rows)
  } catch (err) { next(err) }
})

router.get('/:id/download', requireAuth, async (req, res, next) => {
  try {
    const result = await pool.query('SELECT * FROM reportes WHERE id = $1', [req.params.id])
    if (!result.rows.length) return res.status(404).json({ error: 'Not found' })
    const report = result.rows[0]
    if (req.user.role === 'sello' && report.sello_id !== req.user.id) return res.status(403).send('Forbidden')

    const url = await getSignedUrl(r2, new GetObjectCommand({ Bucket: R2_BUCKET, Key: report.r2_key }), { expiresIn: 300 })
    res.json({ url })
  } catch (err) { next(err) }
})

router.delete('/:id', requireAdmin, async (req, res, next) => {
  try {
    const result = await pool.query('SELECT * FROM reportes WHERE id = $1', [req.params.id])
    if (!result.rows.length) return res.status(404).json({ error: 'Not found' })
    const report = result.rows[0]

    await pool.query('DELETE FROM reportes WHERE id = $1', [report.id])
    r2.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: report.r2_key })).catch(() => {})
    res.json({ ok: true })
  } catch (err) { next(err) }
})

export default router