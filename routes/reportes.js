import { Router } from 'express'
import { createHash } from 'crypto'
import multer from 'multer'
import { PutObjectCommand, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import * as XLSX from 'xlsx'
import pool from '../config/db.js'
import { r2, R2_BUCKET } from '../config/r2.js'
import { requireAdmin, requireAuth } from '../middleware/auth.js'

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 20 * 1024 * 1024, // 20 MB per file
    files: 30,                  // max 30 files per request
  },
  fileFilter: (_req, file, cb) => {
    if (file.mimetype.includes('spreadsheet') || file.originalname.endsWith('.xlsx')) {
      cb(null, true)
    } else {
      cb(new Error('Only .xlsx files are allowed'))
    }
  },
})

// Simple in-memory rate limiter for uploads (20 per hour per admin)
const uploadAttempts = new Map()
const RATE_WINDOW_MS = 60 * 60 * 1000 // 1 hour
const RATE_LIMIT_MAX = 20

function uploadRateLimit(req, res, next) {
  const key = req.user?.id || req.ip
  const now = Date.now()
  let record = uploadAttempts.get(key)
  if (!record || now > record.resetAt) {
    record = { count: 0, resetAt: now + RATE_WINDOW_MS }
  }
  // Count each file individually so bulk (30 files) consumes 30 slots
  const fileCount = Array.isArray(req.files) ? req.files.length : 1
  record.count += fileCount
  uploadAttempts.set(key, record)
  if (record.count > RATE_LIMIT_MAX) {
    return res.status(429).json({ error: 'Too many upload attempts. Please try again in an hour.' })
  }
  next()
}

const LIMIT_BYTES = 8 * 1024 * 1024 * 1024 // 8 GB

// ---------------------------------------------------------------------------
// Shared Excel parser — returns { topSongs, totalReg, tipoFinal, tipoCorregido }
// ---------------------------------------------------------------------------
function parseExcel(buffer, tipoHint) {
  const wb = XLSX.read(buffer, { type: 'buffer' })
  const ws = wb.Sheets[wb.SheetNames[0]]
  const data = XLSX.utils.sheet_to_json(ws)

  // Normalize column names: trim surrounding spaces
  const normalized = data.map(row =>
    Object.fromEntries(Object.entries(row).map(([k, v]) => [k.trim(), v]))
  )

  // Auto-detect real file type by column names
  const cols = normalized.length > 0 ? Object.keys(normalized[0]).map(c => c.toLowerCase()) : []
  const looksStreaming = cols.some(c => ['total_earned_usd', 'total_earned', 'streams', 'song_title'].includes(c))
  const looksYoutube = cols.some(c => ['total net earnings', 'ad total views', 'asset title'].includes(c))
  const detectedTipo = looksYoutube ? 'youtube' : looksStreaming ? 'streaming' : null

  let tipoFinal = tipoHint
  let tipoCorregido = false
  if (detectedTipo && detectedTipo !== tipoHint) {
    tipoFinal = detectedTipo
    tipoCorregido = true
  }

  let topSongs = []
  let totalReg = 0

  if (tipoFinal === 'streaming') {
    const map = new Map()
    for (const row of normalized) {
      const rawValue = row['PAYABLE TO LABELS'] ?? row['total_earned_usd'] ?? 0
      const earnings = typeof rawValue === 'number'
        ? rawValue
        : parseFloat(String(rawValue).replace(',', '.').replace(/[^0-9.-]/g, '')) || 0

      const isrc = String(row['isrc'] ?? row['ISRC'] ?? '')
      const titulo = String(row['song_title'] ?? row['title'] ?? '')
      // Skip summary/total rows: no identifier, or titulo explicitly says "total"
      if (!isrc && !titulo) continue
      if (!isrc && titulo.toLowerCase().includes('total')) continue

      totalReg += earnings
      const artista = String(row['artist'] ?? '')
      const key = isrc || titulo

      if (!map.has(key)) {
        map.set(key, { titulo, artista, isrc: isrc || null, reproducciones: 0, regalias: 0 })
      }
      const entry = map.get(key)
      const streams = typeof row['streams'] === 'number'
        ? row['streams']
        : parseInt(String(row['streams'] ?? 0).replace(/\D/g, '')) || 0
      entry.reproducciones += streams
      entry.regalias += earnings
    }
    topSongs = [...map.values()]
      .sort((a, b) => b.regalias - a.regalias)
      .slice(0, 10)
      .map((s, i) => ({ ...s, posicion: i + 1 }))
  } else {
    const map = new Map()
    for (const row of normalized) {
      const assetTitle = String(row['Asset Title'] ?? row['title'] ?? '').toLowerCase()
      const isrcRaw = String(row['ISRC'] ?? row['Custom ID'] ?? '').toLowerCase()
      if (assetTitle.includes('total') || (assetTitle === '' && isrcRaw === '')) continue

      const rawEarnings = row['Total Net Earnings'] ?? 0
      const earnings = typeof rawEarnings === 'number'
        ? rawEarnings
        : parseFloat(String(rawEarnings).replace(',', '.')) || 0
      totalReg += earnings

      const isrc = String(row['ISRC'] ?? row['Custom ID'] ?? '')
      const titulo = String(row['Asset Title'] ?? row['title'] ?? 'Track')
      const artista = String(row['Artist'] ?? '')
      const key = isrc || titulo

      if (!map.has(key)) {
        map.set(key, { titulo, artista, isrc: isrc || null, reproducciones: 0, regalias: 0 })
      }
      const entry = map.get(key)
      const views = parseInt(String(row['AD Total Views'] ?? 0).replace(/\D/g, '')) || 0
      entry.reproducciones += views
      entry.regalias += earnings
    }
    topSongs = [...map.values()]
      .sort((a, b) => b.regalias - a.regalias)
      .slice(0, 10)
      .map((s, i) => ({ ...s, posicion: i + 1 }))
  }

  totalReg = Math.round(totalReg * 100) / 100
  return { topSongs, totalReg, tipoFinal, tipoCorregido }
}

// ---------------------------------------------------------------------------
// Insert a parsed report into DB within an existing client transaction.
// upsert=true  → ON CONFLICT (r2_key) DO UPDATE  (single upload uses same R2 path)
// upsert=false → plain INSERT, allows multiple rows per quarter            (bulk streaming)
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

  // upsert=true  → replace the total (one file owns the slot)
  // upsert=false → add to existing total (multiple monthly files accumulate)
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
// Filename parser for bulk upload
// Patterns:
//   Streaming → "2025-12_Discos Relampago_T4Q2.xlsx"  (sello = part after first _, año=2025, trimestre=4)
//   YouTube   → "4Q-2025 Youtube_Elite Records.xlsx"  (sello = part after "Youtube_", trimestre=4, año=2025)
// Returns { tipo, selloRaw, trimestre?, anio? } — trimestre/anio only if detectable
// ---------------------------------------------------------------------------
function parseFilename(filename) {
  const base = filename.replace(/\.xlsx$/i, '')
  const isYoutube = /youtube/i.test(base)
  const tipo = isYoutube ? 'youtube' : 'streaming'

  let selloRaw = ''
  let trimestre = null
  let anio = null

  if (isYoutube) {
    // "4Q-2025 Youtube -Elite Records" or "4Q-2025 Youtube_Elite Records"
    // Extract trimestre and anio before "Youtube"
    const match = base.match(/^(\d)Q-(\d{4})\s*Youtube[\s_-]+(.+)/i)
    if (match) {
      trimestre = parseInt(match[1], 10)
      anio = parseInt(match[2], 10)
      selloRaw = match[3].trim()
    } else {
      // Fallback: just sello after Youtube
      const selloMatch = base.match(/youtube[\s_-]+(.+)/i)
      selloRaw = selloMatch ? selloMatch[1].trim() : ''
    }
  } else {
    // Streaming: "2025-12_Discos Relampago_T4Q2" or "2026-01_Oliva Records"
    // Extract anio from start, trimestre from explicit T#Q# pattern or month if present
    const yearMatch = base.match(/^(\d{4})/)
    if (yearMatch) {
      anio = parseInt(yearMatch[1], 10)
    }
    const trimMatch = base.match(/T(\d)Q(\d)/i)
    if (trimMatch) {
      trimestre = parseInt(trimMatch[1], 10)
    } else {
      const monthMatch = base.match(/^\d{4}-(\d{2})/)
      if (monthMatch) {
        const month = parseInt(monthMatch[1], 10)
        if (month >= 1 && month <= 12) {
          trimestre = Math.ceil(month / 3)
        }
      }
    }
    // Sello between first _ and second _ or end
    const parts = base.split(/[_]/)
    if (parts.length >= 2) {
      selloRaw = parts[1].trim().replace(/^[-\s]+/, '')
    } else {
      const m = base.match(/\d{4}-\d{2}[\s_-]+(.+?)(?:_|$)/i)
      selloRaw = m ? m[1].trim() : ''
    }
  }

  return { tipo, selloRaw, trimestre, anio }
}

// Fuzzy sello match: normalize unicode/case/spaces and check substring containment
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
// Storage usage (admin only) — must be before /:id routes
// ---------------------------------------------------------------------------
router.get('/storage', requireAdmin, async (_req, res, next) => {
  try {
    const result = await pool.query(
      'SELECT COALESCE(SUM(file_size), 0) AS used, COUNT(*) AS total_files FROM reportes'
    )
    const usedBytes = parseInt(result.rows[0].used)
    const totalFiles = parseInt(result.rows[0].total_files)
    res.json({
      used_bytes: usedBytes,
      limit_bytes: LIMIT_BYTES,
      used_gb: (usedBytes / (1024 ** 3)).toFixed(3),
      limit_gb: 8,
      pct: Math.min(Math.round((usedBytes / LIMIT_BYTES) * 100), 100),
      total_files: totalFiles,
    })
  } catch (err) {
    next(err)
  }
})

// ---------------------------------------------------------------------------
// Upload report (admin only) — single file with R2 storage
// ---------------------------------------------------------------------------
router.post('/', requireAdmin, uploadRateLimit, upload.single('file'), async (req, res, next) => {
  try {
    const { sello_id, tipo, trimestre, anio } = req.body
    if (!req.file || !sello_id || !tipo || !trimestre || !anio) {
      return res.status(400).json({ error: 'file, sello_id, tipo, trimestre, anio are required' })
    }
    if (!UUID_RE.test(sello_id)) {
      return res.status(400).json({ error: 'sello_id must be a valid UUID' })
    }
    if (!['streaming', 'youtube'].includes(tipo)) {
      return res.status(400).json({ error: 'tipo must be streaming or youtube' })
    }
    const trimestreInt = parseInt(trimestre, 10)
    const anioInt = parseInt(anio, 10)
    if (isNaN(trimestreInt) || trimestreInt < 1 || trimestreInt > 4) {
      return res.status(400).json({ error: 'trimestre must be an integer between 1 and 4' })
    }
    if (isNaN(anioInt) || anioInt < 2000 || anioInt > 2100) {
      return res.status(400).json({ error: 'anio must be a valid year' })
    }

    // Validate magic bytes: XLSX files are ZIP archives starting with PK\x03\x04
    const buf = req.file.buffer
    if (buf[0] !== 0x50 || buf[1] !== 0x4B || buf[2] !== 0x03 || buf[3] !== 0x04) {
      return res.status(400).json({ error: 'Invalid file. The file is not a valid .xlsx document.' })
    }

    // Check for duplicate file by filename
    const fileHash = createHash('sha256').update(buf).digest('hex')
    const dupCheck = await pool.query(
      `SELECT r.id, r.tipo, r.trimestre, r.anio, s.nombre AS sello_nombre
       FROM reportes r JOIN sellos s ON s.id = r.sello_id
       WHERE r.nombre_archivo = $1`,
      [req.file.originalname]
    )
    if (dupCheck.rows.length) {
      const dup = dupCheck.rows[0]
      return res.status(409).json({
        error: `Este archivo ya fue subido anteriormente (${dup.sello_nombre} · ${dup.tipo} · Q${dup.trimestre} ${dup.anio}).`,
        duplicado: { id: dup.id, tipo: dup.tipo, trimestre: dup.trimestre, anio: dup.anio, sello: dup.sello_nombre },
      })
    }

    // Check storage limit (8 GB)
    const usedRes = await pool.query('SELECT COALESCE(SUM(file_size), 0) AS used FROM reportes')
    const usedBytes = parseInt(usedRes.rows[0].used)
    if (usedBytes + req.file.size > LIMIT_BYTES) {
      const usedGB = (usedBytes / (1024 ** 3)).toFixed(2)
      return res.status(507).json({
        error: `Storage limit reached. Used: ${usedGB} GB of 8 GB. Delete old reports before uploading.`,
      })
    }

    // Parse Excel
    let parsed = { topSongs: [], totalReg: 0, tipoFinal: tipo, tipoCorregido: false }
    try { parsed = parseExcel(buf, tipo) } catch { /* non-standard format — skip parsing */ }
    const { topSongs, totalReg, tipoFinal, tipoCorregido } = parsed

    // Fetch sello name for readable R2 path
    const selloRow = await pool.query('SELECT nombre FROM sellos WHERE id = $1', [sello_id])
    const selloNombre = selloRow.rows[0]?.nombre ?? sello_id
    const safeSelloNombre = selloNombre.replace(/[^a-zA-Z0-9 _\-áéíóúÁÉÍÓÚñÑ]/g, '').trim()

    // Upload to R2 — fixed path per sello+tipo+trimestre+año so re-uploads overwrite cleanly
    const r2Key = `reportes/${anioInt}/Q${trimestreInt}/${safeSelloNombre}/${tipoFinal}.xlsx`
    await r2.send(new PutObjectCommand({
      Bucket: R2_BUCKET,
      Key: r2Key,
      Body: buf,
      ContentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      ContentDisposition: `attachment; filename="${req.file.originalname}"`,
    }))

    const client = await pool.connect()
    try {
      await client.query('BEGIN')
      const row = await insertReporte(client, {
        sello_id, tipoFinal, nombre_archivo: req.file.originalname,
        r2Key, trimestre: trimestreInt, anio: anioInt, totalReg,
        fileSize: req.file.size, fileHash, topSongs,
      })
      await client.query('COMMIT')
      res.status(201).json({ ...row, tipoCorregido: tipoCorregido ? tipo : null })
    } catch (err) {
      await client.query('ROLLBACK')
      r2.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: r2Key })).catch(() => {})
      throw err
    } finally {
      client.release()
    }
  } catch (err) {
    next(err)
  }
})

// ---------------------------------------------------------------------------
// Bulk upload (admin only) — multiple files, no R2, sello detected from filename
//
// Body (multipart/form-data):
//   files[]   — up to 30 .xlsx files
//   trimestre — 1-4
//   anio      — e.g. 2025
//
// The sello is inferred from each filename:
//   Streaming: "2025-12_{Sello Name}_*.xlsx"
//   YouTube:   "*Youtube_{Sello Name}.xlsx"
//
// Returns:
//   { ok: number, errors: number, results: [{ filename, status, sello, tipo, total_regalias, error? }] }
// ---------------------------------------------------------------------------
router.post('/bulk', requireAdmin, uploadRateLimit, upload.array('files[]', 30), async (req, res, next) => {
  try {
    const { trimestre, anio } = req.body
    if (!req.files?.length) {
      return res.status(400).json({ error: 'At least one file is required' })
    }
    const trimestreInt = parseInt(trimestre, 10)
    const anioInt = parseInt(anio, 10)
    if (isNaN(trimestreInt) || trimestreInt < 1 || trimestreInt > 4) {
      return res.status(400).json({ error: 'trimestre must be an integer between 1 and 4' })
    }
    if (isNaN(anioInt) || anioInt < 2000 || anioInt > 2100) {
      return res.status(400).json({ error: 'anio must be a valid year' })
    }

    // Load all active sellos once for matching
    const sellosRes = await pool.query("SELECT id, nombre FROM sellos WHERE estado = 'activo'")
    const sellos = sellosRes.rows

    const results = []

    for (const file of req.files) {
      const filename = file.originalname

      // Validate magic bytes
      const buf = file.buffer
      if (buf[0] !== 0x50 || buf[1] !== 0x4B || buf[2] !== 0x03 || buf[3] !== 0x04) {
        results.push({ filename, status: 'error', error: 'No es un archivo .xlsx válido' })
        continue
      }

      // Detect tipo, sello, trimestre, anio from filename
      const { tipo, selloRaw, trimestre: fileTrimestre, anio: fileAnio } = parseFilename(filename)
      if (fileTrimestre === null || fileAnio === null) {
        results.push({ filename, status: 'error', tipo, sello_detectado: selloRaw, error: 'No se pudo identificar trimestre y año en el nombre del archivo' })
        continue
      }
      if (fileTrimestre !== trimestreInt || fileAnio !== anioInt) {
        results.push({ filename, status: 'error', tipo, sello_detectado: selloRaw, error: `El trimestre/año en el nombre del archivo (${fileTrimestre}/${fileAnio}) no coincide con los especificados (${trimestreInt}/${anioInt})` })
        continue
      }

      const sello = matchSello(selloRaw, sellos)
      if (!sello) {
        results.push({ filename, status: 'error', tipo, sello_detectado: selloRaw, error: `No se encontró un sello activo que coincida con "${selloRaw}"` })
        continue
      }

      // Duplicate check by filename
      const fileHash = createHash('sha256').update(buf).digest('hex')
      const dupCheck = await pool.query(
        `SELECT r.id, r.tipo, r.trimestre, r.anio FROM reportes r WHERE r.nombre_archivo = $1`,
        [filename]
      )
      if (dupCheck.rows.length) {
        const dup = dupCheck.rows[0]
        results.push({ filename, status: 'skipped', sello: sello.nombre, tipo, error: `Duplicate — already uploaded as Q${dup.trimestre} ${dup.anio} ${dup.tipo}` })
        continue
      }

      // Parse Excel
      let parsed = { topSongs: [], totalReg: 0, tipoFinal: tipo, tipoCorregido: false }
      try { parsed = parseExcel(buf, tipo) } catch { /* non-standard format */ }
      const { topSongs, totalReg, tipoFinal, tipoCorregido } = parsed

      // Upload to R2 — include original filename so each monthly file gets its own path
      const safeSelloNombre = sello.nombre.replace(/[^a-zA-Z0-9 _\-áéíóúÁÉÍÓÚñÑ]/g, '').trim()
      const safeFilename = filename.replace(/[^a-zA-Z0-9 _\-áéíóúÁÉÍÓÚñÑ.]/g, '').trim()
      const r2Key = `reportes/${anioInt}/Q${trimestreInt}/${safeSelloNombre}/${safeFilename}`
      try {
        await r2.send(new PutObjectCommand({
          Bucket: R2_BUCKET,
          Key: r2Key,
          Body: buf,
          ContentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
          ContentDisposition: `attachment; filename="${filename}"`,
        }))
      } catch (r2Err) {
        results.push({ filename, status: 'error', sello: sello.nombre, tipo: tipoFinal, error: `R2 upload failed: ${r2Err.message}` })
        continue
      }

      const client = await pool.connect()
      try {
        await client.query('BEGIN')
        const row = await insertReporte(client, {
          sello_id: sello.id, tipoFinal, nombre_archivo: filename,
          r2Key, trimestre: trimestreInt, anio: anioInt, totalReg,
          fileSize: file.size, fileHash, topSongs,
          upsert: false, // allow multiple monthly files per quarter
        })
        await client.query('COMMIT')
        results.push({
          filename,
          status: 'ok',
          sello: sello.nombre,
          tipo: tipoFinal,
          tipo_corregido: tipoCorregido ? tipo : null,
          total_regalias: totalReg,
          reporte_id: row.id,
        })
      } catch (err) {
        await client.query('ROLLBACK')
        r2.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: r2Key })).catch(() => {})
        results.push({ filename, status: 'error', sello: sello.nombre, tipo: tipoFinal, error: err.message })
      } finally {
        client.release()
      }
    }

    const ok = results.filter(r => r.status === 'ok').length
    const errors = results.filter(r => r.status === 'error').length
    const skipped = results.filter(r => r.status === 'skipped').length
    res.status(ok > 0 ? 201 : 400).json({ ok, errors, skipped, results })
  } catch (err) {
    next(err)
  }
})

// ---------------------------------------------------------------------------
// List reports
// ---------------------------------------------------------------------------
router.get('/', requireAuth, async (req, res, next) => {
  try {
    let query, params = []
    if (req.user.role === 'admin') {
      const conditions = []
      const { sello_id, tipo, anio } = req.query
      if (sello_id) { params.push(sello_id); conditions.push(`r.sello_id = $${params.length}`) }
      if (tipo) { params.push(tipo); conditions.push(`r.tipo = $${params.length}`) }
      if (anio) { params.push(anio); conditions.push(`r.anio = $${params.length}`) }
      const where = conditions.length ? 'WHERE ' + conditions.join(' AND ') : ''
      query = `SELECT r.*, s.nombre AS sello_nombre, s.iniciales
               FROM reportes r JOIN sellos s ON s.id = r.sello_id
               ${where}
               ORDER BY r.created_at DESC`
    } else {
      query = `SELECT r.*, s.nombre AS sello_nombre, s.iniciales
               FROM reportes r JOIN sellos s ON s.id = r.sello_id
               WHERE r.sello_id = $1
               ORDER BY r.created_at DESC`
      params = [req.user.id]
    }
    const result = await pool.query(query, params)
    res.json(result.rows)
  } catch (err) {
    next(err)
  }
})

// ---------------------------------------------------------------------------
// Get signed download URL (5-minute expiry)
// ---------------------------------------------------------------------------
router.get('/:id/download', requireAuth, async (req, res, next) => {
  try {
    if (!UUID_RE.test(req.params.id)) return res.status(400).json({ error: 'Invalid id' })
    const result = await pool.query('SELECT * FROM reportes WHERE id = $1', [req.params.id])
    if (!result.rows.length) return res.status(404).json({ error: 'Report not found' })
    const report = result.rows[0]

    if (req.user.role === 'sello' && report.sello_id !== req.user.id) {
      return res.status(403).json({ error: 'Access denied' })
    }

    const url = await getSignedUrl(
      r2,
      new GetObjectCommand({ Bucket: R2_BUCKET, Key: report.r2_key }),
      { expiresIn: 300 }
    )
    res.json({ url })
  } catch (err) {
    next(err)
  }
})

// ---------------------------------------------------------------------------
// Delete report (admin only)
// ---------------------------------------------------------------------------
router.delete('/:id', requireAdmin, async (req, res, next) => {
  try {
    if (!UUID_RE.test(req.params.id)) return res.status(400).json({ error: 'Invalid id' })

    const result = await pool.query('SELECT * FROM reportes WHERE id = $1', [req.params.id])
    if (!result.rows.length) return res.status(404).json({ error: 'Report not found' })
    const report = result.rows[0]

    const client = await pool.connect()
    try {
      await client.query('BEGIN')

      // Delete report — top_canciones deleted automatically by CASCADE on reporte_id
      await client.query('DELETE FROM reportes WHERE id = $1', [report.id])

      // Update resumen_regalias: check if a sibling report (opposite tipo) still exists
      const sibling = await client.query(
        'SELECT tipo, total_regalias FROM reportes WHERE sello_id=$1 AND trimestre=$2 AND anio=$3',
        [report.sello_id, report.trimestre, report.anio]
      )
      if (sibling.rows.length === 0) {
        // No more reports for this sello+quarter → remove the resumen row entirely
        await client.query(
          'DELETE FROM resumen_regalias WHERE sello_id=$1 AND trimestre=$2 AND anio=$3',
          [report.sello_id, report.trimestre, report.anio]
        )
      } else {
        // Zero out only the column for the deleted tipo
        const col = report.tipo === 'streaming' ? 'total_streaming' : 'total_youtube'
        await client.query(
          `UPDATE resumen_regalias SET ${col} = 0 WHERE sello_id=$1 AND trimestre=$2 AND anio=$3`,
          [report.sello_id, report.trimestre, report.anio]
        )
      }

      await client.query('COMMIT')
    } catch (err) {
      await client.query('ROLLBACK')
      throw err
    } finally {
      client.release()
    }

    // Clean up R2 file after DB transaction succeeds (non-blocking)
    r2.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: report.r2_key })).catch(() => {})
    res.json({ ok: true })
  } catch (err) {
    next(err)
  }
})

export default router
