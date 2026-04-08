import { Router } from 'express'
import { createHash } from 'crypto'
import multer from 'multer'
import { PutObjectCommand, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import * as XLSX from 'xlsx'
import pool from '../config/db.js'
import { r2, R2_BUCKET } from '../config/r2.js'
import { requireAdmin, requireAuth } from '../middleware/auth.js'

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 }, // 20 MB
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
const RATE_WINDOW_MS  = 60 * 60 * 1000 // 1 hour
const RATE_LIMIT_MAX  = 20

function uploadRateLimit(req, res, next) {
  const key = req.user?.id || req.ip
  const now = Date.now()
  let record = uploadAttempts.get(key)
  if (!record || now > record.resetAt) {
    record = { count: 0, resetAt: now + RATE_WINDOW_MS }
  }
  record.count++
  uploadAttempts.set(key, record)
  if (record.count > RATE_LIMIT_MAX) {
    return res.status(429).json({ error: 'Too many upload attempts. Please try again in an hour.' })
  }
  next()
}

const LIMIT_BYTES = 8 * 1024 * 1024 * 1024 // 8 GB

const router = Router()

// Storage usage (admin only) — must be before /:id routes
router.get('/storage', requireAdmin, async (_req, res, next) => {
  try {
    const result = await pool.query(
      'SELECT COALESCE(SUM(file_size), 0) AS used, COUNT(*) AS total_files FROM reportes'
    )
    const usedBytes  = parseInt(result.rows[0].used)
    const totalFiles = parseInt(result.rows[0].total_files)
    res.json({
      used_bytes:  usedBytes,
      limit_bytes: LIMIT_BYTES,
      used_gb:     (usedBytes / (1024 ** 3)).toFixed(3),
      limit_gb:    8,
      pct:         Math.min(Math.round((usedBytes / LIMIT_BYTES) * 100), 100),
      total_files: totalFiles,
    })
  } catch (err) {
    next(err)
  }
})

// Upload report (admin only)
router.post('/', requireAdmin, uploadRateLimit, upload.single('file'), async (req, res, next) => {
  try {
    const { sello_id, tipo, trimestre, anio } = req.body
    if (!req.file || !sello_id || !tipo || !trimestre || !anio) {
      return res.status(400).json({ error: 'file, sello_id, tipo, trimestre, anio are required' })
    }
    if (!['streaming', 'youtube'].includes(tipo)) {
      return res.status(400).json({ error: 'tipo must be streaming or youtube' })
    }

    // Validate magic bytes: XLSX files are ZIP archives starting with PK\x03\x04
    const buf = req.file.buffer
    if (buf[0] !== 0x50 || buf[1] !== 0x4B || buf[2] !== 0x03 || buf[3] !== 0x04) {
      return res.status(400).json({ error: 'Invalid file. The file is not a valid .xlsx document.' })
    }

    // Check for duplicate file by content hash
    const fileHash = createHash('sha256').update(buf).digest('hex')
    const dupCheck = await pool.query(
      `SELECT r.id, r.tipo, r.trimestre, r.anio, s.nombre AS sello_nombre
       FROM reportes r JOIN sellos s ON s.id = r.sello_id
       WHERE r.file_hash = $1`,
      [fileHash]
    )
    if (dupCheck.rows.length) {
      const dup = dupCheck.rows[0]
      return res.status(409).json({
        error: `Este archivo ya fue subido anteriormente (${dup.sello_nombre} · ${dup.tipo} · Q${dup.trimestre} ${dup.anio}).`,
        duplicado: { id: dup.id, tipo: dup.tipo, trimestre: dup.trimestre, anio: dup.anio, sello: dup.sello_nombre },
      })
    }

    // Check storage limit (8 GB)
    const usedRes  = await pool.query('SELECT COALESCE(SUM(file_size), 0) AS used FROM reportes')
    const usedBytes = parseInt(usedRes.rows[0].used)
    if (usedBytes + req.file.size > LIMIT_BYTES) {
      const usedGB = (usedBytes / (1024 ** 3)).toFixed(2)
      return res.status(507).json({
        error: `Storage limit reached. Used: ${usedGB} GB of 8 GB. Delete old reports before uploading.`,
      })
    }

    // Parse Excel FIRST to detect real type before uploading to R2
    let topSongs = []
    let totalReg = 0
    let tipoFinal = tipo
    let tipoCorregido = false
    try {
      const wb = XLSX.read(req.file.buffer, { type: 'buffer' })
      const ws = wb.Sheets[wb.SheetNames[0]]
      const data = XLSX.utils.sheet_to_json(ws)

      // Normalize column names: trim surrounding spaces
      const normalized = data.map(row =>
        Object.fromEntries(Object.entries(row).map(([k, v]) => [k.trim(), v]))
      )

      console.log('[Excel] tipo enviado:', tipo)
      console.log('[Excel] total filas:', normalized.length)
      console.log('[Excel] columnas normalizadas:', normalized.length > 0 ? Object.keys(normalized[0]) : 'sin datos')

      // Auto-detect real file type by column names
      const cols = normalized.length > 0 ? Object.keys(normalized[0]).map(c => c.toLowerCase()) : []
      const looksStreaming = cols.some(c => ['total_earned_usd', 'total_earned', 'streams', 'song_title'].includes(c))
      const looksYoutube   = cols.some(c => ['total net earnings', 'ad total views', 'asset title'].includes(c))
      const detectedTipo   = looksYoutube ? 'youtube' : looksStreaming ? 'streaming' : null

      if (detectedTipo && detectedTipo !== tipo) {
        tipoFinal = detectedTipo
        tipoCorregido = true
        console.log(`[Excel] tipo corregido automáticamente: ${tipo} → ${tipoFinal}`)
      }

      if (tipoFinal === 'streaming') {
        // Streaming: reporting_month/label/isrc/song_title/artist/streams/total_earned_usd
        const map = new Map()
        for (const row of normalized) {
          const earnings = parseFloat(row['total_earned_usd'] ?? row['total_earned'] ?? 0) || 0
          totalReg += earnings
          const isrc    = String(row['isrc'] ?? row['ISRC'] ?? '')
          const titulo  = String(row['song_title'] ?? row['title'] ?? 'Track')
          const artista = String(row['artist'] ?? '')
          const key     = isrc || titulo
          if (!map.has(key)) {
            map.set(key, { titulo, artista, isrc: isrc || null, reproducciones: 0, regalias: 0 })
          }
          const entry = map.get(key)
          entry.reproducciones += parseInt(row['streams'] ?? 0) || 0
          entry.regalias       += earnings
        }
        topSongs = [...map.values()]
          .sort((a, b) => b.regalias - a.regalias)
          .slice(0, 10)
          .map((s, i) => ({ ...s, posicion: i + 1 }))

      } else {
        // YouTube Content ID: Asset Title/ISRC/Artist/Total Net Earnings/AD Total Views
        const map = new Map()
        for (const row of normalized) {
          const earnings = parseFloat(row['Total Net Earnings'] ?? 0) || 0
          totalReg += earnings
          const isrc    = String(row['ISRC'] ?? row['Custom ID'] ?? '')
          const titulo  = String(row['Asset Title'] ?? row['title'] ?? 'Track')
          const artista = String(row['Artist'] ?? '')
          const key     = isrc || titulo
          if (!map.has(key)) {
            map.set(key, { titulo, artista, isrc: isrc || null, reproducciones: 0, regalias: 0 })
          }
          const entry = map.get(key)
          entry.reproducciones += parseInt(row['AD Total Views'] ?? 0) || 0
          entry.regalias       += earnings
        }
        topSongs = [...map.values()]
          .sort((a, b) => b.regalias - a.regalias)
          .slice(0, 10)
          .map((s, i) => ({ ...s, posicion: i + 1 }))
      }

      totalReg = Math.round(totalReg * 100) / 100
      console.log('[Excel] totalReg calculado:', totalReg)
      console.log('[Excel] top canciones:', topSongs)
    } catch { /* non-standard format — skip parsing */ }

    // Upload to R2 using the (possibly corrected) tipo
    const r2Key = `reportes/${sello_id}/${tipoFinal}/${anio}-Q${trimestre}-${Date.now()}.xlsx`
    await r2.send(new PutObjectCommand({
      Bucket: R2_BUCKET,
      Key: r2Key,
      Body: req.file.buffer,
      ContentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      ContentDisposition: `attachment; filename="${req.file.originalname}"`,
    }))

    const client = await pool.connect()
    try {
      await client.query('BEGIN')

      // Upsert reporte (one per sello+tipo+trimestre+anio)
      const rep = await client.query(
        `INSERT INTO reportes (sello_id, tipo, nombre_archivo, r2_key, trimestre, anio, total_regalias, file_size, file_hash)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
         ON CONFLICT (sello_id, tipo, trimestre, anio) DO UPDATE
           SET nombre_archivo = EXCLUDED.nombre_archivo,
               r2_key         = EXCLUDED.r2_key,
               total_regalias = EXCLUDED.total_regalias,
               file_size      = EXCLUDED.file_size,
               file_hash      = EXCLUDED.file_hash,
               created_at     = NOW()
         RETURNING *`,
        [sello_id, tipoFinal, req.file.originalname, r2Key, trimestre, anio, totalReg, req.file.size, fileHash]
      )
      const repId = rep.rows[0].id

      // Replace top songs for this report
      await client.query('DELETE FROM top_canciones WHERE reporte_id = $1', [repId])
      for (const s of topSongs) {
        await client.query(
          `INSERT INTO top_canciones (reporte_id, posicion, titulo, artista, isrc, reproducciones, regalias)
           VALUES ($1,$2,$3,$4,$5,$6,$7)`,
          [repId, s.posicion, s.titulo, s.artista, s.isrc, s.reproducciones, s.regalias]
        )
      }

      // Upsert resumen_regalias
      await client.query(
        `INSERT INTO resumen_regalias (sello_id, trimestre, anio, total_streaming, total_youtube)
         VALUES ($1, $2, $3,
           CASE WHEN $4 = 'streaming' THEN $5::numeric ELSE 0 END,
           CASE WHEN $4 = 'youtube'   THEN $5::numeric ELSE 0 END
         )
         ON CONFLICT (sello_id, trimestre, anio) DO UPDATE SET
           total_streaming = CASE WHEN $4 = 'streaming' THEN $5::numeric ELSE resumen_regalias.total_streaming END,
           total_youtube   = CASE WHEN $4 = 'youtube'   THEN $5::numeric ELSE resumen_regalias.total_youtube   END`,
        [sello_id, trimestre, anio, tipoFinal, totalReg]
      )

      await client.query('COMMIT')
      res.status(201).json({ ...rep.rows[0], tipoCorregido: tipoCorregido ? tipo : null })
    } catch (err) {
      await client.query('ROLLBACK')
      // Try to remove the R2 object we just uploaded
      r2.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: r2Key })).catch(() => {})
      throw err
    } finally {
      client.release()
    }
  } catch (err) {
    next(err)
  }
})

// List reports
router.get('/', requireAuth, async (req, res, next) => {
  try {
    let query, params = []
    if (req.user.role === 'admin') {
      const conditions = []
      const { sello_id, tipo, anio } = req.query
      if (sello_id) { params.push(sello_id); conditions.push(`r.sello_id = $${params.length}`) }
      if (tipo)     { params.push(tipo);     conditions.push(`r.tipo = $${params.length}`) }
      if (anio)     { params.push(anio);     conditions.push(`r.anio = $${params.length}`) }
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

// Get signed download URL (5-minute expiry)
router.get('/:id/download', requireAuth, async (req, res, next) => {
  try {
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

// Delete report (admin only)
router.delete('/:id', requireAdmin, async (req, res, next) => {
  try {
    const result = await pool.query('SELECT * FROM reportes WHERE id = $1', [req.params.id])
    if (!result.rows.length) return res.status(404).json({ error: 'Report not found' })
    const report = result.rows[0]
    r2.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: report.r2_key })).catch(() => {})
    await pool.query('DELETE FROM reportes WHERE id = $1', [req.params.id])
    res.json({ ok: true })
  } catch (err) {
    next(err)
  }
})

export default router
