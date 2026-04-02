import { Router } from 'express'
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

const router = Router()

// Upload report (admin only)
router.post('/', requireAdmin, upload.single('file'), async (req, res, next) => {
  try {
    const { sello_id, tipo, trimestre, anio, total_regalias } = req.body
    if (!req.file || !sello_id || !tipo || !trimestre || !anio) {
      return res.status(400).json({ error: 'file, sello_id, tipo, trimestre, anio are required' })
    }
    if (!['streaming', 'youtube'].includes(tipo)) {
      return res.status(400).json({ error: 'tipo must be streaming or youtube' })
    }

    const r2Key = `reportes/${sello_id}/${tipo}/${anio}-Q${trimestre}-${Date.now()}.xlsx`

    // Upload to R2
    await r2.send(new PutObjectCommand({
      Bucket: R2_BUCKET,
      Key: r2Key,
      Body: req.file.buffer,
      ContentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      ContentDisposition: `attachment; filename="${req.file.originalname}"`,
    }))

    // Parse top songs from Excel (best-effort)
    let topSongs = []
    try {
      const wb = XLSX.read(req.file.buffer, { type: 'buffer' })
      const ws = wb.Sheets[wb.SheetNames[0]]
      const data = XLSX.utils.sheet_to_json(ws)
      topSongs = data.slice(0, 10).map((row, i) => ({
        posicion: i + 1,
        titulo: String(row.titulo ?? row.title ?? row.Titulo ?? row.Title ?? row.TITLE ?? `Track ${i + 1}`),
        artista: String(row.artista ?? row.artist ?? row.Artista ?? row.Artist ?? ''),
        isrc: row.isrc ?? row.ISRC ?? null,
        reproducciones: parseInt(row.reproducciones ?? row.streams ?? row.Streams ?? row.plays ?? 0) || 0,
        regalias: parseFloat(row.regalias ?? row.royalties ?? row.Royalties ?? 0) || 0,
      }))
    } catch { /* non-standard format — skip top songs */ }

    const totalReg = parseFloat(total_regalias) || 0

    const client = await pool.connect()
    try {
      await client.query('BEGIN')

      // Upsert reporte (one per sello+tipo+trimestre+anio)
      const rep = await client.query(
        `INSERT INTO reportes (sello_id, tipo, nombre_archivo, r2_key, trimestre, anio, total_regalias)
         VALUES ($1,$2,$3,$4,$5,$6,$7)
         ON CONFLICT (sello_id, tipo, trimestre, anio) DO UPDATE
           SET nombre_archivo = EXCLUDED.nombre_archivo,
               r2_key         = EXCLUDED.r2_key,
               total_regalias = EXCLUDED.total_regalias,
               created_at     = NOW()
         RETURNING *`,
        [sello_id, tipo, req.file.originalname, r2Key, trimestre, anio, totalReg]
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
        [sello_id, trimestre, anio, tipo, totalReg]
      )

      await client.query('COMMIT')
      res.status(201).json(rep.rows[0])
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
