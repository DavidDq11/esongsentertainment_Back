import { Router } from 'express'
import bcrypt from 'bcryptjs'
import { DeleteObjectCommand } from '@aws-sdk/client-s3'
import pool from '../config/db.js'
import { r2, R2_BUCKET } from '../config/r2.js'
import { requireAdmin } from '../middleware/auth.js'

const router = Router()

// List all sellos with stats
router.get('/', requireAdmin, async (_req, res, next) => {
  try {
    const result = await pool.query(`
      SELECT
        s.id, s.nombre, s.representante, s.email, s.pais,
        s.telefono, s.iniciales, s.estado,
        COALESCE(COUNT(r.id), 0)::int         AS total_reportes,
        COALESCE(SUM(r.total_regalias), 0)    AS total_regalias
      FROM sellos s
      LEFT JOIN reportes r ON r.sello_id = s.id
      GROUP BY s.id
      ORDER BY s.nombre
    `)
    res.json(result.rows)
  } catch (err) {
    next(err)
  }
})

// Create sello
router.post('/', requireAdmin, async (req, res, next) => {
  try {
    const { nombre, representante, email, password, pais, telefono, iniciales } = req.body
    if (!nombre || !email || !password) {
      return res.status(400).json({ error: 'nombre, email, and password are required' })
    }
    if (iniciales && iniciales.length > 20) {
      return res.status(400).json({ error: 'iniciales must be 20 characters or less' })
    }
    const hash = await bcrypt.hash(password, 10)
    const result = await pool.query(
      `INSERT INTO sellos
         (nombre, representante, email, password_hash, pais, telefono, iniciales, creado_por)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
       RETURNING id, nombre, representante, email, pais, telefono, iniciales, estado`,
      [nombre, representante || null, email.toLowerCase().trim(), hash,
       pais || null, telefono || null, iniciales || null, req.user.id]
    )
    res.status(201).json(result.rows[0])
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ error: 'Email already registered' })
    next(err)
  }
})

// Update sello
router.put('/:id', requireAdmin, async (req, res, next) => {
  try {
    const { nombre, representante, email, pais, telefono, iniciales, estado, password } = req.body
    let result
    if (password) {
      const hash = await bcrypt.hash(password, 10)
      result = await pool.query(
        `UPDATE sellos
         SET nombre=$1, representante=$2, email=$3, pais=$4, telefono=$5,
             iniciales=$6, estado=$7, password_hash=$8
         WHERE id=$9
         RETURNING id, nombre, representante, email, pais, telefono, iniciales, estado`,
        [nombre, representante, email, pais, telefono, iniciales, estado, hash, req.params.id]
      )
    } else {
      result = await pool.query(
        `UPDATE sellos
         SET nombre=$1, representante=$2, email=$3, pais=$4, telefono=$5,
             iniciales=$6, estado=$7
         WHERE id=$8
         RETURNING id, nombre, representante, email, pais, telefono, iniciales, estado`,
        [nombre, representante, email, pais, telefono, iniciales, estado, req.params.id]
      )
    }
    if (!result.rows.length) return res.status(404).json({ error: 'Not found' })
    res.json(result.rows[0])
  } catch (err) {
    next(err)
  }
})

// Toggle estado (activo / inactivo)
router.patch('/:id/estado', requireAdmin, async (req, res, next) => {
  try {
    const { estado } = req.body
    if (!['activo', 'inactivo'].includes(estado)) {
      return res.status(400).json({ error: 'estado must be activo or inactivo' })
    }
    const result = await pool.query(
      `UPDATE sellos SET estado = $1 WHERE id = $2
       RETURNING id, nombre, representante, email, pais, telefono, iniciales, estado`,
      [estado, req.params.id]
    )
    if (!result.rows.length) return res.status(404).json({ error: 'Not found' })
    res.json(result.rows[0])
  } catch (err) {
    next(err)
  }
})

// Hard delete sello (removes row + cascades reportes/resumen/top_canciones + cleans R2 files)
router.delete('/:id', requireAdmin, async (req, res, next) => {
  try {
    // Collect R2 keys before cascade delete
    const repsRes = await pool.query(
      'SELECT r2_key FROM reportes WHERE sello_id = $1',
      [req.params.id]
    )

    // Delete the sello (CASCADE handles DB children)
    const del = await pool.query(
      'DELETE FROM sellos WHERE id = $1 RETURNING id',
      [req.params.id]
    )
    if (!del.rows.length) return res.status(404).json({ error: 'Not found' })

    // Clean up R2 files in background (non-blocking)
    for (const { r2_key } of repsRes.rows) {
      r2.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: r2_key })).catch(() => {})
    }

    res.json({ ok: true })
  } catch (err) {
    next(err)
  }
})

export default router
