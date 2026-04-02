import { Router } from 'express'
import bcrypt from 'bcryptjs'
import jwt from 'jsonwebtoken'
import pool from '../config/db.js'

const router = Router()

router.post('/login', async (req, res, next) => {
  try {
    const { email, password } = req.body
    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password are required' })
    }

    // Try admin table first
    const adminRes = await pool.query(
      'SELECT * FROM admins WHERE email = $1',
      [email.toLowerCase().trim()]
    )
    if (adminRes.rows.length > 0) {
      const admin = adminRes.rows[0]
      const ok = await bcrypt.compare(password, admin.password_hash)
      if (!ok) return res.status(401).json({ error: 'Invalid credentials' })
      const token = jwt.sign(
        { role: 'admin', id: admin.id, nombre: admin.nombre },
        process.env.JWT_SECRET,
        { expiresIn: '8h' }
      )
      return res.json({ token, role: 'admin', nombre: admin.nombre })
    }

    // Try sello table
    const selloRes = await pool.query(
      "SELECT * FROM sellos WHERE email = $1 AND estado = 'activo'",
      [email.toLowerCase().trim()]
    )
    if (selloRes.rows.length > 0) {
      const sello = selloRes.rows[0]
      const ok = await bcrypt.compare(password, sello.password_hash)
      if (!ok) return res.status(401).json({ error: 'Invalid credentials' })
      const token = jwt.sign(
        { role: 'sello', id: sello.id, nombre: sello.nombre, iniciales: sello.iniciales },
        process.env.JWT_SECRET,
        { expiresIn: '8h' }
      )
      return res.json({
        token,
        role: 'sello',
        nombre: sello.nombre,
        iniciales: sello.iniciales,
      })
    }

    res.status(401).json({ error: 'Invalid credentials' })
  } catch (err) {
    next(err)
  }
})

export default router
