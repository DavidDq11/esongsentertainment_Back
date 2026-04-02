import bcrypt from 'bcryptjs'
import pool from './config/db.js'
import dotenv from 'dotenv'

dotenv.config()

const nombre   = 'Administrador'
const email    = 'admin@esongs.com'   // <-- cambia esto
const password = 'admin123'           // <-- cambia esto

const hash = await bcrypt.hash(password, 10)

await pool.query(
  'INSERT INTO admins (nombre, email, password_hash) VALUES ($1, $2, $3)',
  [nombre, email, hash]
)

console.log('Admin creado:', email)
process.exit()
