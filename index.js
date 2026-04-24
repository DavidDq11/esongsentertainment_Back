import express from 'express'
import cors from 'cors'
import helmet from 'helmet'
import dotenv from 'dotenv'
import authRoutes from './routes/auth.js'
import sellosRoutes from './routes/sellos.js'
import reportesRoutes from './routes/reportes.js'
import portalRoutes from './routes/portal.js'

dotenv.config()

if (!process.env.CORS_ORIGIN) {
  console.warn('[WARN] CORS_ORIGIN not set — defaulting to localhost:3000. Set CORS_ORIGIN in production.')
}

const app = express()

app.use(helmet())
app.use(cors({
  origin: process.env.CORS_ORIGIN || 'http://localhost:3000',
  credentials: true,
}))
app.use(express.json())

app.use('/api/auth',     authRoutes)
app.use('/api/sellos',   sellosRoutes)
app.use('/api/reportes', reportesRoutes)
app.use('/api/portal',   portalRoutes)

app.get('/api/health', (_req, res) => res.json({ ok: true }))

// Global error handler
app.use((err, _req, res, _next) => {
  console.error(err)
  res.status(500).json({ error: 'Internal server error' })
})

const PORT = process.env.PORT || 4000
app.listen(PORT, () => console.log(`esongs-api listening on port ${PORT}`))
