import express from 'express'
import cors from 'cors'
import dotenv from 'dotenv'
import authRoutes from './routes/auth.js'
import sellosRoutes from './routes/sellos.js'
import reportesRoutes from './routes/reportes.js'
import portalRoutes from './routes/portal.js'

dotenv.config()

const app = express()

app.use(cors({
  origin: process.env.CORS_ORIGIN || '*',
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
