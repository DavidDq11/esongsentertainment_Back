import { Router } from 'express'
import pool from '../config/db.js'
import { requireSello } from '../middleware/auth.js'

const router = Router()

// Dashboard: royalty summary + top songs for the authenticated sello
router.get('/dashboard', requireSello, async (req, res, next) => {
  try {
    const selloId = req.user.id

    // Find the most recent period (trimestre+anio) that has any report for this sello
    const periodoRes = await pool.query(
      `SELECT anio, trimestre FROM reportes
       WHERE sello_id = $1
       ORDER BY anio DESC, trimestre DESC
       LIMIT 1`,
      [selloId]
    )
    const periodo = periodoRes.rows[0] || null

    // Sum streaming and youtube for that exact period
    const resumenRes = await pool.query(
      `SELECT
         SUM(CASE WHEN tipo = 'streaming' THEN total_regalias ELSE 0 END) AS total_streaming,
         SUM(CASE WHEN tipo = 'youtube'   THEN total_regalias ELSE 0 END) AS total_youtube
       FROM reportes
       WHERE sello_id = $1 AND anio = $2 AND trimestre = $3`,
      [selloId, periodo?.anio ?? 0, periodo?.trimestre ?? 0]
    )
    const resumen = {
      total_streaming: resumenRes.rows[0]?.total_streaming ?? 0,
      total_youtube:   resumenRes.rows[0]?.total_youtube   ?? 0,
      trimestre: periodo?.trimestre ?? null,
      anio:      periodo?.anio      ?? null,
    }

    // Top 5 songs from reports within that same period (both streaming and youtube)
    const topRes = await pool.query(
      `SELECT tc.titulo, tc.artista, tc.reproducciones, tc.regalias, tc.posicion
       FROM top_canciones tc
       JOIN reportes r ON r.id = tc.reporte_id
       WHERE r.sello_id = $1 AND r.anio = $2 AND r.trimestre = $3
       ORDER BY tc.regalias DESC
       LIMIT 5`,
      [selloId, periodo?.anio ?? 0, periodo?.trimestre ?? 0]
    )

    // Compute percentages relative to max
    const songs = topRes.rows
    const maxStreams = songs.reduce((m, s) => Math.max(m, Number(s.reproducciones)), 1)
    const topSongs = songs.map(s => ({
      ...s,
      pct: Math.round((Number(s.reproducciones) / maxStreams) * 100),
    }))

    // Sello info
    const selloRes = await pool.query(
      'SELECT nombre, iniciales FROM sellos WHERE id = $1',
      [selloId]
    )

    const streaming = parseFloat(resumen.total_streaming) || 0
    const youtube   = parseFloat(resumen.total_youtube)   || 0

    res.json({
      sello: selloRes.rows[0],
      resumen: {
        streaming,
        youtube,
        total: streaming + youtube,
        trimestre: resumen.trimestre,
        anio: resumen.anio,
      },
      topSongs,
    })
  } catch (err) {
    next(err)
  }
})

export default router
