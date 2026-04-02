import { Router } from 'express'
import pool from '../config/db.js'
import { requireSello } from '../middleware/auth.js'

const router = Router()

// Dashboard: royalty summary + top songs for the authenticated sello
router.get('/dashboard', requireSello, async (req, res, next) => {
  try {
    const selloId = req.user.id

    // Most recent quarter with data
    const resumenRes = await pool.query(
      `SELECT * FROM resumen_regalias
       WHERE sello_id = $1
       ORDER BY anio DESC, trimestre DESC
       LIMIT 1`,
      [selloId]
    )
    const resumen = resumenRes.rows[0] || { total_streaming: 0, total_youtube: 0, trimestre: null, anio: null }

    // Top 5 songs across all reports for this sello (by royalties)
    const topRes = await pool.query(
      `SELECT tc.titulo, tc.artista, tc.reproducciones, tc.regalias, tc.posicion
       FROM top_canciones tc
       JOIN reportes r ON r.id = tc.reporte_id
       WHERE r.sello_id = $1
       ORDER BY tc.regalias DESC
       LIMIT 5`,
      [selloId]
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
