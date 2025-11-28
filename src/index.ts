import express from "express";
import { config } from "dotenv";
import { pool } from "./config/db";
import { fullSync11055 } from "./modules/workflow/fullSync11055";
import { webhook11055 } from "./modules/workflow/webhook11055";
import { fullSync11053Atct } from "./modules/workflow/fullSync11053Atct";
import { webhook11053Atct } from "./modules/workflow/webhook11053Atct";

config();

const app = express();

// CORS & Middleware
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
  res.header(
    "Access-Control-Allow-Headers",
    "Content-Type, Authorization, ngrok-skip-browser-warning"
  );
  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }
  next();
});

// Middleware đọc body
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// --- ROUTES ---

// Health Check
app.get("/health", async (_req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true, db: "connected" });
  } catch (e) {
    res.status(500).json({ ok: false, error: (e as Error).message });
  }
});

// 1. WORKFLOW 11055 (HUYỆT MỘ)
app.post("/api/workflow/11055/full-sync", async (_req, res) => {
  try {
    const total = await fullSync11055();
    res.json({ ok: true, total });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: (e as Error).message });
  }
});
app.post("/api/workflow/11055/webhook", webhook11055);

// 2. WORKFLOW 11053 (AT/CT)
// API này dùng cho Cron Job gọi định kỳ (Scheduled Polling)
app.post("/api/workflow/11053/full-sync-atct", async (_req, res) => {
  try {
    console.log("--> Trigger Full Sync ATCT (Background Mode)");
    
    // FIRE AND FORGET: Chạy ngầm, không chờ kết quả để tránh Timeout
    fullSync11053Atct()
      .then((total) => console.log(`[Background Sync] Done. Total: ${total}`))
      .catch((err) => console.error(`[Background Sync] Failed:`, err));

    // Trả lời OK ngay lập tức
    res.json({ 
      ok: true, 
      message: "Full sync started in background. Check server logs for progress." 
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: (e as Error).message });
  }
});
app.post("/api/workflow/11053/webhook", webhook11053Atct);

// --- PUBLIC APIs (GET DATA) ---

app.get("/api/atct/jobs", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 50, 200);
    const offset = Number(req.query.offset) || 0;
    const khu = req.query.khu ? String(req.query.khu) : null;
    const tieukhu = req.query.tieukhu ? String(req.query.tieukhu) : null;

    const whereClauses: string[] = ["workflow_id = 11053"];
    const params: any[] = [];
    let paramIndex = 1;

    if (khu) {
      whereClauses.push(`position_khu = $${paramIndex++}`);
      params.push(khu);
    }
    if (tieukhu) {
      whereClauses.push(`position_tieukhu = $${paramIndex++}`);
      params.push(tieukhu);
    }

    const whereSql = "WHERE " + whereClauses.join(" AND ");

    const sql = `
      SELECT * FROM workflow_job_atct_min
      ${whereSql}
      ORDER BY time_of_funeral NULLS LAST, time_of_death DESC
      LIMIT $${paramIndex++} OFFSET $${paramIndex++}
    `;
    params.push(limit, offset);

    const result = await pool.query(sql, params);

    res.json({
      ok: true,
      count: result.rowCount,
      limit,
      offset,
      items: result.rows.map((r) => ({
        id: r.id,
        workflow_id: r.workflow_id,
        job_id: r.job_id,
        ceremony_type: r.ceremony_type,
        age_phrase: r.age_phrase,
        position: {
          khu: r.position_khu,
          tieukhu: r.position_tieukhu,
          tieukhu_no: r.position_tieukhu_no,
          row: r.position_row,
          index: r.position_index,
        },
        deceased: {
          name: r.deceased_name,
          birth_year: r.deceased_birth_year,
        },
        time_of_death: r.time_of_death,
        lunar_date: r.lunar_date,
        time_of_funeral: r.time_of_funeral,
        departure_place: r.departure_place,
        updated_at: r.updated_at,
      })),
    });
  } catch (e) {
    console.error("[ATCT] Error GET /api/atct/jobs:", e);
    res.status(500).json({ ok: false, error: (e as Error).message });
  }
});

app.get("/api/atct/jobs/:jobId", async (req, res) => {
  try {
    const jobId = String(req.params.jobId);
    const sql = `SELECT * FROM workflow_job_atct_min WHERE workflow_id = 11053 AND job_id = $1 LIMIT 1`;
    const result = await pool.query(sql, [jobId]);

    if (result.rowCount === 0) {
      return res.status(404).json({ ok: false, error: "Job not found" });
    }
    res.json({ ok: true, item: result.rows[0] });
  } catch (e) {
    console.error("[ATCT] Error GET job detail:", e);
    res.status(500).json({ ok: false, error: (e as Error).message });
  }
});

app.get("/api/atct/events", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 200, 500);
    const offset = Number(req.query.offset) || 0;
    
    const sql = `
      SELECT 
        job_id, ceremony_type, deceased_name, deceased_birth_year, 
        age_phrase, time_of_death, lunar_date, time_of_funeral, 
        departure_place, position_khu, position_tieukhu, 
        position_tieukhu_no, position_row, position_index, updated_at
      FROM workflow_job_atct_min
      WHERE workflow_id = 11053
      ORDER BY time_of_funeral NULLS LAST, time_of_death DESC
      LIMIT $1 OFFSET $2
    `;

    const result = await pool.query(sql, [limit, offset]);
    res.json({ ok: true, items: result.rows });
  } catch (e) {
    console.error("[ATCT] Error GET events:", e);
    res.status(500).json({ ok: false, error: (e as Error).message });
  }
});

const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`\n---------------------------------------------------------`);
  console.log(`🚀 CPHACO Backend is running on port ${port}`);
  console.log(`👉 API Full Sync: POST /api/workflow/11053/full-sync-atct`);
  console.log(`---------------------------------------------------------\n`);
});