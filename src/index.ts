import express from "express";
import { config } from "dotenv";
import { pool } from "./config/db";
import { fullSync11055 } from "./modules/workflow/fullSync11055";
import { webhook11055 } from "./modules/workflow/webhook11055";
import { fullSync11053Atct } from "./modules/workflow/fullSync11053Atct";
import { webhook11053Atct } from "./modules/workflow/webhook11053Atct";
// import { QueryResult } from "pg"; // không dùng tới
// import cors from "cors";

config();

const app = express();

// CORS đơn giản
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header(
    "Access-Control-Allow-Methods",
    "GET,POST,PUT,DELETE,OPTIONS"
  );
  res.header(
    "Access-Control-Allow-Headers",
    "Content-Type, Authorization, ngrok-skip-browser-warning"
  );

  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }
  next();
});

app.use(express.json());

// Health check – test DB ok chưa
app.get("/health", async (_req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: (e as Error).message });
  }
});

// API để chạy full sync lần đầu cho 11055
app.post("/api/workflow/11055/full-sync", async (_req, res) => {
  try {
    const total = await fullSync11055();
    res.json({ ok: true, total });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: (e as Error).message });
  }
});

// Webhook cho workflow 11055
app.post("/api/workflow/11055/webhook", webhook11055);

// Webhook cho workflow 11053 (AT/CT)
app.post("/api/workflow/11053/webhook", webhook11053Atct);

// API để chạy full sync lần đầu cho 11053 AT/CT
app.post("/api/workflow/11053/full-sync-atct", async (_req, res) => {
  try {
    const total = await fullSync11053Atct();
    res.json({ ok: true, workflow_id: 11053, total });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: (e as Error).message });
  }
});

/**
 * API trả danh sách AT/CT đang mở (từ workflow_job_atct_min)
 * - Dùng cho các màn hình quản trị / filter theo khu / tiểu khu
 */
app.get("/api/atct/jobs", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 50, 200); // max 200
    const offset = Number(req.query.offset) || 0;
    const khu = req.query.khu ? String(req.query.khu) : null;           // ví dụ F, G...
    const tieukhu = req.query.tieukhu ? String(req.query.tieukhu) : null; // ví dụ F6.1

    // WHERE động, vẫn dùng parameterized query để an toàn
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

    const whereSql = whereClauses.length ? "WHERE " + whereClauses.join(" AND ") : "";

    const sql = `
      SELECT
        id,
        workflow_id,
        job_id,
        ceremony_type,
        position_khu,
        position_tieukhu,
        position_tieukhu_no,
        position_row,
        position_index,
        deceased_name,
        deceased_birth_year,
        age_phrase,
        time_of_death,
        lunar_date,
        time_of_funeral,
        departure_place,
        updated_at
      FROM workflow_job_atct_min
      ${whereSql}
      ORDER BY time_of_funeral NULLS LAST, time_of_death DESC
      LIMIT $${paramIndex++}
      OFFSET $${paramIndex++};
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
        ceremony_type: r.ceremony_type,   // AN TÁNG / CẢI TÁNG / ...
        age_phrase: r.age_phrase,         // "Hưởng dương 45 tuổi" / "Hưởng thọ 88 tuổi"...

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
        // thêm field phẳng cho frontend nào thích dùng trực tiếp
        name: r.deceased_name,
        birth_year: r.deceased_birth_year,

        time_of_death: r.time_of_death,
        lunar_date: r.lunar_date,
        time_of_funeral: r.time_of_funeral,
        departure_place: r.departure_place,
        updated_at: r.updated_at,
      })),
    });
  } catch (e) {
    console.error("[ATCT] Error in GET /api/atct/jobs:", e);
    res.status(500).json({ ok: false, error: (e as Error).message });
  }
});

/**
 * API lấy chi tiết 1 job AT/CT theo job_id
 * - Dùng cho trang cáo phó riêng: atct-obit.html?job=...
 */
app.get("/api/atct/jobs/:jobId", async (req, res) => {
  try {
    const jobId = String(req.params.jobId);

    const sql = `
      SELECT
        id,
        workflow_id,
        job_id,
        ceremony_type,
        position_khu,
        position_tieukhu,
        position_tieukhu_no,
        position_row,
        position_index,
        deceased_name,
        deceased_birth_year,
        age_phrase,
        time_of_death,
        lunar_date,
        time_of_funeral,
        departure_place,
        updated_at
      FROM workflow_job_atct_min
      WHERE workflow_id = 11053 AND job_id = $1
      LIMIT 1;
    `;

    const result = await pool.query(sql, [jobId]);

    if (result.rowCount === 0) {
      return res.status(404).json({ ok: false, error: "Job not found" });
    }

    const r = result.rows[0];
    res.json({
      ok: true,
      item: {
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
        name: r.deceased_name,
        birth_year: r.deceased_birth_year,

        time_of_death: r.time_of_death,
        lunar_date: r.lunar_date,
        time_of_funeral: r.time_of_funeral,
        departure_place: r.departure_place,
        updated_at: r.updated_at,
      },
    });
  } catch (e) {
    console.error("[ATCT] Error in GET /api/atct/jobs/:jobId:", e);
    res.status(500).json({ ok: false, error: (e as Error).message });
  }
});

/**
 * API đơn giản cho frontend public: danh sách sự kiện AT/CT
 * - atct-index.html đang gọi /api/atct/events
 */
app.get("/api/atct/events", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 200, 500);
    const offset = Number(req.query.offset) || 0;

    const sql = `
      SELECT
        id,
        workflow_id,
        job_id,
        ceremony_type,
        position_khu,
        position_tieukhu,
        position_tieukhu_no,
        position_row,
        position_index,
        deceased_name,
        deceased_birth_year,
        age_phrase,
        time_of_death,
        lunar_date,
        time_of_funeral,
        departure_place,
        updated_at
      FROM workflow_job_atct_min
      WHERE workflow_id = 11053
      ORDER BY time_of_funeral NULLS LAST, time_of_death DESC
      LIMIT $1 OFFSET $2;
    `;

    const result = await pool.query(sql, [limit, offset]);

    res.json({
      ok: true,
      items: result.rows.map((r) => ({
        id: r.id,
        job_id: r.job_id,

        ceremony_type: r.ceremony_type,
        age_phrase: r.age_phrase,

        name: r.deceased_name,
        birth_year: r.deceased_birth_year,
        deceased_birth_year: r.deceased_birth_year, // để frontend dùng cả 2 key

        time_of_death: r.time_of_death,
        lunar_date: r.lunar_date,
        time_of_funeral: r.time_of_funeral,
        departure_place: r.departure_place,
        position: {
          khu: r.position_khu,
          tieukhu: r.position_tieukhu,
          tieukhu_no: r.position_tieukhu_no,
          row: r.position_row,
          index: r.position_index,
        },
        updated_at: r.updated_at,
      })),
    });
  } catch (e) {
    console.error("[ATCT] /api/atct/events error:", e);
    res.status(500).json({ ok: false, error: (e as Error).message });
  }
});

console.log("WORKFLOW_ID", process.env.WORKFLOW_ID);
console.log("BASE_DOMAIN", process.env.BASE_DOMAIN);
console.log(
  "BASE_ACCESS_TOKEN",
  process.env.BASE_ACCESS_TOKEN
    ? process.env.BASE_ACCESS_TOKEN.slice(0, 10) + "..."
    : "undefined"
);

const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`CPHACO backend listening on ${port}`);
});
