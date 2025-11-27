/************************************************************
 * BASE Workflow 11053 → Neon (workflow_job_atct_min)
 * - Chỉ sync các job CHƯA HOÀN THÀNH (status ≠ 10)
 ************************************************************/

import fetch from "node-fetch";
import { pool } from "../../config/db";

const WORKFLOW_ID = 11053;
const BASE_DOMAIN = process.env.BASE_DOMAIN || "base.vn";

// Dùng CHUNG BASE_ACCESS_TOKEN từ .env
const BASE_ACCESS_TOKEN = (process.env.BASE_ACCESS_TOKEN || "").trim();
const PAGE_SIZE = 50;

if (!BASE_ACCESS_TOKEN) {
  console.warn("[11053-ATCT] ⚠ BASE_ACCESS_TOKEN is empty");
} else {
  console.log(
    "[11053-ATCT] Using BASE_ACCESS_TOKEN prefix:",
    BASE_ACCESS_TOKEN.slice(0, 15) + "..."
  );
}

interface BaseJob {
  id: string | number;
  title?: string;
  status?: string | number; // 👈 trạng thái job
}

/* ───────── Helpers status ───────── */

function toNumOrNull(x: any): number | null {
  const n = Number(x);
  return Number.isNaN(n) ? null : n;
}

// status = 10 coi như "Hoàn thành"
function isCompletedStatus(st: any): boolean {
  const n = toNumOrNull(st);
  const s = String(st).toLowerCase();
  return n === 10 || s === "10" || s === "done" || s === "completed";
}

/** Parse title dạng "Label: Value &middot; Label2:: Value2 ..." */
function parseTitleToMap(title: string | undefined): Record<string, string> {
  const result: Record<string, string> = {};
  if (!title) return result;

  const parts = title.split("&middot;");

  for (let rawPart of parts) {
    let part = rawPart.trim();
    if (!part) continue;

    const segments = part.split(":");
    if (segments.length < 2) continue;

    const key = segments[0].trim();
    let value = segments.slice(1).join(":").trim();
    value = value.replace(/^:+\s*/, "");

    if (key) {
      result[key] = value;
    }
  }

  return result;
}

/** Parse "27/11/2025 09:39" → ISO (cho timestamptz) */
function parseVnDateTime(input: any): string | null {
  if (!input) return null;
  const s = String(input).trim();
  if (!s) return null;

  const [datePart, timePart] = s.split(" ");
  if (!datePart) return null;

  const [d, m, y] = datePart.split(/[\/\-]/).map((x) => Number(x));
  if (!d || !m || !y) return null;

  let hh = 0;
  let mm = 0;

  if (timePart) {
    const [hhStr, mmStr] = timePart.split(":");
    hh = Number(hhStr) || 0;
    mm = Number(mmStr) || 0;
  }

  const iso = new Date(Date.UTC(y, m - 1, d, hh - 7, mm)).toISOString();
  return iso;
}

/** Map 1 job Base → 1 record để upsert vào DB */
function mapJobToAtctRow(job: BaseJob) {
  const titleMap = parseTitleToMap(job.title);

  const position_khu        = titleMap["Vị trí (Khu)"];
  const position_tieukhu    = titleMap["Tiểu khu"];
  const position_tieukhu_no = titleMap["Vị trí(Tiểu khu)"];
  const position_row        = titleMap["Vị trí (Hàng)"];
  const position_index      = titleMap["Vị trí(Stt)"];
  const deceased_name       = titleMap["Họ tên người mất"];
  const deceased_birth_year = Number(titleMap["Năm sinh"]) || null;
  const time_of_death_raw   = titleMap["Mất lúc"];
  const lunar_date          = titleMap["Nhằm ngày"];
  const time_of_funeral_raw = titleMap["Động quan lúc"];
  const departure_place     = titleMap["Xuất phát từ"];

  const time_of_death   = parseVnDateTime(time_of_death_raw);
  const time_of_funeral = parseVnDateTime(time_of_funeral_raw);

  return {
    workflow_id: WORKFLOW_ID,
    job_id: String(job.id),
    position_khu: position_khu || null,
    position_tieukhu: position_tieukhu || null,
    position_tieukhu_no: position_tieukhu_no || null,
    position_row: position_row || null,
    position_index: position_index || null,
    deceased_name: deceased_name || null,
    deceased_birth_year,
    time_of_death,
    lunar_date: lunar_date || null,
    time_of_funeral,
    departure_place: departure_place || null,
  };
}

/** Gọi API Base – lấy 1 page job của workflow (giống Postman: form-urlencoded) */
async function fetchJobsPage(pageId: number): Promise<BaseJob[]> {
  const url = `https://workflow.${BASE_DOMAIN}/extapi/v1/workflow/jobs`;

  const form = new URLSearchParams();
  form.append("access_token", BASE_ACCESS_TOKEN);
  form.append("id", String(WORKFLOW_ID));         // workflow_id
  form.append("page_id", String(pageId));         // 0,1,2,...
  form.append("limit", String(PAGE_SIZE));        // số job / trang

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: form.toString(),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(
      `[11053-ATCT] Fetch jobs failed (page_id=${pageId}): ${res.status} ${res.statusText} – ${text}`
    );
  }

  const data = (await res.json()) as any;

  if (data.code !== 1) {
    console.warn(
      `[11053-ATCT] API returned code=${data.code}, message=${data.message}`
    );
  }

  const jobs: BaseJob[] = Array.isArray(data.jobs) ? data.jobs : [];

  if (pageId === 0) {
    console.log(
      `[11053-ATCT] Page 0: total_items=${data.total_items}, jobs_in_page=${jobs.length}`
    );
  }

  return jobs;
}

/** Upsert 1 record vào bảng workflow_job_atct_min */
async function upsertAtctRow(row: ReturnType<typeof mapJobToAtctRow>) {
  const sql = `
    INSERT INTO workflow_job_atct_min (
      workflow_id, job_id,
      position_khu, position_tieukhu, position_tieukhu_no,
      position_row, position_index,
      deceased_name, deceased_birth_year,
      time_of_death, lunar_date, time_of_funeral,
      departure_place,
      updated_at
    ) VALUES (
      $1, $2,
      $3, $4, $5,
      $6, $7,
      $8, $9,
      $10, $11, $12,
      $13,
      NOW()
    )
    ON CONFLICT (workflow_id, job_id)
    DO UPDATE SET
      position_khu        = EXCLUDED.position_khu,
      position_tieukhu    = EXCLUDED.position_tieukhu,
      position_tieukhu_no = EXCLUDED.position_tieukhu_no,
      position_row        = EXCLUDED.position_row,
      position_index      = EXCLUDED.position_index,
      deceased_name       = EXCLUDED.deceased_name,
      deceased_birth_year = EXCLUDED.deceased_birth_year,
      time_of_death       = EXCLUDED.time_of_death,
      lunar_date          = EXCLUDED.lunar_date,
      time_of_funeral     = EXCLUDED.time_of_funeral,
      departure_place     = EXCLUDED.departure_place,
      updated_at          = NOW();
  `;

  const params = [
    row.workflow_id,
    row.job_id,
    row.position_khu,
    row.position_tieukhu,
    row.position_tieukhu_no,
    row.position_row,
    row.position_index,
    row.deceased_name,
    row.deceased_birth_year,
    row.time_of_death,
    row.lunar_date,
    row.time_of_funeral,
    row.departure_place,
  ];

  await pool.query(sql, params);
}

/** Xoá job khỏi bảng (dùng cho job đã hoàn thành) */
async function deleteAtctRow(jobId: string) {
  const sql = `
    DELETE FROM workflow_job_atct_min
    WHERE workflow_id = $1 AND job_id = $2
  `;
  await pool.query(sql, [WORKFLOW_ID, jobId]);
}

/** Full sync: chỉ giữ job chưa hoàn thành */
export async function fullSync11053Atct(): Promise<number> {
  console.log(`[11053-ATCT] Start full sync workflow ${WORKFLOW_ID}`);

  let pageId = 0;   // page_id 0,1,2,...
  let total = 0;

  while (true) {
    console.log(`[11053-ATCT] Fetch page_id ${pageId}...`);
    const jobs = await fetchJobsPage(pageId);

    if (!jobs.length) {
      console.log("[11053-ATCT] No more jobs.");
      break;
    }

    for (const job of jobs) {
      const status = (job as any).status;

      if (isCompletedStatus(status)) {
        // Job đã hoàn thành: đảm bảo không còn trong bảng
        await deleteAtctRow(String(job.id));
        continue;
      }

      const row = mapJobToAtctRow(job);
      await upsertAtctRow(row);
      total++;
    }

    pageId++;
  }

  console.log(
    `[11053-ATCT] Done. Total rows upserted (active only): ${total}`
  );
  return total;
}
