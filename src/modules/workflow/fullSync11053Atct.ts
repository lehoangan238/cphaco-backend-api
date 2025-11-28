/************************************************************
 * BASE Workflow 11053 → Neon (workflow_job_atct_min)
 * - Chỉ sync các job CHƯA HOÀN THÀNH, CHƯA THẤT BẠI
 *   (bỏ qua: Hoàn thành, Thất bại/Hủy)
 * - Lấy:
 *   + ĐỀ NGHỊ → ceremony_type ("AN TÁNG", "CẢI TÁNG", ...)
 *   + Vị trí / Họ tên / Năm sinh / Mất lúc / Nhằm ngày / Động quan lúc
 *   + age_phrase: lấy thẳng từ field "Hưởng dương" trong title
 ************************************************************/

import fetch from "node-fetch";
import { pool } from "../../config/db";

const WORKFLOW_ID = 11053;
const BASE_DOMAIN = process.env.BASE_DOMAIN || "base.vn";

// Dùng CHUNG BASE_ACCESS_TOKEN từ .env
const BASE_ACCESS_TOKEN = (process.env.BASE_ACCESS_TOKEN || "").trim();
const PAGE_SIZE = 50;



interface BaseJob {
  id: string | number;
  title?: string;
  status?: string | number; // hoặc status_id / job_status tuỳ API
}

/* ───────── Helpers status ───────── */

function toNumOrNull(x: any): number | null {
  const n = Number(x);
  return Number.isNaN(n) ? null : n;
}

/**
 * Lấy raw status từ job
 * Một số API của Base trả:
 *  - job.status
 *  - job.status_id
 *  - job.job_status
 */
function getJobStatusRaw(job: any): any {
  if (!job) return null;

  if (job.status !== undefined && job.status !== null) return job.status;
  if (job.status_id !== undefined && job.status_id !== null)
    return job.status_id;
  if (job.job_status !== undefined && job.job_status !== null)
    return job.job_status;

  return null;
}

// status = 10 coi như "Hoàn thành"
function isCompletedStatus(st: any): boolean {
  if (st === null || st === undefined) return false;

  const n = toNumOrNull(st);
  const s = String(st).toLowerCase().trim();

  // Các trường hợp numeric
  if (n === 10) return true; // trạng thái 10 = Hoàn thành
  // Nếu hệ thống bạn dùng 100 là hoàn thành thì mở dòng dưới:
  // if (n === 100) return true;

  // Các trường hợp string
  if (s === "10") return true;
  if (s === "done" || s === "completed") return true;
  if (s.includes("hoàn thành")) return true; // "Hoàn thành", "Đã hoàn thành",...

  return false;
}

// status thất bại / huỷ → không giữ trong bảng
function isFailedStatus(st: any): boolean {
  if (st === null || st === undefined) return false;

  const n = toNumOrNull(st);
  const s = String(st).toLowerCase().trim();

  // Numeric: thường trạng thái âm là failed / cancel
  if (n !== null && n < 0) return true; // -10, -1, ...

  // String: tuỳ cách đặt trong Base
  if (s.includes("thất bại")) return true;
  if (s.includes("hủy") || s.includes("huỷ")) return true;
  if (s.includes("cancel")) return true;
  if (s.includes("failed") || s.includes("failure")) return true;

  return false;
}

/* ───────── Parse title ───────── */

/**
 * Parse title dạng "Label: Value · Label2: Value2 ..."
 * Lưu ý: Base thường trả kí tự "·" (U+00B7), không phải chuỗi "&middot;"
 */
function parseTitleToMap(title: string | undefined): Record<string, string> {
  const result: Record<string, string> = {};
  if (!title) return result;

  // Tách theo cả "·" thật lẫn chuỗi "&middot;" cho chắc
  const parts = title.split(/(?:·|&middot;)/);

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

/* ───────── Helpers cho loại lễ (AN TÁNG / CẢI TÁNG) ───────── */

function normalizeCeremonyType(raw: string | undefined): string | null {
  if (!raw) return null;
  const s = raw
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "") // bỏ dấu
    .trim();

  if (s.includes("cai tang")) return "CẢI TÁNG";
  if (s.includes("an tang")) return "AN TÁNG";

  // nếu không bắt được thì trả lại text gốc (giữ nguyên để hiển thị)
  return raw.trim() || null;
}

/** Map 1 job Base → 1 record để upsert vào DB */
function mapJobToAtctRow(job: BaseJob) {
  const titleMap = parseTitleToMap(job.title);

  // ĐỀ NGHỊ: AN TÁNG / CẢI TÁNG
  const request_raw =
    titleMap["ĐỀ NGHỊ"] || titleMap["Đề nghị"] || titleMap["Đề Nghị"];
  const ceremony_type = normalizeCeremonyType(request_raw);

  // Hưởng dương / Hưởng thọ: lấy thẳng từ field "Hưởng dương"
  const age_phrase_raw =
    titleMap["Hưởng dương"] ||
    titleMap["HƯỞNG DƯƠNG"] ||
    titleMap["Hưởng Dương"] ||
    titleMap["Hưởng thọ"] ||
    titleMap["HƯỞNG THỌ"] ||
    titleMap["Hưởng Thọ"];

  const age_phrase = age_phrase_raw ? age_phrase_raw.trim() : null;

  const position_khu = titleMap["Vị trí (Khu)"];
  const position_tieukhu = titleMap["Tiểu khu"];
  const position_tieukhu_no = titleMap["Vị trí(Tiểu khu)"];
  const position_row = titleMap["Vị trí (Hàng)"];
  const position_index = titleMap["Vị trí(Stt)"];
  const deceased_name = titleMap["Họ tên người mất"];
  const deceased_birth_year = Number(titleMap["Năm sinh"]) || null;
  const time_of_death_raw = titleMap["Mất lúc"];
  const lunar_date = titleMap["Nhằm ngày"];
  const time_of_funeral_raw = titleMap["Động quan lúc"];
  const departure_place = titleMap["Xuất phát từ"];

  const time_of_death = parseVnDateTime(time_of_death_raw);
  const time_of_funeral = parseVnDateTime(time_of_funeral_raw);

  return {
    workflow_id: WORKFLOW_ID,
    job_id: String(job.id),

    ceremony_type: ceremony_type || null, // AN TÁNG / CẢI TÁNG
    age_phrase: age_phrase || null,       // lấy raw từ "Hưởng dương"

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

/** Gọi API Base – lấy 1 page job của workflow (giống Postman: form-urlencoded) */
async function fetchJobsPage(pageId: number): Promise<BaseJob[]> {
  const url = `https://workflow.${BASE_DOMAIN}/extapi/v1/workflow/jobs`;

  const form = new URLSearchParams();
  form.append("access_token", BASE_ACCESS_TOKEN);
  form.append("id", String(WORKFLOW_ID));   // workflow_id
  form.append("page_id", String(pageId));   // 0,1,2,...
  form.append("limit", String(PAGE_SIZE));  // số job / trang

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
      ceremony_type,
      position_khu, position_tieukhu, position_tieukhu_no,
      position_row, position_index,
      deceased_name, deceased_birth_year,
      age_phrase,
      time_of_death, lunar_date, time_of_funeral,
      departure_place,
      updated_at
    ) VALUES (
      $1, $2,
      $3,
      $4, $5, $6,
      $7, $8,
      $9, $10,
      $11,
      $12, $13, $14,
      $15,
      NOW()
    )
    ON CONFLICT (workflow_id, job_id)
    DO UPDATE SET
      ceremony_type       = EXCLUDED.ceremony_type,
      position_khu        = EXCLUDED.position_khu,
      position_tieukhu    = EXCLUDED.position_tieukhu,
      position_tieukhu_no = EXCLUDED.position_tieukhu_no,
      position_row        = EXCLUDED.position_row,
      position_index      = EXCLUDED.position_index,
      deceased_name       = EXCLUDED.deceased_name,
      deceased_birth_year = EXCLUDED.deceased_birth_year,
      age_phrase          = EXCLUDED.age_phrase,
      time_of_death       = EXCLUDED.time_of_death,
      lunar_date          = EXCLUDED.lunar_date,
      time_of_funeral     = EXCLUDED.time_of_funeral,
      departure_place     = EXCLUDED.departure_place,
      updated_at          = NOW();
  `;

  const params = [
    row.workflow_id,
    row.job_id,
    row.ceremony_type,
    row.position_khu,
    row.position_tieukhu,
    row.position_tieukhu_no,
    row.position_row,
    row.position_index,
    row.deceased_name,
    row.deceased_birth_year,
    row.age_phrase,
    row.time_of_death,
    row.lunar_date,
    row.time_of_funeral,
    row.departure_place,
  ];

  await pool.query(sql, params);
}

/** Xoá job khỏi bảng (dùng cho job đã hoàn thành / thất bại / huỷ) */
async function deleteAtctRow(jobId: string) {
  const sql = `
    DELETE FROM workflow_job_atct_min
    WHERE workflow_id = $1 AND job_id = $2
  `;
  await pool.query(sql, [WORKFLOW_ID, jobId]);
}

/** Full sync: chỉ giữ job active (không hoàn thành, không thất bại) */
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
      const rawStatus = getJobStatusRaw(job);

      // 1) Đã hoàn thành → xoá khỏi bảng
      if (isCompletedStatus(rawStatus)) {
        await deleteAtctRow(String(job.id));
        continue;
      }

      // 2) Thất bại / bị huỷ → cũng xoá luôn
      if (isFailedStatus(rawStatus)) {
        await deleteAtctRow(String(job.id));
        continue;
      }

      // 3) Còn lại (đang chạy, pending, ...) → giữ trong bảng
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
