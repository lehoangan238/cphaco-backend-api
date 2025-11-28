/************************************************************
 * BASE Workflow 11053 → DB (workflow_job_atct_min)
 ************************************************************/

import fetch from "node-fetch";
import { pool } from "../../config/db";

const WORKFLOW_ID = 11053;
const BASE_DOMAIN = process.env.BASE_DOMAIN || "base.vn";
const BASE_ACCESS_TOKEN = (process.env.BASE_ACCESS_TOKEN || "").trim();
const PAGE_SIZE = 50;

interface BaseJob {
  id: string | number;
  title?: string;
  status?: string | number; 
  status_id?: number;
  job_status?: any;
  last_update?: string | number; // Quan trọng: lấy thời gian sửa đổi từ Base
}

/* ───────── Helpers status ───────── */

function getJobStatusRaw(job: any): any {
  if (!job) return null;
  if (job.status !== undefined && job.status !== null) return job.status;
  if (job.status_id !== undefined && job.status_id !== null) return job.status_id;
  if (job.job_status !== undefined && job.job_status !== null) return job.job_status;
  return null;
}

function isCompletedStatus(st: any): boolean {
  if (st === null || st === undefined) return false;
  const n = Number(st);
  const s = String(st).toLowerCase().trim();

  // Status ID 10 thường là hoàn thành (Done)
  if (n === 10) return true; 
  if (s === "done" || s === "completed") return true;
  return false;
}

function isFailedStatus(st: any): boolean {
  if (st === null || st === undefined) return false;
  const n = Number(st);
  const s = String(st).toLowerCase().trim();
  
  if (!Number.isNaN(n) && n < 0) return true;
  if (s.includes("thất bại") || s.includes("hủy") || s.includes("cancel") || s.includes("failed")) return true;
  return false;
}

/* ───────── Mapping Logic ───────── */

function parseTitleToMap(title: string | undefined): Record<string, string> {
  const result: Record<string, string> = {};
  if (!title) return result;

  const parts = title.split(/(?:·|&middot;|\|)/);
  for (let rawPart of parts) {
    let part = rawPart.trim();
    if (!part) continue;
    const segments = part.split(":");
    if (segments.length < 2) continue;
    const key = segments[0].trim();
    let value = segments.slice(1).join(":").trim();
    value = value.replace(/^:+\s*/, "");
    if (key) result[key] = value;
  }
  return result;
}

function normalizeCeremonyType(raw: string | undefined): string | null {
  if (!raw) return null;
  const s = raw.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").trim();
  if (s.includes("cai tang")) return "CẢI TÁNG";
  if (s.includes("an tang")) return "AN TÁNG";
  return raw.trim() || null;
}

function parseVnDateTime(input: any): string | null {
  if (!input) return null;
  const s = String(input).trim();
  if (!s) return null;

  const [datePart, timePart] = s.split(" ");
  if (!datePart) return null;

  let d, m, y;
  if (datePart.includes("/")) {
      [d, m, y] = datePart.split("/").map(Number);
  } else if (datePart.includes("-")) {
      [y, m, d] = datePart.split("-").map(Number);
  }

  if (!d || !m || !y) return null;

  let hh = 0, mm = 0;
  if (timePart) {
    const [hhStr, mmStr] = timePart.split(":");
    hh = Number(hhStr) || 0;
    mm = Number(mmStr) || 0;
  }

  const iso = new Date(Date.UTC(y, m - 1, d, hh - 7, mm)).toISOString();
  return iso;
}

// Chuyển timestamp của Base (giây) sang Date object
function parseBaseLastUpdate(lastUpdate: string | number | undefined): Date {
  if (!lastUpdate) return new Date(); 
  const ts = Number(lastUpdate);
  if (Number.isNaN(ts) || ts === 0) return new Date();
  return new Date(ts * 1000);
}

function mapJobToAtctRow(job: BaseJob) {
  const titleMap = parseTitleToMap(job.title);

  const request_raw = titleMap["ĐỀ NGHỊ"] || titleMap["Đề nghị"] || titleMap["Đề Nghị"];
  const ceremony_type = normalizeCeremonyType(request_raw);

  const age_phrase_raw = titleMap["Hưởng dương"] || titleMap["Hưởng thọ"] || titleMap["HƯỞNG THỌ"];
  const age_phrase = age_phrase_raw ? age_phrase_raw.trim() : null;

  return {
    workflow_id: WORKFLOW_ID,
    job_id: String(job.id),
    ceremony_type: ceremony_type || null,
    age_phrase: age_phrase || null,
    position_khu: titleMap["Vị trí (Khu)"] || null,
    position_tieukhu: titleMap["Tiểu khu"] || null,
    position_tieukhu_no: titleMap["Vị trí(Tiểu khu)"] || null,
    position_row: titleMap["Vị trí (Hàng)"] || null,
    position_index: titleMap["Vị trí(Stt)"] || null,
    deceased_name: titleMap["Họ tên người mất"] || null,
    deceased_birth_year: Number(titleMap["Năm sinh"]) || null,
    time_of_death: parseVnDateTime(titleMap["Mất lúc"]),
    lunar_date: titleMap["Nhằm ngày"] || null,
    time_of_funeral: parseVnDateTime(titleMap["Động quan lúc"]),
    departure_place: titleMap["Xuất phát từ"] || null,
    // QUAN TRỌNG: Lấy thời gian cập nhật từ Base
    base_updated_at: parseBaseLastUpdate(job.last_update) 
  };
}

/* ───────── API & DB Calls ───────── */

async function fetchJobsPage(pageId: number): Promise<BaseJob[]> {
  const url = `https://workflow.${BASE_DOMAIN}/extapi/v1/workflow/jobs`;
  const form = new URLSearchParams();
  form.append("access_token", BASE_ACCESS_TOKEN);
  form.append("id", String(WORKFLOW_ID));
  form.append("page_id", String(pageId));
  form.append("limit", String(PAGE_SIZE));

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: form.toString(),
  });

  if (!res.ok) throw new Error(`Fetch jobs failed: ${res.status}`);
  const data = (await res.json()) as any;
  return Array.isArray(data.jobs) ? data.jobs : [];
}

async function upsertAtctRow(row: ReturnType<typeof mapJobToAtctRow>) {
  // Logic cập nhật: updated_at sẽ lấy từ base_updated_at (thời gian thực trên Base)
  const sql = `
    INSERT INTO workflow_job_atct_min (
      workflow_id, job_id, ceremony_type, position_khu, position_tieukhu, position_tieukhu_no,
      position_row, position_index, deceased_name, deceased_birth_year, age_phrase,
      time_of_death, lunar_date, time_of_funeral, departure_place, updated_at
    ) VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
    )
    ON CONFLICT (workflow_id, job_id) DO UPDATE SET
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
      updated_at          = EXCLUDED.updated_at;
  `;

  await pool.query(sql, [
    row.workflow_id, row.job_id, row.ceremony_type, row.position_khu, row.position_tieukhu,
    row.position_tieukhu_no, row.position_row, row.position_index, row.deceased_name,
    row.deceased_birth_year, row.age_phrase, row.time_of_death, row.lunar_date,
    row.time_of_funeral, row.departure_place,
    row.base_updated_at 
  ]);
}

async function deleteAtctRow(jobId: string) {
  await pool.query(`DELETE FROM workflow_job_atct_min WHERE workflow_id = $1 AND job_id = $2`, [WORKFLOW_ID, jobId]);
}

/**
 * FETCH JOB DETAIL
 * Endpoint đúng: .../job/get (Fix lỗi HTML 404 cũ)
 */
export async function fetchJobDetailAtct(jobId: string) {
  const url = `https://workflow.${BASE_DOMAIN}/extapi/v1/job/get`;
  
  const form = new URLSearchParams();
  form.append("access_token", BASE_ACCESS_TOKEN);
  form.append("id", String(jobId)); 

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: form.toString(),
  });
  
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`HTTP Error ${res.status}: ${text.slice(0, 100)}`);
  }

  const data = await res.json();
  if (String(data.code) !== "1") {
     throw new Error(`API Error: ${data.message || JSON.stringify(data)}`);
  }
  
  const job = data.job || data.data;

  if (!job) {
     throw new Error(`No job data in response.`);
  }
  
  return job;
}

/* ───────── Main Processors ───────── */

export async function processJob11053Atct(job: any) {
  const rawStatus = getJobStatusRaw(job);
  const jobId = String(job.id);
  
  console.log(`[11053-PROCESS] Job ${jobId} | Status: ${rawStatus}`);

  if (isCompletedStatus(rawStatus)) {
    console.log(`[11053-PROCESS] Job ${jobId} is COMPLETED -> Deleting.`);
    await deleteAtctRow(jobId);
    return { action: "deleted", reason: "completed" };
  }

  if (isFailedStatus(rawStatus)) {
    console.log(`[11053-PROCESS] Job ${jobId} is FAILED -> Deleting.`);
    await deleteAtctRow(jobId);
    return { action: "deleted", reason: "failed" };
  }

  const row = mapJobToAtctRow(job);
  await upsertAtctRow(row);
  return { action: "upserted", data: row };
}

export async function fullSync11053Atct(): Promise<number> {
  console.log(`[11053-FULLSYNC] Starting...`);
  let pageId = 0;
  let total = 0;

  while (true) {
    const jobs = await fetchJobsPage(pageId);
    if (!jobs.length) break;

    console.log(`[11053-FULLSYNC] Page ${pageId}: found ${jobs.length} jobs.`);

    for (const job of jobs) {
      try {
        await processJob11053Atct(job);
        total++;
      } catch (err) {
        console.error(`[11053-FULLSYNC] Error processing job ${job.id}:`, err);
      }
    }
    pageId++;
  }
  console.log(`[11053-FULLSYNC] Done. Total processed: ${total}`);
  return total;
}