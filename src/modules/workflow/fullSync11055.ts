// src/modules/workflow/fullSync11055.ts

import fetch from "node-fetch";
import { pool } from "../../config/db";

const WORKFLOW_ID = Number(process.env.WORKFLOW_ID);
const BASE_DOMAIN = process.env.BASE_DOMAIN || "base.vn";
const BASE_ACCESS_TOKEN = process.env.BASE_ACCESS_TOKEN || "";
const PAGE_SIZE = 20;

if (!WORKFLOW_ID) {
  console.warn("[11055] ⚠ WORKFLOW_ID is not set or invalid");
}
if (!BASE_ACCESS_TOKEN) {
  console.warn("[11055] ⚠ BASE_ACCESS_TOKEN is empty");
}

// ───────── Helpers status ─────────
function toNumOrNull(x: any): number | null {
  const n = Number(x);
  return Number.isNaN(n) ? null : n;
}

function isCompletedStatus(st: any): boolean {
  const n = toNumOrNull(st);
  const s = String(st).toLowerCase();
  return st === 10 || st === "10" || n === 10 || s === "completed";
}

function isFailedStatus(st: any): boolean {
  const n = toNumOrNull(st);
  const s = String(st).toLowerCase();
  return st === -10 || st === "-10" || n === -10 || s.includes("fail");
}

// ───────── Parse composite field (plot + tên + phone) ─────────

interface ParsedCompositeField {
  plotCodes: string[];
  customerName: string | null;
  customerPhone: string | null;
}

// chỉ parse mã huyệt, bỏ hết phần tên/điện thoại, tránh sinh ra NG-0979...
function parsePlotCodes(segmentRaw: string | null | undefined): string[] {
  if (!segmentRaw) return [];

  // bỏ khoảng trắng
  let clean = segmentRaw.replace(/\s+/g, "");
  const result = new Set<string>();

  // nhiều cụm ngăn bởi "+"
  for (const seg of clean.split("+")) {
    const part = seg.trim();
    if (!part) continue;

    // Dạng mở rộng: F6.3-09-16;17;18
    let m = part.match(/^([A-Za-z]\d(?:\.\d+)?)-(\d{2})-([\d;]+)$/);
    if (m) {
      const block = m[1];       // F6.3
      const row = m[2];         // 09
      const rest = m[3];        // 16;17;18

      for (const g of rest.split(";")) {
        if (!g) continue;
        const num = g.padStart(2, "0");
        result.add(`${block}-${row}-${num}`);
      }
      continue;
    }

    // Dạng đơn: F6.3-09-16
    m = part.match(/^([A-Za-z]\d(?:\.\d+)?-\d{2}-\d{2})$/);
    if (m) {
      result.add(m[1]);
      continue;
    }

    // các dạng khác (vd NG-0979944914) => bỏ qua
  }

  return Array.from(result);
}

/**
 * Nhận chuỗi dạng:
 *   "F6.3-09-16;17-TỐNG QUỐC TRỌNG-0979944914"
 *   "H1.9-07-16;17;18+H1.9-08-15-TRẦN VĂN A-0909 123 456"
 * Trả về:
 *   {
 *     plotCodes: ["F6.3-09-16","F6.3-09-17"],
 *     customerName: "TỐNG QUỐC TRỌNG",
 *     customerPhone: "0979944914"
 *   }
 */
function parseCompositeField(raw: string | null | undefined): ParsedCompositeField {
  const empty: ParsedCompositeField = {
    plotCodes: [],
    customerName: null,
    customerPhone: null,
  };
  if (!raw) return empty;

  let work = raw.trim();
  if (!work) return empty;

  let customerPhone: string | null = null;
  let customerName: string | null = null;

  // 1. tách phone ở cuối: "-<nhiều số>" (cho phép có space giữa các số)
  const phoneRegex = /-(\d[\d\s]{6,})\s*$/;
  const phoneMatch = work.match(phoneRegex);
  if (phoneMatch && phoneMatch.index !== undefined) {
    customerPhone = phoneMatch[1].replace(/\D+/g, ""); // chỉ giữ số
    work = work.slice(0, phoneMatch.index).trim();     // cắt phần "-phone"
  }

  // 2. tách NAME nếu còn: hyphen cuối mà bên phải có chữ cái
  const lastHyphen = work.lastIndexOf("-");
  if (lastHyphen !== -1) {
    const right = work.slice(lastHyphen + 1).trim();
    if (/[A-Za-zÀ-ỹ]/u.test(right)) {
      customerName = right;
      work = work.slice(0, lastHyphen).trim();
    }
  }

  // 3. phần còn lại chỉ còn mã huyệt: F6.3-09-16;17+...
  const plotCodes = parsePlotCodes(work);

  return {
    plotCodes,
    customerName,
    customerPhone,
  };
}

// ───────── Lấy list jobs theo page ─────────
async function fetchJobsPage(page: number): Promise<any[]> {
  const url = `https://workflow.${BASE_DOMAIN}/extapi/v1/workflow/jobs`;

  const body = new URLSearchParams({
    access_token: BASE_ACCESS_TOKEN!,
    id: String(WORKFLOW_ID), // workflow id
    page_id: String(page),
    items_per_page: String(PAGE_SIZE),
  });

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });

  const text = await res.text();
  console.log(`[11055] workflow/jobs page ${page} raw:`, text.slice(0, 200));

  let json: any;
  try {
    json = JSON.parse(text);
  } catch (e) {
    throw new Error(
      `workflow/jobs JSON parse error: ${String(e)} - body=${text.slice(0, 200)}`
    );
  }

  if (json.code !== 1) {
    throw new Error(`workflow/jobs error: ${json.message || "unknown"}`);
  }

  return json.jobs || [];
}

// ───────── Job detail – đa endpoint ─────────
const JOB_DETAIL_ENDPOINTS_11055 = [
  {
    url: (d: string) => `https://workflow.${d}/extapi/v1/workflow/jobs/get`,
    idParam: "job_id",
  },
  {
    url: (d: string) => `https://workflow.${d}/extapi/v1/workflow/jobs/get`,
    idParam: "id",
  },
  {
    url: (d: string) => `https://workflow.${d}/extapi/v1/job/get`,
    idParam: "job_id",
  },
  {
    url: (d: string) => `https://workflow.${d}/extapi/v1/job/get`,
    idParam: "id",
  },
];

function extractJobFromPayload(parsed: any): any | null {
  if (!parsed || typeof parsed !== "object") return null;

  if (parsed.job && typeof parsed.job === "object") return parsed.job;

  if (parsed.data) {
    if (parsed.data.job && typeof parsed.data.job === "object")
      return parsed.data.job;
    if (parsed.data.item && typeof parsed.data.item === "object")
      return parsed.data.item;
    if (Array.isArray(parsed.data.jobs) && parsed.data.jobs.length)
      return parsed.data.jobs[0];
    if (typeof parsed.data === "object" && (parsed.data as any).id)
      return parsed.data;
  }

  if (Array.isArray(parsed.jobs) && parsed.jobs.length) return parsed.jobs[0];
  if (parsed.item && typeof parsed.item === "object") return parsed.item;

  return null;
}

export async function fetchJobDetail(jobId: string): Promise<any> {
  let lastErr = "";

  for (const ep of JOB_DETAIL_ENDPOINTS_11055) {
    try {
      const body = new URLSearchParams({
        access_token: BASE_ACCESS_TOKEN!,
      });
      body.append(ep.idParam, jobId);

      const url = ep.url(BASE_DOMAIN);

      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body,
      });

      const text = await res.text();
      console.log(
        "[11055] job detail",
        url,
        ep.idParam,
        "jobId=",
        jobId,
        "HTTP",
        res.status,
        "-",
        text.slice(0, 160)
      );

      if (!res.ok) {
        lastErr = `HTTP ${res.status} ${text.slice(0, 160)}`;
        continue;
      }

      let parsed: any;
      try {
        parsed = JSON.parse(text);
      } catch {
        lastErr = `JSON parse error: ${text.slice(0, 160)}`;
        continue;
      }

      if (parsed.code !== undefined && String(parsed.code) !== "1") {
        lastErr = `API code=${parsed.code}, message=${parsed.message ?? ""}`;
        continue;
      }

      const job = extractJobFromPayload(parsed);
      if (job) return job;

      lastErr = "Không tìm thấy field 'job' trong response";
    } catch (e: any) {
      lastErr = `Exception: ${e?.message ?? String(e)}`;
    }
  }

  throw new Error(lastErr || "No job in response");
}

// ───────── Extract system fields ─────────
function extractSystem(job: any) {
  const name = job.name ?? "";
  const status = job.status ?? job.state ?? "";
  const stage =
    job.stage ??
    job.phase ??
    job.milestone ??
    job.stage_name ??
    "";

  const assignee =
    job.username ??
    job.assignee?.name ??
    job.assignee?.email ??
    (Array.isArray(job.assignees) &&
      (job.assignees[0]?.name || job.assignees[0]?.email)) ??
    "";

  const creator =
    job.created_by?.name ??
    job.created_by?.email ??
    job.creator_name ??
    job.creator ??
    "";

  let followers = "";
  if (Array.isArray(job.followers)) {
    followers = job.followers
      .map((x: any) => x.name || x.email || x)
      .join(", ");
  } else if (Array.isArray(job.watchers)) {
    followers = job.watchers
      .map((x: any) => x.name || x.email || x)
      .join(", ");
  }

  const labels = Array.isArray(job.labels)
    ? job.labels.join(", ")
    : Array.isArray(job.tags)
    ? job.tags.join(", ")
    : job.label ?? "";

  const description = job.description ?? job.content ?? "";

  const createdAt =
    job.since != null
      ? new Date(Number(job.since) * 1000)
      : job.created_at
      ? new Date(job.created_at)
      : null;

  const updatedAt = job.updated_at ? new Date(job.updated_at) : null;

  const doneAt =
    job.done_at || job.completed_at
      ? new Date(job.done_at || job.completed_at)
      : null;

  const failReason = job.failure_reason ?? job.fail_reason ?? "";

  return {
    name,
    status,
    stage,
    assignee,
    creator,
    followers,
    labels,
    description,
    created_at: createdAt,
    updated_at: updatedAt,
    done_at: doneAt,
    fail_reason: failReason,
  };
}

// ───────── Upsert job vào Postgres ─────────
async function upsertJobToDb(job: any) {
  const sys = extractSystem(job);

  const client = await pool.connect();
  try {
    await client.query(
      `
      INSERT INTO workflow_jobs_huyetmo (
        job_id, workflow_id,
        name, status, stage, assignee, creator, followers, labels,
        description, created_at, updated_at, done_at, fail_reason,
        raw_json, is_frozen, deleted_at, synced_at
      )
      VALUES (
        $1, $2,
        $3, $4, $5, $6, $7, $8, $9,
        $10, $11, $12, $13, $14,
        $15, FALSE, NULL, NOW()
      )
      ON CONFLICT (job_id) DO UPDATE SET
        workflow_id = EXCLUDED.workflow_id,
        name        = EXCLUDED.name,
        status      = EXCLUDED.status,
        stage       = EXCLUDED.stage,
        assignee    = EXCLUDED.assignee,
        creator     = EXCLUDED.creator,
        followers   = EXCLUDED.followers,
        labels      = EXCLUDED.labels,
        description = EXCLUDED.description,
        created_at  = EXCLUDED.created_at,
        updated_at  = EXCLUDED.updated_at,
        done_at     = EXCLUDED.done_at,
        fail_reason = EXCLUDED.fail_reason,
        raw_json    = EXCLUDED.raw_json,
        deleted_at  = NULL,
        synced_at   = NOW();
    `,
      [
        String(job.id),
        job.workflow_id ?? WORKFLOW_ID,
        sys.name,
        sys.status,
        sys.stage,
        sys.assignee,
        sys.creator,
        sys.followers,
        sys.labels,
        sys.description,
        sys.created_at,
        sys.updated_at,
        sys.done_at,
        sys.fail_reason,
        JSON.stringify(job),
      ]
    );
  } finally {
    client.release();
  }
}

// ───────── Xử lý 1 job (dùng cho full sync + webhook) ─────────
export async function processJob11055(jobDetail: any) {
  const jobId = String(jobDetail.id);
  const st = jobDetail.status ?? jobDetail.state ?? "";
  console.log("[11055] processJob11055", jobId, "status:", st);

  // 1) failed (-10) → đánh dấu deleted
  if (isFailedStatus(st)) {
    await pool.query(
      `UPDATE workflow_jobs_huyetmo
       SET deleted_at = NOW(), is_frozen = FALSE
       WHERE job_id = $1`,
      [jobId]
    );
    await pool.query(
      `DELETE FROM workflow_job_plots WHERE job_id = $1`,
      [jobId]
    );
    return;
  }

  // 2) completed (10) → không giữ trong DB
  if (isCompletedStatus(st)) {
    console.log("[11055] ⏭ Skip completed job", jobId);
    await pool.query(
      `DELETE FROM workflow_job_plots WHERE job_id = $1`,
      [jobId]
    );
    await pool.query(
      `DELETE FROM workflow_jobs_huyetmo WHERE job_id = $1`,
      [jobId]
    );
    return;
  }

  // 3) job đang chạy → upsert + plot
  await upsertJobToDb(jobDetail);

  const parsed = parseCompositeField(
    jobDetail.name || jobDetail.title || ""
  );
  if (!parsed) return;

  // update customer info
  if (parsed.customerName || parsed.customerPhone) {
    await pool.query(
      `UPDATE workflow_jobs_huyetmo
       SET customer_name  = COALESCE($2, customer_name),
           customer_phone = COALESCE($3, customer_phone)
       WHERE job_id = $1`,
      [jobId, parsed.customerName || null, parsed.customerPhone || null]
    );
  }

  // refresh plots of that job
  await pool.query(`DELETE FROM workflow_job_plots WHERE job_id = $1`, [
    jobId,
  ]);

  for (const code of parsed.plotCodes) {
    await pool.query(
      `INSERT INTO workflow_job_plots (job_id, plot_code)
       VALUES ($1, $2)
       ON CONFLICT (job_id, plot_code) DO NOTHING`,
      [jobId, code]
    );
  }

  // OPTIONAL: cập nhật chủ sử dụng trong ocm_plots nếu có
  if (parsed.customerName || parsed.customerPhone) {
    for (const code of parsed.plotCodes) {
      await pool.query(
        `UPDATE ocm_plots
         SET owner_name  = COALESCE($2, owner_name),
             owner_phone = COALESCE($3, owner_phone),
             updated_at  = NOW()
         WHERE plot_code = $1`,
        [code, parsed.customerName || null, parsed.customerPhone || null]
      );
    }
  }
}

// ───────── Full sync lần đầu / định kỳ ─────────
export async function fullSync11055(): Promise<number> {
  let page = 1;
  let total = 0;

  while (true) {
    const jobs = await fetchJobsPage(page);
    if (!jobs.length) {
      console.log("[11055] No more jobs, stop at page", page);
      break;
    }

    console.log(`[11055] Page ${page} - jobs: ${jobs.length}`);

    for (const j of jobs) {
      let jobDetail;
      try {
        jobDetail = await fetchJobDetail(String(j.id));
      } catch (e: any) {
        console.log(
          "[11055] ❌ fetchJobDetail failed for",
          j.id,
          "-",
          e?.message || e
        );
        continue;
      }

      await processJob11055(jobDetail);
      total++;
    }

    if (jobs.length < PAGE_SIZE) {
      console.log("[11055] Last page reached:", page);
      break;
    }
    page++;
  }

  console.log("[11055] Full sync done, total processed jobs:", total);
  return total;
}
