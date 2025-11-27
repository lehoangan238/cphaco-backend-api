"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.fullSync11055 = fullSync11055;
const node_fetch_1 = __importDefault(require("node-fetch"));
const db_1 = require("../../config/db");
const WORKFLOW_ID = Number(process.env.WORKFLOW_ID);
const BASE_DOMAIN = process.env.BASE_DOMAIN || "base.vn";
const BASE_API_KEY = process.env.BASE_API_KEY || "";
const PAGE_SIZE = 200;
// ── Helpers status giống GAS ──────────────────────────────
function toNumOrNull(x) {
    const n = Number(x);
    return Number.isNaN(n) ? null : n;
}
function isCompletedStatus(st) {
    const n = toNumOrNull(st);
    const s = String(st).toLowerCase();
    return st === 10 || st === "10" || n === 10 || s === "completed";
}
function isFailedStatus(st) {
    const n = toNumOrNull(st);
    const s = String(st).toLowerCase();
    return st === -10 || st === "-10" || n === -10 || s.includes("fail");
}
// ── Lấy danh sách jobs theo trang ─────────────────────────
async function fetchJobsPage(page) {
    const url = `https://workflow.${BASE_DOMAIN}/extapi/v1/workflow/jobs`;
    const body = {
        workflow_id: WORKFLOW_ID,
        page,
        size: PAGE_SIZE,
    };
    const res = await (0, node_fetch_1.default)(url, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            "X-Api-Key": BASE_API_KEY,
        },
        body: JSON.stringify(body),
    });
    const text = await res.text();
    if (!res.ok) {
        throw new Error(`workflow/jobs HTTP ${res.status}: ${text.slice(0, 300)}`);
    }
    let json;
    try {
        json = JSON.parse(text);
    }
    catch {
        throw new Error(`Cannot parse workflow/jobs JSON: ${text.slice(0, 200)}`);
    }
    const jobs = json.data || json.jobs || [];
    return jobs;
}
// ── Lấy chi tiết 1 job (để có full JSON như GAS dùng) ─────
async function fetchJobDetail(jobId) {
    const url = `https://workflow.${BASE_DOMAIN}/extapi/v1/job/get`;
    const body = new URLSearchParams({
        access_token: BASE_API_KEY,
        job_id: jobId,
    });
    const res = await (0, node_fetch_1.default)(url, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body,
    });
    const text = await res.text();
    if (!res.ok) {
        throw new Error(`job/get HTTP ${res.status}: ${text.slice(0, 300)}`);
    }
    let parsed;
    try {
        parsed = JSON.parse(text);
    }
    catch {
        throw new Error(`Cannot parse job/get JSON: ${text.slice(0, 200)}`);
    }
    const job = parsed?.job ||
        parsed?.data?.job ||
        (Array.isArray(parsed?.jobs) && parsed.jobs[0]) ||
        parsed?.item ||
        parsed?.data;
    if (!job) {
        throw new Error("No job object in job/get response");
    }
    return job;
}
// ── Extract vài trường hệ thống chính ─────────────────────
function extractSystem(job) {
    const name = job.name ?? "";
    const status = job.status ?? job.state ?? "";
    const stage = job.stage ??
        job.phase ??
        job.milestone ??
        job.stage_name ??
        "";
    const assignee = job.username ??
        job.assignee?.name ??
        job.assignee?.email ??
        (Array.isArray(job.assignees) &&
            (job.assignees[0]?.name || job.assignees[0]?.email)) ??
        "";
    const creator = job.created_by?.name ??
        job.created_by?.email ??
        job.creator_name ??
        job.creator ??
        "";
    let followers = "";
    if (Array.isArray(job.followers)) {
        followers = job.followers
            .map((x) => x.name || x.email || x)
            .join(", ");
    }
    else if (Array.isArray(job.watchers)) {
        followers = job.watchers
            .map((x) => x.name || x.email || x)
            .join(", ");
    }
    const labels = Array.isArray(job.labels)
        ? job.labels.join(", ")
        : Array.isArray(job.tags)
            ? job.tags.join(", ")
            : job.label ?? "";
    const description = job.description ?? job.content ?? "";
    const createdAt = job.since != null
        ? new Date(Number(job.since) * 1000)
        : job.created_at
            ? new Date(job.created_at)
            : null;
    const updatedAt = job.updated_at ? new Date(job.updated_at) : null;
    const doneAt = job.done_at || job.completed_at
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
// ── Upsert job vào Postgres ───────────────────────────────
async function upsertJobToDb(job) {
    if (!job || String(job.workflow_id) !== String(WORKFLOW_ID))
        return;
    const sys = extractSystem(job);
    const client = await db_1.pool.connect();
    try {
        await client.query(`
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
    `, [
            String(job.id),
            job.workflow_id ?? null,
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
            job,
        ]);
    }
    finally {
        client.release();
    }
}
// ── Hàm public: full sync lần đầu ─────────────────────────
async function fullSync11055() {
    let page = 1;
    let total = 0;
    while (true) {
        const jobs = await fetchJobsPage(page);
        if (!jobs.length)
            break;
        for (const j of jobs) {
            // lấy chi tiết sâu như GAS – để có custom fields, tables...
            const jobDetail = await fetchJobDetail(String(j.id));
            const st = jobDetail.status ?? jobDetail.state ?? "";
            if (isFailedStatus(st)) {
                // trạng thái -10: đánh dấu deleted
                const client = await db_1.pool.connect();
                try {
                    await client.query(`UPDATE workflow_jobs_huyetmo
             SET deleted_at = NOW(), is_frozen = FALSE
             WHERE job_id = $1`, [String(jobDetail.id)]);
                }
                finally {
                    client.release();
                }
            }
            else {
                await upsertJobToDb(jobDetail);
                if (isCompletedStatus(st)) {
                    // freeze = set is_frozen = TRUE
                    const client = await db_1.pool.connect();
                    try {
                        await client.query(`UPDATE workflow_jobs_huyetmo
               SET is_frozen = TRUE
               WHERE job_id = $1`, [String(jobDetail.id)]);
                    }
                    finally {
                        client.release();
                    }
                }
            }
            total++;
        }
        if (jobs.length < PAGE_SIZE)
            break;
        page++;
    }
    return total;
}
//# sourceMappingURL=fullSync11055.js.map