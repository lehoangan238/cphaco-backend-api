// src/modules/workflow/webhook11053Atct.ts

import { Request, Response } from "express";
import {
  fetchJobDetailAtct,
  processJob11053Atct,
} from "./fullSync11053Atct";

/**
 * Chuẩn hóa check job payload giống webhook 11055
 */
function extractJobFromWebhook(payload: any): any | null {
  if (!payload || typeof payload !== "object") return null;

  if (payload.job && typeof payload.job === "object") return payload.job;
  if (payload.data?.job && typeof payload.data.job === "object")
    return payload.data.job;
  if (payload.data?.item && typeof payload.data.item === "object")
    return payload.data.item;
  if (payload.item && typeof payload.item === "object") return payload.item;

  return null;
}

function extractJobIdFromWebhook(payload: any): string | null {
  const cand =
    payload.job_id ??
    payload.id ??
    payload.data?.job_id ??
    payload.data?.id ??
    null;

  if (!cand) return null;
  const s = String(cand).trim();
  return s || null;
}

/**
 * Webhook xử lý realtime workflow 11053 (An táng / Cải táng)
 */
export async function webhook11053Atct(req: Request, res: Response) {
  try {
    const body = req.body || {};
    const preview = (JSON.stringify(body) || "").slice(0, 500);
    console.log("[11053-ATCT-webhook] payload:", preview);

    // 1) Nếu webhook gửi FULL JOB → xử lý trực tiếp
    const job = extractJobFromWebhook(body);
    if (job) {
      await processJob11053Atct(job);
      return res.json({
        ok: true,
        mode: "job-in-payload",
        job_id: job.id || job.job_id || null,
      });
    }

    // 2) Nếu webhook chỉ gửi job_id → tự fetch detail từ Base
    const jobId = extractJobIdFromWebhook(body);
    if (!jobId) {
      return res
        .status(400)
        .json({ ok: false, error: "No job or job_id in webhook payload" });
    }

    console.log("[11053-ATCT-webhook] fetching detail for job:", jobId);
    const jobDetail = await fetchJobDetailAtct(jobId);
    await processJob11053Atct(jobDetail);

    return res.json({
      ok: true,
      mode: "fetch-job",
      job_id: jobId,
    });
  } catch (err: any) {
    console.error("[11053-ATCT-webhook] error:", err);
    return res.status(500).json({
      ok: false,
      error: err?.message || "Unknown error",
    });
  }
}
