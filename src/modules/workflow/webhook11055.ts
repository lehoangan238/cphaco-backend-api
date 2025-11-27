// src/modules/workflow/webhook11055.ts

import { Request, Response } from "express";
import { fetchJobDetail, processJob11055 } from "./fullSync11055";

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

export async function webhook11055(req: Request, res: Response) {
  try {
    const body = req.body || {};
    console.log("[11055-webhook] payload:", JSON.stringify(body).slice(0, 500));

    // 1) Nếu webhook gửi full job → xử lý luôn
    const job = extractJobFromWebhook(body);
    if (job) {
      await processJob11055(job);
      return res.status(200).json({ ok: true, mode: "job-in-payload" });
    }

    // 2) Nếu chỉ gửi job_id/id → gọi API để lấy detail
    const jobId = extractJobIdFromWebhook(body);
    if (!jobId) {
      return res
        .status(400)
        .json({ ok: false, error: "No job or job_id in payload" });
    }

    const jobDetail = await fetchJobDetail(jobId);
    await processJob11055(jobDetail);

    return res.status(200).json({ ok: true, mode: "fetch-job", job_id: jobId });
  } catch (err: any) {
    console.error("[11055-webhook] error:", err);
    return res
      .status(500)
      .json({ ok: false, error: err?.message || String(err) });
  }
}
