import { Request, Response } from "express";
import {
  fullSync11053Atct,
  fetchJobDetailAtct,
  processJob11053Atct,
} from "./fullSync11053Atct";

/**
 * Hàm thông minh hơn để lấy Job Object từ Webhook Payload
 * Cập nhật: Bắt được trường hợp Base gửi Job object ngay tại root payload (Case 0)
 */

function extractJobFromWebhook(payload: any): any | null {
  if (!payload || typeof payload !== "object") return null;

  // Case 0: Payload CHÍNH LÀ Job (Trường hợp log của bạn: {"id":"...", "type":"workflowjobs", ...})
  // Nhận diện: có 'id' VÀ ('type'='workflowjobs' HOẶC có 'title')
  if (payload.id && (payload.type === 'workflowjobs' || payload.title || payload.name)) {
    return payload;
  }

  // Case 1: Job nằm trong payload.job (thường thấy ở action comment/update)
  if (payload.job && typeof payload.job === "object" && payload.job.id) return payload.job;

  // Case 2: Job nằm trong payload.data.job
  if (payload.data?.job && typeof payload.data.job === "object" && payload.data.job.id)
    return payload.data.job;

  // Case 3: Job nằm trong payload.data (nếu data là object chứa thông tin job)
  if (payload.data && typeof payload.data === "object" && payload.data.id) {
     return payload.data;
  }

  // Case 4: Legacy (cấu trúc cũ)
  if (payload.data?.item && typeof payload.data.item === "object")
    return payload.data.item;
  if (payload.item && typeof payload.item === "object") return payload.item;

  return null;
}

/** Lấy job_id từ payload nếu Base chỉ gửi ID */
function extractJobIdFromWebhook(payload: any): string | null {
  if (!payload || typeof payload !== "object") return null;

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
 * Webhook cho workflow 11053 (AT/CT)
 */
export async function webhook11053Atct(req: Request, res: Response) {
  try {
    const body = req.body || {};
    
    // LOG DEBUG: Xem cấu trúc payload nhận được
    console.log("---------------------------------------------------------");
    console.log(`[11053-WEBHOOK] Time: ${new Date().toISOString()}`);
    console.log(`[11053-WEBHOOK] Payload ID: ${body.id || body.data?.id || "unknown"}`);

    // 1) Cố gắng lấy Full Job từ Payload (Ưu tiên cao nhất)
    const job = extractJobFromWebhook(body);
    if (job) {
      const jobId = String(job.id || job.job_id || "unknown");
      console.log(`[11053-WEBHOOK] ✅ Found FULL JOB in payload. ID: ${jobId}`);
      
      const result = await processJob11053Atct(job);
      console.log(`[11053-WEBHOOK] Process Result:`, result);
      
      return res.status(200).json({
        ok: true,
        mode: "job-in-payload",
        job_id: jobId,
        action: result
      });
    }

    // 2) Nếu không có Full Job, lấy ID và gọi API Fetch
    const jobId = extractJobIdFromWebhook(body);
    if (jobId) {
      console.log(`[11053-WEBHOOK] ⚠️ Found ID only: ${jobId}. Fetching details...`);
      
      try {
        const jobDetail = await fetchJobDetailAtct(jobId);
        console.log(`[11053-WEBHOOK] Fetch success.`);
        
        const result = await processJob11053Atct(jobDetail);
        
        return res.status(200).json({
          ok: true,
          mode: "fetch-job",
          job_id: jobId,
          action: result
        });
      } catch (fetchErr: any) {
         console.error(`[11053-WEBHOOK] ❌ Fetch failed: ${fetchErr.message}`);
         return res.status(200).json({ ok: false, error: "Fetch detail failed", details: fetchErr.message });
      }
    }

    console.log(`[11053-WEBHOOK] ❓ No Job data found. Ignored.`);
    return res.status(200).json({ ok: true, msg: "ignored" });

  } catch (err: any) {
    console.error("[11053-WEBHOOK] CRITICAL ERROR:", err);
    return res.status(500).json({
      ok: false,
      error: err?.message || String(err),
    });
  }
}