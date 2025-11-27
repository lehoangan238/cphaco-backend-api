// src/modules/workflow/webhook11053Atct.ts
import { Request, Response } from "express";
import { fullSync11053Atct } from "./fullSync11053Atct";

/**
 * Webhook cho workflow 11053 (AT/CT)
 * - Mỗi lần Base gọi vào đây, ta chạy lại fullSync11053Atct
 * - DB sẽ:
 *    + Upsert các job đang chạy
 *    + Xoá các job đã hoàn thành (status = 10)
 */
export async function webhook11053Atct(req: Request, res: Response) {
  try {
    console.log(
      "[11053-ATCT] Webhook received:",
      JSON.stringify(req.body).slice(0, 500)
    );

    const total = await fullSync11053Atct();

    res.json({
      ok: true,
      workflow_id: 11053,
      mode: "full-sync-on-webhook",
      total,
    });
  } catch (e: any) {
    console.error("[11053-ATCT] Webhook error:", e);
    res.status(500).json({
      ok: false,
      error: e?.message || "Unknown error",
    });
  }
}
