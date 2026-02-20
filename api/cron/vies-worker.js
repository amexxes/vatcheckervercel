// /api/cron/vies-worker.js
import { kv } from "@vercel/kv";
import { runWorkerSlice } from "../fr-job/[id].js";

export default async function handler(req, res) {
  // Optional protection (recommended)
  const secret = process.env.CRON_SECRET;
  if (secret) {
    const auth = req.headers?.authorization || req.headers?.Authorization || "";
    if (auth !== `Bearer ${secret}`) return res.status(401).json({ error: "Unauthorized" });
  }

  const maxTasks = Number(process.env.CRON_MAX_TASKS || "150");
  const maxMs = Number(process.env.CRON_MAX_MS || "8000");
  const wantDebug = String(req.query?.debug || "") === "1";

  const started = Date.now();
  const slice = await runWorkerSlice({ maxTasks, maxMs, debugEnabled: wantDebug, source: "cron" });

  // (handig voor zichtbaarheid)
  let queue_vies_len = null;
  let queue_pending_len = null;
  try {
    queue_vies_len = await kv.llen("queue:vies");
  } catch (e) {
    queue_vies_len = `ERR:${String(e?.message || e)}`;
  }
  try {
    queue_pending_len = await kv.hlen("queue:pending");
  } catch (e) {
    queue_pending_len = `ERR:${String(e?.message || e)}`;
  }

  return res.status(200).json({
    ok: true,
    ms: Date.now() - started,
    ...slice,
    queue_vies_len,
    queue_pending_len,
  });
}
