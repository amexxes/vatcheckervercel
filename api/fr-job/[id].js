// api/fr-job/[id].js
import { kv } from "@vercel/kv";

const VIES_BASE = "https://ec.europa.eu/taxation_customs/vies/rest-api";
const VIES_TIMEOUT_MS = Number(process.env.VIES_TIMEOUT_MS || 20000);
const JOB_TTL_SEC = 6 * 60 * 60;

// Retries
const MAX_RETRIES = Number(process.env.MAX_RETRIES || 25); // max pogingen per VAT
const FIXED_RETRY_DELAY_MS = Number(process.env.RETRY_DELAY_MS || 0); // 0 = exponential backoff
const WORKER_LOCK_SEC = Number(process.env.WORKER_LOCK_SEC || 8); // voorkomt dubbele workers tegelijk

const RETRYABLE_CODES = new Set([
  "SERVICE_UNAVAILABLE",
  "MS_UNAVAILABLE",
  "TIMEOUT",
  "GLOBAL_MAX_CONCURRENT_REQ",
  "GLOBAL_MAX_CONCURRENT_REQ_TIME",
  "MS_MAX_CONCURRENT_REQ",
  "MS_MAX_CONCURRENT_REQ_TIME",
  "NETWORK_ERROR",
  "HTTP_429",
  "HTTP_502",
  "HTTP_503",
  "HTTP_504",
]);

function isRetryable(code, httpStatus) {
  if (RETRYABLE_CODES.has(String(code || "").trim())) return true;
  if ([429, 502, 503, 504].includes(Number(httpStatus || 0))) return true;
  if (Number(httpStatus || 0) === 0) return true;
  return false;
}

function normalizeVatLine(s) {
  return String(s || "")
    .trim()
    .replace(/\s+/g, "")
    .replace(/[^A-Za-z0-9]/g, "")
    .toUpperCase();
}

function maskKey(key) {
  const s = String(key || "");
  const parts = s.split(":");
  if (parts.length !== 2) return s;
  const cc = parts[0];
  const vat = parts[1] || "";
  const tail = vat.slice(-4);
  return `${cc}:…${tail}`;
}

function logIf(debugEnabled, ...args) {
  if (debugEnabled || process.env.WORKER_LOG === "1") console.log(...args);
}

async function fetchJson(url, init, timeoutMs = VIES_TIMEOUT_MS) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);

  try {
    const resp = await fetch(url, {
      ...init,
      signal: ctrl.signal,
      headers: { Accept: "application/json", ...(init?.headers || {}) },
    });

    const text = await resp.text();
    let data;
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = { raw: text };
    }

    return { ok: resp.ok, status: resp.status, data };
  } catch (e) {
    return {
      ok: false,
      status: 0,
      data: { error: "NETWORK_ERROR", message: String(e?.message || e) },
    };
  } finally {
    clearTimeout(t);
  }
}

function isCommonResponse(data) {
  return data && typeof data === "object" && "actionSucceed" in data && "errorWrappers" in data;
}
function extractErrorCode(data) {
  const wrappers = data?.errorWrappers;
  if (Array.isArray(wrappers) && wrappers.length) return wrappers[0]?.error || null;
  if (typeof data?.error === "string") return data.error;
  return null;
}
function extractErrorMessage(data) {
  const wrappers = data?.errorWrappers;
  if (Array.isArray(wrappers) && wrappers.length) return wrappers[0]?.message || "";
  if (typeof data?.message === "string") return data.message;
  return "";
}

async function viesCheck(p, requester) {
  const body = { countryCode: p.countryCode, vatNumber: p.vatNumber };

  if (requester?.ms && requester?.vat) {
    body.requesterMemberStateCode = requester.ms;
    body.requesterNumber = normalizeVatLine(requester.vat).replace(/^[A-Z]{2}/, "");
  }

  const r = await fetchJson(`${VIES_BASE}/check-vat-number`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (isCommonResponse(r.data) && r.data.actionSucceed === false) {
    return {
      ok: false,
      status: r.status,
      errorCode: extractErrorCode(r.data),
      message: extractErrorMessage(r.data),
      data: r.data,
    };
  }

  if (!r.ok) {
    return {
      ok: false,
      status: r.status,
      errorCode: extractErrorCode(r.data) || `HTTP_${r.status || 0}`,
      message: extractErrorMessage(r.data),
      data: r.data,
    };
  }

  return { ok: true, status: r.status, data: r.data };
}

function nextDelayMs(attempt) {
  if (FIXED_RETRY_DELAY_MS > 0) return FIXED_RETRY_DELAY_MS;

  // exponential backoff met cap + jitter
  const base = 10_000; // 10s
  const max = 5 * 60_000; // 5m
  const ms = Math.min(max, base * Math.pow(2, Math.max(0, attempt - 1)));
  const jitter = Math.floor(Math.random() * 1000);
  return ms + jitter;
}

async function readAllResults(resKey) {
  const obj = await kv.hgetall(resKey);
  const out = [];
  if (!obj) return out;

  for (const v of Object.values(obj)) {
    if (typeof v === "string") {
      try {
        out.push(JSON.parse(v));
      } catch {}
    } else if (v && typeof v === "object") {
      out.push(v);
    }
  }
  return out;
}

async function acquireWorkerLock() {
  // SET key value NX EX
  // @vercel/kv ondersteunt set options nx/ex
  const ok = await kv.set("lock:vies-worker", "1", { nx: true, ex: WORKER_LOCK_SEC });
  return !!ok;
}

async function releaseWorkerLock() {
  try {
    await kv.del("lock:vies-worker");
  } catch {
    // ignore
  }
}

// Worker: leest uit queue:vies (list) en fallback uit queue:pending (hash)
async function workerSlice({ maxTasks = 4, maxMs = 2500, debugEnabled = false } = {}) {
  const locked = await acquireWorkerLock();
  if (!locked) {
    logIf(debugEnabled, "[worker] lock busy -> skip");
    return 0;
  }

  const start = Date.now();
  let processed = 0;

  const requester = {
    ms: (process.env.REQUESTER_MS || "").toUpperCase(),
    vat: process.env.REQUESTER_VAT || "",
  };

  try {
    while (processed < maxTasks && Date.now() - start < maxMs) {
      // 1) probeer list
      let raw = await kv.rpop("queue:vies");

      // 2) fallback: hash queue
      if (!raw) {
        const keys = await kv.hkeys("queue:pending");
        if (!keys || keys.length === 0) break;

        const k = keys[0];
        raw = await kv.hget("queue:pending", k);

        // haal ‘m weg zodat we niet dubbel draaien
        await kv.hdel("queue:pending", k);

        if (!raw) continue;
      }

      let task = null;
      try {
        task = typeof raw === "string" ? JSON.parse(raw) : raw;
      } catch {
        continue;
      }
      if (!task?.jobId || !task?.key || !task?.p) continue;

      const now = Date.now();
      if (task.nextRunAt && task.nextRunAt > now) {
        // nog niet aan de beurt -> terug in pending en stop (anders loop je hot)
        await kv.hset("queue:pending", { [task.key]: JSON.stringify(task) });
        await kv.expire("queue:pending", JOB_TTL_SEC);
        logIf(debugEnabled, "[worker] not due -> requeue pending", maskKey(task.key));
        break;
      }

      const metaKey = `job:${task.jobId}:meta`;
      const resKey = `job:${task.jobId}:results`;

      const meta = await kv.get(metaKey);
      if (!meta) continue;

      let cur = null;
      try {
        const curRaw = await kv.hget(resKey, task.key);
        cur = curRaw ? (typeof curRaw === "string" ? JSON.parse(curRaw) : curRaw) : null;
      } catch {
        cur = null;
      }

      if (cur) {
        cur.state = "processing";
        cur.checked_at = Date.now();
        await kv.hset(resKey, { [task.key]: JSON.stringify(cur) });
      }

      logIf(
        debugEnabled,
        "[worker] run",
        { jobId: task.jobId, key: maskKey(task.key), attempt: Number(task.attempt || 0) }
      );

      const r = await viesCheck(task.p, requester);

      if (r.ok) {
        const d = r.data || {};
        const row = {
          ...(cur || {}),
          state: d.valid ? "valid" : "invalid",
          valid: !!d.valid,
          name: d?.name && d.name !== "---" ? d.name : "",
          address: d?.address && d.address !== "---" ? d.address : "",
          error_code: "",
          error: "",
          details: d?.requestIdentifier ? `requestIdentifier=${d.requestIdentifier}` : "",
          checked_at: Date.now(),
        };

        await kv.hset(resKey, { [task.key]: JSON.stringify(row) });

        meta.done = (meta.done || 0) + 1;
        meta.status = meta.done >= meta.total ? "completed" : "running";
        meta.updated_at = Date.now();
        await kv.set(metaKey, meta, { ex: JOB_TTL_SEC });

        logIf(debugEnabled, "[worker] done", { key: maskKey(task.key), state: row.state });

        processed++;
        continue;
      }

      const code = r.errorCode || `HTTP_${r.status || 0}`;
      const details = r.message || JSON.stringify(r.data);

      if (isRetryable(code, r.status)) {
        const attempt = Number(task.attempt || 0) + 1;

        // max retries -> hard fail + done++
        if (attempt > MAX_RETRIES) {
          const row = {
            ...(cur || {}),
            state: "error",
            valid: null,
            error_code: "RETRY_EXHAUSTED",
            error: "RETRY_EXHAUSTED",
            details: String(details || "").slice(0, 1000),
            attempt,
            checked_at: Date.now(),
          };

          await kv.hset(resKey, { [task.key]: JSON.stringify(row) });

          meta.done = (meta.done || 0) + 1;
          meta.status = meta.done >= meta.total ? "completed" : "running";
          meta.updated_at = Date.now();
          await kv.set(metaKey, meta, { ex: JOB_TTL_SEC });

          logIf(debugEnabled, "[worker] exhausted", { key: maskKey(task.key), code, attempt });

          processed++;
          continue;
        }

        const nextRetryAt = Date.now() + nextDelayMs(attempt);

        const row = {
          ...(cur || {}),
          state: "retry",
          valid: null,
          error_code: code,
          error: code,
          details: String(details || "").slice(0, 1000),
          attempt,
          next_retry_at: nextRetryAt,
          checked_at: Date.now(),
        };

        await kv.hset(resKey, { [task.key]: JSON.stringify(row) });

        meta.status = "running";
        meta.updated_at = Date.now();
        await kv.set(metaKey, meta, { ex: JOB_TTL_SEC });

        // requeue (pending is het meest stabiel in deze setup)
        task.attempt = attempt;
        task.nextRunAt = nextRetryAt;
        await kv.hset("queue:pending", { [task.key]: JSON.stringify(task) });
        await kv.expire("queue:pending", JOB_TTL_SEC);

        logIf(debugEnabled, "[worker] retry", {
          key: maskKey(task.key),
          code,
          attempt,
          next: new Date(nextRetryAt).toISOString(),
        });

        processed++;
        continue;
      }

      // non-retryable -> error + done++
      const row = {
        ...(cur || {}),
        state: "error",
        valid: null,
        error_code: code,
        error: code,
        details: String(details || "").slice(0, 1000),
        checked_at: Date.now(),
      };

      await kv.hset(resKey, { [task.key]: JSON.stringify(row) });

      meta.done = (meta.done || 0) + 1;
      meta.status = meta.done >= meta.total ? "completed" : "running";
      meta.updated_at = Date.now();
      await kv.set(metaKey, meta, { ex: JOB_TTL_SEC });

      logIf(debugEnabled, "[worker] error", { key: maskKey(task.key), code });

      processed++;
    }

    return processed;
  } finally {
    await releaseWorkerLock();
  }
}

export default async function handler(req, res) {
  const { id } = req.query;
  if (!id) return res.status(400).json({ error: "Missing id" });

  const wantDebug = String(req.query.debug || "") === "1";

  const debug = {
    processed: 0,
    queue_vies_len: null,
    queue_pending_len: null,
    error: null,
  };

  try {
    debug.processed = await workerSlice({ maxTasks: 4, maxMs: 2500, debugEnabled: wantDebug });

    try {
      debug.queue_vies_len = await kv.llen("queue:vies");
    } catch (e) {
      debug.queue_vies_len = `ERR:${String(e?.message || e)}`;
    }

    try {
      debug.queue_pending_len = await kv.hlen("queue:pending");
    } catch (e) {
      debug.queue_pending_len = `ERR:${String(e?.message || e)}`;
    }

    const metaKey = `job:${id}:meta`;
    const resKey = `job:${id}:results`;

    const job = await kv.get(metaKey);
    if (!job) return res.status(404).json({ error: "Not found", ...(wantDebug ? { debug } : {}) });

    // cosmetisch: queued -> running als er werk is
    if (job.status === "queued" && (job.total || 0) > 0) {
      job.status = "running";
      job.updated_at = Date.now();
      await kv.set(metaKey, job, { ex: JOB_TTL_SEC });
    }

    const results = await readAllResults(resKey);

    return res.status(200).json(wantDebug ? { job, results, debug } : { job, results });
  } catch (e) {
    debug.error = String(e?.message || e);
    return res.status(500).json({ error: "fr-job failed", debug });
  }
}
