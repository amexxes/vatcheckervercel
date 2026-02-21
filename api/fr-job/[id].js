// api/fr-job/[id].js
import { kv } from "@vercel/kv";

const VIES_BASE = "https://ec.europa.eu/taxation_customs/vies/rest-api";
const VIES_TIMEOUT_MS = Number(process.env.VIES_TIMEOUT_MS || 20000);
const JOB_TTL_SEC = 6 * 60 * 60;

// Retries
const MAX_RETRIES = Number(process.env.MAX_RETRIES || 55); // interpreted as max TOTAL attempts
const FIXED_RETRY_DELAY_MS = Number(process.env.RETRY_DELAY_MS || 0); // 0 = exponential backoff
const WORKER_LOCK_SEC = Number(process.env.WORKER_LOCK_SEC || 8);

// Pending scan
const PENDING_SCAN_LIMIT = Number(process.env.PENDING_SCAN_LIMIT || 200);

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
  const c = String(code || "").trim();
  const s = Number(httpStatus || 0);
  if (RETRYABLE_CODES.has(c)) return true;
  if ([429, 502, 503, 504].includes(s)) return true;
  if (s === 0) return true; // network/abort
  return false;
}

function normalizeVatLine(s) {
  return String(s || "")
    .trim()
    .replace(/\s+/g, "")
    .replace(/[^A-Za-z0-9]/g, "")
    .toUpperCase();
}

function parseVatKey(key) {
  const s = String(key || "");
  const parts = s.split(":");
  if (parts.length === 2) return { cc: parts[0], vat: parts[1] || "" };
  // fallback if some other separator was used
  const cc = s.slice(0, 2);
  const vat = s.slice(2);
  return { cc, vat };
}

function maskKey(key) {
  const { cc, vat } = parseVatKey(key);
  const tail = String(vat || "").slice(-4);
  return `${cc}:…${tail}`;
}

function logIf(debugEnabled, ...args) {
  if (debugEnabled || process.env.WORKER_LOG === "1") console.log(...args);
}

function safeJsonParse(x) {
  if (!x) return null;
  if (typeof x === "object") return x;
  if (typeof x !== "string") return null;
  try {
    return JSON.parse(x);
  } catch {
    return null;
  }
}

function isFinalState(state) {
  const s = String(state || "").toLowerCase();
  return s === "valid" || s === "invalid" || s === "error";
}

function pendingField(task) {
  // Avoid collisions across jobs; still compatible with old fields.
  // Field name is only used inside queue:pending hash.
  return `${task.jobId}|${task.key}`;
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
    const isAbort = String(e?.name || "").toLowerCase() === "aborterror";
    return {
      ok: false,
      status: 0,
      data: { error: isAbort ? "TIMEOUT" : "NETWORK_ERROR", message: String(e?.message || e) },
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
      const j = safeJsonParse(v);
      if (j) out.push(j);
    } else if (v && typeof v === "object") {
      out.push(v);
    }
  }
  return out;
}

async function acquireWorkerLock() {
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

async function getMeta(metaKey) {
  const meta = await kv.get(metaKey);
  if (!meta) return null;
  if (typeof meta === "string") {
    return safeJsonParse(meta);
  }
  return meta;
}

async function setMeta(metaKey, meta) {
  meta.updated_at = Date.now();
  await kv.set(metaKey, meta, { ex: JOB_TTL_SEC });
}

async function ensureKeyTTL(resKey) {
  try {
    await kv.expire(resKey, JOB_TTL_SEC);
  } catch {
    // ignore
  }
}

function buildBaseRow(cur, task) {
  const { cc, vat } = parseVatKey(task?.key);
  const country_code = (cur && cur.country_code) || (task?.p?.countryCode || cc || "").toUpperCase();
  const vat_part = (cur && cur.vat_part) || (task?.p?.vatNumber || vat || "");
  const vat_number = (cur && cur.vat_number) || `${country_code}${vat_part}`;

  return {
    ...(cur || {}),
    country_code,
    vat_part,
    vat_number,
    input: (cur && cur.input) || vat_number,
  };
}

async function popDuePendingTask(nowMs, debugEnabled, debugObj) {
  const fields = await kv.hkeys("queue:pending");
  if (!fields || fields.length === 0) return null;

  const scan = fields.slice(0, Math.min(fields.length, PENDING_SCAN_LIMIT));
  if (debugObj) debugObj.pending_scanned = scan.length;

  for (const field of scan) {
    const raw = await kv.hget("queue:pending", field);
    if (!raw) {
      await kv.hdel("queue:pending", field);
      continue;
    }

    const task = safeJsonParse(raw);
    if (!task?.jobId || !task?.key || !task?.p) {
      await kv.hdel("queue:pending", field);
      continue;
    }

    const dueAt = Number(task.nextRunAt || 0);
    if (!dueAt || dueAt <= nowMs) {
      await kv.hdel("queue:pending", field);
      logIf(debugEnabled, `[worker] pop pending due`, maskKey(task.key));
      return task;
    }
  }

  // Nothing due in scan window
  return null;
}

// Exported so Cron endpoint can reuse it.
export async function runWorkerSlice({ maxTasks = 4, maxMs = 2500, debugEnabled = false, source = "poll" } = {}) {
  const locked = await acquireWorkerLock();
  if (!locked) {
    logIf(debugEnabled, `[worker:${source}] lock busy -> skip`);
    return { processed: 0, locked: false, skippedFinal: 0, requeuedFuture: 0 };
  }

  const start = Date.now();
  let processed = 0;
  let skippedFinal = 0;
  let requeuedFuture = 0;

  const requester = {
    ms: (process.env.REQUESTER_MS || "").toUpperCase(),
    vat: process.env.REQUESTER_VAT || "",
  };

  const debugObj = debugEnabled ? { pending_scanned: 0 } : null;

  try {
    while (processed < maxTasks && Date.now() - start < maxMs) {
      // 1) primary list queue
      let raw = await kv.rpop("queue:vies");
      let task = raw ? safeJsonParse(raw) : null;

      // 2) fallback pending hash (due-only)
      if (!task) {
        task = await popDuePendingTask(Date.now(), debugEnabled, debugObj);
      }

      if (!task) break;
      if (!task?.jobId || !task?.key || !task?.p) continue;

      const now = Date.now();

      // If task is scheduled in the future (should normally live in pending), move it and continue.
      if (task.nextRunAt && Number(task.nextRunAt) > now) {
        await kv.hset("queue:pending", { [pendingField(task)]: JSON.stringify(task) });
        await kv.expire("queue:pending", JOB_TTL_SEC);
        requeuedFuture++;
        processed++; // we consumed an item from the list
        continue;
      }

      const metaKey = `job:${task.jobId}:meta`;
      const resKey = `job:${task.jobId}:results`;

      const meta = await getMeta(metaKey);
      if (!meta) {
        processed++;
        continue;
      }

      let cur = null;
      try {
        const curRaw = await kv.hget(resKey, task.key);
        cur = curRaw ? safeJsonParse(curRaw) : null;
      } catch {
        cur = null;
      }

      // Idempotency + meta self-heal: if already final, do not run VIES again.
      if (cur && isFinalState(cur.state)) {
        if (!cur.done_counted) {
          cur.done_counted = true;
          await kv.hset(resKey, { [task.key]: JSON.stringify(cur) });
          await ensureKeyTTL(resKey);

          meta.done = Number(meta.done || 0) + 1;
          meta.status = Number(meta.done || 0) >= Number(meta.total || 0) ? "completed" : "running";
          await setMeta(metaKey, meta);
        }
        skippedFinal++;
        processed++;
        continue;
      }

      // Attempt semantics:
      // task.attempt = number of attempts already performed (after last write).
      // currentAttempt = this attempt number (1..)
      const currentAttempt = Math.max(1, Number(task.attempt || 0) + 1);

      // Mark row as processing (and clear next_retry_at; avoid "processing stuck with old ETA")
      const processingRow = {
        ...buildBaseRow(cur, task),
        state: "processing",
        attempt: currentAttempt,
        next_retry_at: null,
        checked_at: now,
        // keep last error fields visible while processing? (optional)
        error_code: cur?.error_code || "",
        error: cur?.error || "",
        details: cur?.details || "",
      };

      await kv.hset(resKey, { [task.key]: JSON.stringify(processingRow) });
      await ensureKeyTTL(resKey);

      logIf(debugEnabled, `[worker:${source}] run`, {
        jobId: task.jobId,
        key: maskKey(task.key),
        attempt: currentAttempt,
      });

      const r = await viesCheck(task.p, requester);

      if (r.ok) {
        const d = r.data || {};
        const valid = !!d.valid;

        const row = {
          ...buildBaseRow(cur, task),
          state: valid ? "valid" : "invalid",
          valid,
          name: d?.name && d.name !== "---" ? d.name : "",
          address: d?.address && d.address !== "---" ? d.address : "",
          error_code: "",
          error: "",
          details: d?.requestIdentifier ? `requestIdentifier=${d.requestIdentifier}` : "",
          next_retry_at: null,
          attempt: currentAttempt,
          checked_at: Date.now(),
          done_counted: true,
        };

        await kv.hset(resKey, { [task.key]: JSON.stringify(row) });
        await ensureKeyTTL(resKey);

        // meta.done increments only once per row (via done_counted)
        const prevCounted = !!(cur && cur.done_counted);
        if (!prevCounted) meta.done = Number(meta.done || 0) + 1;

        meta.status = Number(meta.done || 0) >= Number(meta.total || 0) ? "completed" : "running";
        await setMeta(metaKey, meta);

        logIf(debugEnabled, `[worker:${source}] done`, { key: maskKey(task.key), state: row.state, attempt: currentAttempt });

        processed++;
        continue;
      }

      const code = String(r.errorCode || `HTTP_${r.status || 0}`).trim();
      const details = String(r.message || JSON.stringify(r.data || {})).slice(0, 1000);

      if (isRetryable(code, r.status)) {
        // Exhausted?
        if (currentAttempt >= MAX_RETRIES) {
          const row = {
            ...buildBaseRow(cur, task),
            state: "error",
            valid: null,
            error_code: "RETRY_EXHAUSTED",
            error: "RETRY_EXHAUSTED",
            details,
            next_retry_at: null,
            attempt: currentAttempt,
            checked_at: Date.now(),
            done_counted: true,
          };

          await kv.hset(resKey, { [task.key]: JSON.stringify(row) });
          await ensureKeyTTL(resKey);

          const prevCounted = !!(cur && cur.done_counted);
          if (!prevCounted) meta.done = Number(meta.done || 0) + 1;

          meta.status = Number(meta.done || 0) >= Number(meta.total || 0) ? "completed" : "running";
          await setMeta(metaKey, meta);

          logIf(debugEnabled, `[worker:${source}] exhausted`, { key: maskKey(task.key), code, attempt: currentAttempt });

          processed++;
          continue;
        }

        const nextRetryAt = Date.now() + nextDelayMs(currentAttempt);

        // IMPORTANT: clear name/address on retry to avoid "looks like a result but state=retry"
        const row = {
          ...buildBaseRow(cur, task),
          state: "retry",
          valid: null,
          name: "",
          address: "",
          error_code: code,
          error: code,
          details,
          attempt: currentAttempt,
          next_retry_at: nextRetryAt,
          checked_at: Date.now(),
        };

        await kv.hset(resKey, { [task.key]: JSON.stringify(row) });
        await ensureKeyTTL(resKey);

        meta.status = "running";
        await setMeta(metaKey, meta);

        // Requeue task into pending (collision-safe)
        task.attempt = currentAttempt; // attempts performed so far
        task.nextRunAt = nextRetryAt;
        await kv.hset("queue:pending", { [pendingField(task)]: JSON.stringify(task) });
        await kv.expire("queue:pending", JOB_TTL_SEC);

        logIf(debugEnabled, `[worker:${source}] retry`, {
          key: maskKey(task.key),
          code,
          attempt: currentAttempt,
          next: new Date(nextRetryAt).toISOString(),
        });

        processed++;
        continue;
      }

      // Non-retryable error -> final
      const row = {
        ...buildBaseRow(cur, task),
        state: "error",
        valid: null,
        error_code: code,
        error: code,
        details,
        next_retry_at: null,
        attempt: currentAttempt,
        checked_at: Date.now(),
        done_counted: true,
      };

      await kv.hset(resKey, { [task.key]: JSON.stringify(row) });
      await ensureKeyTTL(resKey);

      const prevCounted = !!(cur && cur.done_counted);
      if (!prevCounted) meta.done = Number(meta.done || 0) + 1;

      meta.status = Number(meta.done || 0) >= Number(meta.total || 0) ? "completed" : "running";
      await setMeta(metaKey, meta);

      logIf(debugEnabled, `[worker:${source}] error`, { key: maskKey(task.key), code, attempt: currentAttempt });

      processed++;
    }

    return { processed, locked: true, skippedFinal, requeuedFuture, ...(debugObj ? { debug: debugObj } : {}) };
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
    locked: null,
    skippedFinal: 0,
    requeuedFuture: 0,
    queue_vies_len: null,
    queue_pending_len: null,
    pending_scanned: null,
    error: null,
  };

  try {
    const slice = await runWorkerSlice({ maxTasks: 4, maxMs: 2500, debugEnabled: wantDebug, source: "poll" });
    debug.processed = slice.processed;
    debug.locked = slice.locked;
    debug.skippedFinal = slice.skippedFinal || 0;
    debug.requeuedFuture = slice.requeuedFuture || 0;
    debug.pending_scanned = slice?.debug?.pending_scanned ?? null;

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

    const job = await getMeta(metaKey);
    if (!job) return res.status(404).json({ error: "Not found", ...(wantDebug ? { debug } : {}) });

    // If job was created as queued, flip to running once polling starts
    if (job.status === "queued" && Number(job.total || 0) > 0) {
      job.status = "running";
      await setMeta(metaKey, job);
    }

    const results = await readAllResults(resKey);
    return res.status(200).json(wantDebug ? { job, results, debug } : { job, results });
  } catch (e) {
    debug.error = String(e?.message || e);
    return res.status(500).json({ error: "fr-job failed", debug });
  }
}
