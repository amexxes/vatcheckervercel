// api/fr-job/[id].js
import { kv } from "@vercel/kv";

const VIES_BASE = "https://ec.europa.eu/taxation_customs/vies/rest-api";
const VIES_TIMEOUT_MS = Number(process.env.VIES_TIMEOUT_MS || 20000);
const JOB_TTL_SEC = 6 * 60 * 60;

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
    try { data = text ? JSON.parse(text) : null; } catch { data = { raw: text }; }

    return { ok: resp.ok, status: resp.status, data };
  } catch (e) {
    return { ok: false, status: 0, data: { error: "NETWORK_ERROR", message: String(e?.message || e) } };
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
    return { ok: false, status: r.status, errorCode: extractErrorCode(r.data), message: extractErrorMessage(r.data), data: r.data };
  }
  if (!r.ok) {
    return { ok: false, status: r.status, errorCode: extractErrorCode(r.data) || `HTTP_${r.status || 0}`, message: extractErrorMessage(r.data), data: r.data };
  }
  return { ok: true, status: r.status, data: r.data };
}

function backoffMs(attempt) {
  // simpel + veilig (kan later fancier)
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
    try { out.push(JSON.parse(v)); } catch { /* ignore */ }
  }
  return out;
}

async function workerSlice({ maxTasks = 4, maxMs = 2500 } = {}) {
  const start = Date.now();
  let processed = 0;

  const requester = {
    ms: (process.env.REQUESTER_MS || "").toUpperCase(),
    vat: process.env.REQUESTER_VAT || "",
  };

  while (processed < maxTasks && (Date.now() - start) < maxMs) {
    const raw = await kv.rpop("queue:vies");
    if (!raw) break;

    let task;
    try { task = JSON.parse(raw); } catch { continue; }

    const now = Date.now();
    if (task.nextRunAt && task.nextRunAt > now) {
      // nog niet aan de beurt -> terugzetten en stoppen (voorkomt eindeloos draaien)
      await kv.lpush("queue:vies", raw);
      break;
    }

    const metaKey = `job:${task.jobId}:meta`;
    const resKey = `job:${task.jobId}:results`;

    const meta = await kv.get(metaKey);
    if (!meta) continue;

    // row -> processing
    let cur;
    try {
      const curRaw = await kv.hget(resKey, task.key);
      cur = curRaw ? JSON.parse(curRaw) : null;
    } catch { cur = null; }

    if (cur) {
      cur.state = "processing";
      cur.checked_at = Date.now();
      await kv.hset(resKey, { [task.key]: JSON.stringify(cur) });
    }

    // call VIES
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

      processed++;
      continue;
    }

    const code = r.errorCode || `HTTP_${r.status || 0}`;
    const details = r.message || JSON.stringify(r.data);

    if (isRetryable(code, r.status)) {
      const attempt = Number(task.attempt || 0) + 1;
      const nextRetryAt = Date.now() + backoffMs(attempt);

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

      // requeue
      task.attempt = attempt;
      task.nextRunAt = nextRetryAt;
      await kv.lpush("queue:vies", JSON.stringify(task));

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

    processed++;
  }

  return processed;
}

export default async function handler(req, res) {
  const { id } = req.query;
  if (!id) return res.status(400).json({ error: "Missing id" });

  // kleine verwerking zodat polling ook “werk” doet
  await workerSlice({ maxTasks: 4, maxMs: 2500 });

  const metaKey = `job:${id}:meta`;
  const resKey = `job:${id}:results`;

  const job = await kv.get(metaKey);
  if (!job) return res.status(404).json({ error: "Not found" });

  const results = await readAllResults(resKey);

  return res.status(200).json({ job, results });
}
