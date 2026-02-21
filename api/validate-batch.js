// api/validate-batch.js
import { kv } from "@vercel/kv";
import { randomUUID } from "crypto";

const VIES_BASE = "https://ec.europa.eu/taxation_customs/vies/rest-api";
const STATUS_CACHE_SEC = 30;

const REQUESTER_MS = (process.env.REQUESTER_MS || "").toUpperCase();
const REQUESTER_VAT = process.env.REQUESTER_VAT || "";

const VIES_TIMEOUT_MS = Number(process.env.VIES_TIMEOUT_MS || 20000);
const JOB_TTL_SEC = 6 * 60 * 60; // 6 uur

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
  if (s === 0) return true;
  return false;
}

function pendingField(jobId, key) {
  return `${jobId}|${key}`;
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

async function getViesStatusSnapshot() {
  try {
    const cacheKey = "vies:status";
    const cached = await kv.get(cacheKey);
    if (cached) {
      if (typeof cached === "string") {
        try {
          return JSON.parse(cached);
        } catch {
          return cached;
        }
      }
      return cached;
    }

    const r = await fetchJson(`${VIES_BASE}/check-status`, { method: "GET" }, 10_000);
    const list = Array.isArray(r?.data?.countries)
      ? r.data.countries.map((c) => ({ countryCode: c.countryCode, availability: c.availability }))
      : [];

    await kv.set(cacheKey, list, { ex: STATUS_CACHE_SEC });
    return list;
  } catch {
    return [];
  }
}

function normalizeVatLine(s) {
  return String(s || "")
    .trim()
    .replace(/\s+/g, "")
    .replace(/[^A-Za-z0-9]/g, "")
    .toUpperCase();
}

function parseVat(line) {
  const v = normalizeVatLine(line);
  if (v.length < 3) return null;

  let countryCode = v.slice(0, 2);
  if (!/^[A-Z]{2}$/.test(countryCode)) return null;

  if (countryCode === "GR") countryCode = "EL"; // VIES uses EL
  const vatNumber = v.slice(2);
  if (!vatNumber) return null;

  return { input: line, countryCode, vatNumber, vat_number: v };
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

function requesterString() {
  if (!REQUESTER_MS || !REQUESTER_VAT) return "";
  const reqNo = normalizeVatLine(REQUESTER_VAT).replace(/^[A-Z]{2}/, "");
  return `${REQUESTER_MS}${reqNo}`;
}

function rowBase(p, case_ref) {
  return {
    input: p.input,
    source: "vies",
    vat_number: p.vat_number,
    country_code: p.countryCode,
    vat_part: p.vatNumber,
    requester: requesterString(),
    case_ref,
    checked_at: Date.now(),
  };
}

function rowFromQueued(p, case_ref) {
  return {
    ...rowBase(p, case_ref),
    state: "queued",
    valid: null,
    name: "",
    address: "",
    error_code: "",
    error: "",
    details: "",
    attempt: 0,
    next_retry_at: null,
  };
}

function rowFromOk(p, d, case_ref) {
  return {
    ...rowBase(p, case_ref),
    state: d?.valid ? "valid" : "invalid",
    valid: !!d?.valid,
    name: d?.name && d.name !== "---" ? d.name : "",
    address: d?.address && d.address !== "---" ? d.address : "",
    error_code: "",
    error: "",
    details: d?.requestIdentifier ? `requestIdentifier=${d.requestIdentifier}` : "",
    attempt: 0,
    next_retry_at: null,
  };
}

function rowFromError(p, errorCode, details, case_ref) {
  return {
    ...rowBase(p, case_ref),
    state: "error",
    valid: null,
    name: "",
    address: "",
    error_code: errorCode || "ERROR",
    error: errorCode || "ERROR",
    details: details ? String(details).slice(0, 1000) : "",
    attempt: 0,
    next_retry_at: null,
  };
}

async function viesCheck(p) {
  const body = { countryCode: p.countryCode, vatNumber: p.vatNumber };

  if (REQUESTER_MS && REQUESTER_VAT) {
    body.requesterMemberStateCode = REQUESTER_MS;
    body.requesterNumber = normalizeVatLine(REQUESTER_VAT).replace(/^[A-Z]{2}/, "");
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

// simpele concurrency limiter
async function mapLimit(arr, limit, fn) {
  const out = new Array(arr.length);
  let i = 0;

  const workers = Array.from({ length: Math.min(limit, arr.length) }, async () => {
    while (true) {
      const idx = i++;
      if (idx >= arr.length) break;
      out[idx] = await fn(arr[idx], idx);
    }
  });

  await Promise.all(workers);
  return out;
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

    const body = typeof req.body === "string" ? JSON.parse(req.body) : req.body || {};
    const vat_numbers = Array.isArray(body?.vat_numbers) ? body.vat_numbers : [];
    const case_ref = (body?.case_ref || "").toString().slice(0, 80);

    const parsed = vat_numbers.map(parseVat).filter(Boolean);

    // dedupe
    const seen = new Set();
    const unique = [];
    let duplicates_ignored = 0;

    for (const p of parsed) {
      const key = `${p.countryCode}:${p.vatNumber}`;
      if (seen.has(key)) {
        duplicates_ignored++;
        continue;
      }
      seen.add(key);
      unique.push(p);
    }

    let fr_job_id = null;
    const realtime = [];
    const queued = [];

    const checked = await mapLimit(unique, 6, async (p) => {
      // FR altijd async
      if (p.countryCode === "FR") {
        if (!fr_job_id) fr_job_id = randomUUID();
        return { kind: "queued", row: rowFromQueued(p, case_ref), key: `${p.countryCode}:${p.vatNumber}` };
      }

      const r = await viesCheck(p);
      if (r.ok) return { kind: "realtime", row: rowFromOk(p, r.data, case_ref) };

      const code = r.errorCode || `HTTP_${r.status || 0}`;
      const details = r.message || JSON.stringify(r.data);

      if (isRetryable(code, r.status)) {
        if (!fr_job_id) fr_job_id = randomUUID();
        return { kind: "queued", row: rowFromQueued(p, case_ref), key: `${p.countryCode}:${p.vatNumber}` };
      }

      return { kind: "realtime", row: rowFromError(p, code, details, case_ref) };
    });

    for (const x of checked) {
      if (x.kind === "queued") queued.push(x);
      else realtime.push(x);
    }

    // schrijf job + queued rows naar KV
    if (fr_job_id) {
      const metaKey = `job:${fr_job_id}:meta`;
      const resKey = `job:${fr_job_id}:results`;

      const meta = {
        job_id: fr_job_id,
        status: "queued",
        total: queued.length,
        done: 0,
        created_at: Date.now(),
        updated_at: Date.now(),
      };

      await kv.set(metaKey, meta, { ex: JOB_TTL_SEC });

      for (const q of queued) {
        await kv.hset(resKey, { [q.key]: JSON.stringify(q.row) });

        const task = {
          jobId: fr_job_id,
          key: q.key,
          p: {
            input: q.row.input,
            countryCode: q.row.country_code,
            vatNumber: q.row.vat_part,
            vat_number: q.row.vat_number,
          },
          attempt: 0,
          nextRunAt: Date.now(),
          case_ref,
          // lease fields (optional; fr-job claim code werkt ook zonder)
          leaseOwner: "",
          leaseAt: 0,
          leaseUntil: 0,
        };

        // list queue
        await kv.lpush("queue:vies", JSON.stringify(task));

        // pending queue (collision-safe): jobId|country:vat
        await kv.hset("queue:pending", { [pendingField(fr_job_id, q.key)]: JSON.stringify(task) });
      }

      await kv.expire("queue:pending", JOB_TTL_SEC);
      await kv.expire(resKey, JOB_TTL_SEC);
    }

    const results = [...realtime.map((x) => x.row), ...queued.map((x) => x.row)];
    const vies_status = await getViesStatusSnapshot();

    return res.status(200).json({
      duplicates_ignored,
      vies_status,
      results,
      fr_job_id,
      count: results.length,
    });
  } catch (e) {
    return res.status(500).json({
      error: "validate-batch failed",
      message: String(e?.message || e),
      stack: String(e?.stack || ""),
    });
  }
}
