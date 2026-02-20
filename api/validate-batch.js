// api/validate-batch.js
const VIES_BASE = "https://ec.europa.eu/taxation_customs/vies/rest-api";

const REQUESTER_MS = (process.env.REQUESTER_MS || "").toUpperCase();
const REQUESTER_VAT = process.env.REQUESTER_VAT || "";

const VIES_TIMEOUT_MS = Number(process.env.VIES_TIMEOUT_MS || 20000);

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

// simpele concurrency limiter (zoals je mapLimit)
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
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  const vat_numbers = Array.isArray(req.body?.vat_numbers) ? req.body.vat_numbers : [];
  const case_ref = (req.body?.case_ref || "").toString().slice(0, 80);

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

  const results = await mapLimit(unique, 6, async (p) => {
    const r = await viesCheck(p);
    if (r.ok) return rowFromOk(p, r.data, case_ref);

    const code = r.errorCode || `HTTP_${r.status || 0}`;
    const details = r.message || JSON.stringify(r.data);
    return rowFromError(p, code, details, case_ref);
  });

  return res.status(200).json({
    duplicates_ignored,
    vies_status: [],
    results,
    fr_job_id: null,
    count: results.length,
  });
}
