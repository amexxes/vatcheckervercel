// /src/App.tsx
import React, { useEffect, useMemo, useRef, useState } from "react";
import L from "leaflet";
import type { FrJobResponse, ValidateBatchResponse, VatRow } from "./types";

type SortState = { colIndex: number | null; asc: boolean };
type SavedRun = { id: string; ts: number; caseRef: string; input: string; results: VatRow[] };

const COUNTRY_COORDS: Record<string, { lat: number; lon: number }> = {
  AT:{lat:48.2082,lon:16.3738}, BE:{lat:50.8503,lon:4.3517}, BG:{lat:42.6977,lon:23.3219},
  CY:{lat:35.1856,lon:33.3823}, CZ:{lat:50.0755,lon:14.4378}, DE:{lat:52.52,lon:13.405},
  DK:{lat:55.6761,lon:12.5683}, EE:{lat:59.437,lon:24.7536}, EL:{lat:37.9838,lon:23.7275},
  ES:{lat:40.4168,lon:-3.7038}, FI:{lat:60.1699,lon:24.9384}, FR:{lat:48.8566,lon:2.3522},
  HR:{lat:45.815,lon:15.9819}, HU:{lat:47.4979,lon:19.0402}, IE:{lat:53.3498,lon:-6.2603},
  IT:{lat:41.9028,lon:12.4964}, LT:{lat:54.6872,lon:25.2797}, LU:{lat:49.6116,lon:6.1319},
  LV:{lat:56.9496,lon:24.1052}, MT:{lat:35.8989,lon:14.5146}, NL:{lat:52.3676,lon:4.9041},
  PL:{lat:52.2297,lon:21.0122}, PT:{lat:38.7223,lon:-9.1393}, RO:{lat:44.4268,lon:26.1025},
  SE:{lat:59.3293,lon:18.0686}, SI:{lat:46.0569,lon:14.5058}, SK:{lat:48.1486,lon:17.1077},
  XI:{lat:54.5973,lon:-5.9301},
};

const ERROR_MAP: Record<string, string> = {
  MS_MAX_CONCURRENT_REQ: "Member State heeft te veel gelijktijdige checks; we proberen later opnieuw.",
  MS_UNAVAILABLE: "Member State is tijdelijk niet beschikbaar; we proberen later opnieuw.",
  TIMEOUT: "Timeout richting VIES; we proberen later opnieuw.",
  GLOBAL_MAX_CONCURRENT_REQ: "VIES is druk; we proberen later opnieuw.",
  SERVICE_UNAVAILABLE: "VIES service unavailable; we proberen later opnieuw.",
  NETWORK_ERROR: "Netwerkfout richting VIES; we proberen later opnieuw.",
};

function normalizeLine(s: string): string {
  return String(s || "")
    .trim()
    .replace(/\s+/g, "")
    .replace(/[^A-Za-z0-9]/g, "")
    .toUpperCase();
}

function stateClass(state?: string): string {
  const s = String(state || "").toLowerCase();
  if (["valid","invalid","retry","queued","processing","error"].includes(s)) return s;
  return "queued";
}

function stateLabel(state?: string): string {
  const s = String(state || "").toLowerCase();
  return s || "unknown";
}

function valText(v: unknown): string {
  if (v === true) return "true";
  if (v === false) return "false";
  if (v === null || v === undefined || v === "") return "";
  return String(v);
}

function humanError(code?: string, fallback?: string) {
  const c = (code || "").trim();
  return ERROR_MAP[c] || fallback || c || "";
}

function formatEta(ts?: number) {
  if (!ts) return "";
  const diff = Math.max(0, ts - Date.now());
  const s = Math.round(diff / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.round(s / 60);
  return `${m}m`;
}

function validateFormat(vatNumberWithPrefix: string) {
  const v = normalizeLine(vatNumberWithPrefix);
  if (v.length < 3) return { ok: false, reason: "Too short" };
  const cc = v.slice(0, 2);
  if (!/^[A-Z]{2}$/.test(cc)) return { ok: false, reason: "Missing country prefix" };
  const rest = v.slice(2);
  if (!rest) return { ok: false, reason: "Missing VAT digits" };
  if (!/^[A-Z0-9]+$/.test(rest)) return { ok: false, reason: "Invalid characters" };
  return { ok: true, reason: "" };
}

function computeCountryCountsFromInput(text: string): Record<string, number> {
  const lines = text.split(/\r?\n/).map((x) => x.trim()).filter(Boolean);
  const seen = new Set<string>();
  const counts: Record<string, number> = {};

  for (const line of lines) {
    const v = normalizeLine(line);
    if (!v || v.length < 2) continue;

    let cc = v.slice(0, 2);
    if (!/^[A-Z]{2}$/.test(cc)) continue;
    if (cc === "GR") cc = "EL";

    const key = cc + v.slice(2);
    if (seen.has(key)) continue;
    seen.add(key);

    counts[cc] = (counts[cc] || 0) + 1;
  }
  return counts;
}

export default function App() {
  const [vatInput, setVatInput] = useState<string>("");
  const [caseRef, setCaseRef] = useState<string>("");
  const [filter, setFilter] = useState<string>("");

  const [rows, setRows] = useState<VatRow[]>([]);
  const [loading, setLoading] = useState(false);

  const [duplicatesIgnored, setDuplicatesIgnored] = useState(0);
  const [viesStatus, setViesStatus] = useState<Array<{ countryCode: string; availability: string }>>([]);

  const [frText, setFrText] = useState("-");
  const [lastUpdate, setLastUpdate] = useState("-");
  const [progressText, setProgressText] = useState("0/0");

  const [sortState, setSortState] = useState<SortState>({ colIndex: null, asc: true });
  const [sortLabel, setSortLabel] = useState<string>("");

  const [mapLegend, setMapLegend] = useState("—");
  const [mapCount, setMapCount] = useState("0 countries");
  const [mapGeoVersion, setMapGeoVersion] = useState(0);

  const [expandedKey, setExpandedKey] = useState<string | null>(null);

  const [savedRuns, setSavedRuns] = useState<SavedRun[]>(() => {
    try { return JSON.parse(localStorage.getItem("vat_saved_runs") || "[]"); } catch { return []; }
  });

  const [notes, setNotes] = useState<Record<string, { note: string; tag: "whitelist"|"blacklist"|"" }>>(() => {
    try { return JSON.parse(localStorage.getItem("vat_notes") || "{}"); } catch { return {}; }
  });

  useEffect(() => {
    localStorage.setItem("vat_saved_runs", JSON.stringify(savedRuns.slice(0, 30)));
  }, [savedRuns]);

  useEffect(() => {
    localStorage.setItem("vat_notes", JSON.stringify(notes));
  }, [notes]);

  const currentFrJobIdRef = useRef<string | null>(null);
  const pollTimerRef = useRef<number | null>(null);

  const mapRef = useRef<L.Map | null>(null);
  const markerLayerRef = useRef<L.LayerGroup | null>(null);
  const geoJsonRef = useRef<any | null>(null);
  const loggedIsoRef = useRef<Set<string>>(new Set());

const ISO3_TO_ISO2: Record<string, string> = {
  FRA: "FR",
  DEU: "DE",
  NLD: "NL",
  BEL: "BE",
  LUX: "LU",
  ESP: "ES",
  PRT: "PT",
  ITA: "IT",
  IRL: "IE",
  AUT: "AT",
  DNK: "DK",
  SWE: "SE",
  FIN: "FI",
  POL: "PL",
  CZE: "CZ",
  SVK: "SK",
  SVN: "SI",
  HUN: "HU",
  ROU: "RO",
  BGR: "BG",
  HRV: "HR",
  GRC: "EL",
  CYP: "CY",
  MLT: "MT",
  EST: "EE",
  LVA: "LV",
  LTU: "LT",
  GBR: "XI"
};

function featureToVatCc(feature: any): string {
  const p = feature?.properties || {};

  const raw2 =
    p.ISO_A2 ?? p.iso_a2 ?? p.ISO2 ?? p.iso2 ?? p["alpha-2"];

  let cc2 = String(raw2 || "").toUpperCase().trim();
  if (cc2 === "GR") cc2 = "EL";
  if (cc2 === "GB") cc2 = "XI";

  if (cc2 && cc2 !== "-99") return cc2;

  const raw3 = p.ISO_A3 ?? p.iso_a3 ?? p.ISO3 ?? p.iso3;
  const cc3 = String(raw3 || "").toUpperCase().trim();

  return ISO3_TO_ISO2[cc3] || "";
}

  const countryCounts = useMemo(() => computeCountryCountsFromInput(vatInput), [vatInput]);

  const filteredRows = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter((r) => JSON.stringify(r).toLowerCase().includes(q));
  }, [rows, filter]);

  const stats = useMemo(() => {
    let total = rows.length;
    let done = 0, vOk = 0, vBad = 0, pending = 0, err = 0;

    for (const r of rows) {
      const st = String(r.state || "").toLowerCase();
      if (st === "valid") { done++; vOk++; }
      else if (st === "invalid") { done++; vBad++; }
      else if (st === "error") { done++; err++; }
      else if (st === "queued" || st === "retry" || st === "processing") { pending++; }
    }
    return { total, done, vOk, vBad, pending, err };
  }, [rows]);

  const progressPct = useMemo(() => {
    if (!stats.total) return 0;
    return Math.round((stats.done / stats.total) * 100);
  }, [stats.total, stats.done]);

  function stopPolling() {
    if (pollTimerRef.current) {
      window.clearInterval(pollTimerRef.current);
      pollTimerRef.current = null;
    }
    currentFrJobIdRef.current = null;
  }

  function enrichRow(r: VatRow): VatRow {
    const key = `${r.country_code || ""}:${r.vat_part || ""}`;
    const fmt = validateFormat(r.vat_number || r.input || "");
    const user = notes[key] || { note: "", tag: "" };
    return { ...r, format_ok: fmt.ok, format_reason: fmt.reason, note: user.note, tag: user.tag, case_ref: r.case_ref || caseRef };
  }

  async function pollFrJob(jobId: string) {
    try {
      const resp = await fetch(`/api/fr-job/${encodeURIComponent(jobId)}`);
      if (!resp.ok) return;
      const data = (await resp.json()) as FrJobResponse;

      setFrText(`${data.job.done}/${data.job.total} (${data.job.status})`);

      setRows((prev) => {
        const map = new Map<string, VatRow>();
        for (const r of prev) {
          const k = `${r.country_code || ""}:${r.vat_part || ""}` || r.vat_number || r.input || crypto.randomUUID();
          map.set(k, r);
        }

        for (const r of (data.results || [])) {
          const k = `${r.country_code || ""}:${r.vat_part || ""}` || r.vat_number || r.input || crypto.randomUUID();
          const merged = { ...map.get(k), ...r };
          map.set(k, enrichRow(merged));
        }

        return Array.from(map.values());
      });

      setLastUpdate(new Date().toLocaleString("nl-NL"));

      if (data.job.status === "completed") stopPolling();
    } catch {
      // ignore
    }
  }

  async function onValidate() {
    stopPolling();
    setExpandedKey(null);
    setRows([]);
    setFrText("-");
    setLastUpdate("-");
    setSortState({ colIndex: null, asc: true });
    setSortLabel("");
    setLoading(true);
    setDuplicatesIgnored(0);

    const lines = vatInput.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
    if (!lines.length) { setLoading(false); return; }

    try {
      const resp = await fetch("/api/validate-batch", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ vat_numbers: lines, case_ref: caseRef })
      });

      const data = (await resp.json()) as ValidateBatchResponse & any;

      setDuplicatesIgnored(data.duplicates_ignored || 0);
      setViesStatus(Array.isArray(data.vies_status) ? data.vies_status : []);

      const enriched = (data.results || []).map((r: VatRow) => enrichRow({ ...r, case_ref: caseRef }));
      setRows(enriched);

      setLastUpdate(new Date().toLocaleString("nl-NL"));

      if (data.fr_job_id) {
        currentFrJobIdRef.current = data.fr_job_id;
        await pollFrJob(data.fr_job_id);

        pollTimerRef.current = window.setInterval(() => {
          const id = currentFrJobIdRef.current;
          if (id) void pollFrJob(id);
        }, 3000);
      } else {
        setFrText("-");
      }
    } finally {
      setLoading(false);
    }
  }

  function onClear() {
    stopPolling();
    setVatInput("");
    setCaseRef("");
    setFilter("");
    setRows([]);
    setFrText("-");
    setLastUpdate("-");
    setProgressText("0/0");
    setSortState({ colIndex: null, asc: true });
    setSortLabel("");
    setDuplicatesIgnored(0);
    setViesStatus([]);
    setExpandedKey(null);
  }

  function getCellText(r: VatRow, colIndex: number): string {
    const cols: Array<string> = [
      r.state ?? "",
      r.vat_number ?? "",
      r.name ?? "",
      r.address ?? "",
      r.error_code ?? r.error ?? "",
      r.details ?? ""
    ];
    return cols[colIndex] ?? "";
  }

  function sortByColumn(colIndex: number, label: string) {
    setSortState((prevSort) => {
      const asc = prevSort.colIndex === colIndex ? !prevSort.asc : true;

      setRows((prevRows) => {
        const copy = [...prevRows];
        copy.sort((a, b) => {
          const ta = getCellText(a, colIndex).toLowerCase();
          const tb = getCellText(b, colIndex).toLowerCase();
          const cmp = ta.localeCompare(tb, "nl");
          return asc ? cmp : -cmp;
        });
        return copy;
      });

      setSortLabel(`Sort: ${label} (${asc ? "asc" : "desc"})`);
      return { colIndex, asc };
    });
  }

  useEffect(() => {
    setProgressText(`${stats.done}/${stats.total}`);
  }, [stats.done, stats.total]);

  function exportCsv() {
    const headers = ["case_ref","input","vat_number","country_code","valid","state","name","address","error_code","error","attempt","next_retry_at","note","tag","checked_at"];
    const lines = [
      headers.join(","),
      ...rows.map((r) => headers.map((h) => {
        const v = (r as any)[h];
        const s = v === null || v === undefined ? "" : String(v);
        return `"${s.replace(/"/g,'""')}"`;
      }).join(","))
    ].join("\n");

    const blob = new Blob([lines], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `vat_results_${new Date().toISOString().slice(0,19).replace(/[:T]/g,"-")}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  function saveRun() {
    const id = crypto.randomUUID();
    setSavedRuns((prev) => [{ id, ts: Date.now(), caseRef, input: vatInput, results: rows }, ...prev].slice(0, 30));
  }

  function loadRun(id: string) {
    const run = savedRuns.find((x) => x.id === id);
    if (!run) return;
    stopPolling();
    setVatInput(run.input);
    setCaseRef(run.caseRef || "");
    setRows(run.results || []);
    setLastUpdate(new Date(run.ts).toLocaleString("nl-NL"));
  }

  function updateNoteTag(row: VatRow, note: string, tag: "whitelist"|"blacklist"|"" ) {
    const key = `${row.country_code || ""}:${row.vat_part || ""}`;
    setNotes((prev) => ({ ...prev, [key]: { note, tag } }));
    setRows((prev) => prev.map((r) => {
      const k = `${r.country_code || ""}:${r.vat_part || ""}`;
      return k === key ? { ...r, note, tag } : r;
    }));
  }

function getFillColor(n: number, max: number) {
  if (max <= 0) return "#ffffff";
  const r = n / max; // 0..1

  if (r >= 0.80) return "#0b2e5f";
  if (r >= 0.55) return "#1f6aa5";
  if (r >= 0.35) return "#2bb3e6";
  if (r >= 0.18) return "#7dd3f7";
  if (r > 0) return "#cfefff";
  return "#ffffff";
}


// --- Map init ---
useEffect(() => {
  const el = document.getElementById("countryMap");
  if (!el) return;

  try {
    const map = L.map(el, {
      zoomControl: false,
      attributionControl: false,
      dragging: true,
      scrollWheelZoom: false,
      doubleClickZoom: false,
      boxZoom: false,
      keyboard: false,
    }).setView([53.5, 10], 3);

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 6,
      minZoom: 2,
    }).addTo(map);

    const layer = L.layerGroup().addTo(map);

    mapRef.current = map;
    markerLayerRef.current = layer;

    fetch("/countries.geojson")
      .then(async (r) => {
        if (!r.ok) throw new Error(`countries.geojson HTTP ${r.status}`);
        return r.json();
      })
      .then((j) => {
        console.log("countries.geojson loaded", j);
        geoJsonRef.current = j;
        setMapGeoVersion((v) => v + 1);
      })
      .catch((e) => {
        console.error("countries.geojson failed", e);
        geoJsonRef.current = null;
        setMapGeoVersion((v) => v + 1);
      });
  } catch {
    el.innerHTML =
      "<div style='padding:12px;color:#6b7280;font-size:12px;'>Map unavailable</div>";
  }

  return () => {
    if (mapRef.current) mapRef.current.remove();
    mapRef.current = null;
    markerLayerRef.current = null;
  };
}, []);
// voeg deze helper toe (boven je useEffect)
function mapIso2ToVatCc(raw: unknown): string {
  let cc = String(raw || "").toUpperCase().trim();
  if (cc === "GR") cc = "EL";          // VIES gebruikt EL voor Griekenland
  if (cc === "GB") cc = "XI";          // optioneel: kleur NI mee op UK polygon (als je XI gebruikt)
  return cc;
}

useEffect(() => {
  const entries = Object.entries(countryCounts).sort((a, b) => b[1] - a[1]);
  setMapCount(`${entries.length} countries`);

  if (!entries.length) {
    setMapLegend("—");
  } else {
    const top = entries
      .slice(0, 6)
      .map(([cc, n]) => `${cc}(${n})`)
      .join(" · ");
    const more = entries.length > 6 ? ` +${entries.length - 6}` : "";
    setMapLegend(top + more);
  }

  const map = mapRef.current;
  const layer = markerLayerRef.current;
  if (!map || !layer) return;

 layer.clearLayers();

const maxCount = Math.max(0, ...Object.values(countryCounts));

if (geoJsonRef.current) {
  L.geoJSON(geoJsonRef.current as any, {
style: (feature: any) => {
  const p = feature?.properties || {};

  const raw =
    p.ISO_A2 ?? p.iso_a2 ?? p.ISO2 ?? p.iso2 ??
    p["alpha-2"] ?? p["Alpha-2"] ?? p["ISO3166-1-Alpha-2"] ??
    p.ISO_A3 ?? p.iso_a3 ?? p.ISO3 ?? p.iso3 ??
    p.ADMIN ?? p.name ?? p.NAME ?? p.Name;

  let cc = String(raw || "").toUpperCase().trim();

  // ISO3 → ISO2 (minimaal nodig voor EU + FR)
  if (cc === "FRA") cc = "FR";
  if (cc === "DEU") cc = "DE";
  if (cc === "NLD") cc = "NL";
  if (cc === "BEL") cc = "BE";
  if (cc === "LUX") cc = "LU";
  if (cc === "ESP") cc = "ES";
  if (cc === "PRT") cc = "PT";
  if (cc === "ITA") cc = "IT";
  if (cc === "IRL") cc = "IE";
  if (cc === "GRC") cc = "EL";
  if (cc === "GBR") cc = "XI";

  if (cc === "GR") cc = "EL";
  if (cc === "GB") cc = "XI";

  // éénmalig loggen (zodat je ziet welke codes binnenkomen)
  if (cc && !loggedIsoRef.current.has(cc)) {
    loggedIsoRef.current.add(cc);
    console.log("map feature code:", cc, "count:", countryCounts[cc] || 0);
  }

  const n = cc ? (countryCounts[cc] || 0) : 0;
  const max = Math.max(0, ...Object.values(countryCounts));
  const ratio = max > 0 ? n / max : 0;

  let fill = "#ffffff";
  if (ratio >= 0.8) fill = "#0b2e5f";
  else if (ratio >= 0.55) fill = "#1f6aa5";
  else if (ratio >= 0.35) fill = "#2bb3e6";
  else if (ratio >= 0.18) fill = "#7dd3f7";
  else if (ratio > 0) fill = "#cfefff";

  return {
    color: "#0b2e5f",
    weight: 0.8,
    opacity: 0.7,
    fillColor: fill,
    fillOpacity: n ? 0.85 : 0.05,
  };
},


onEachFeature: (feature: any, lyr: any) => {
  const p = feature?.properties || {};
  const raw = p.ISO_A2 ?? p.iso_a2 ?? p.ISO2 ?? p.iso2 ?? p.ISO_A3 ?? p.iso_a3 ?? p.ISO3 ?? p.iso3;
  let cc = String(raw || "").toUpperCase().trim();

  if (cc === "FRA") cc = "FR";
  if (cc === "DEU") cc = "DE";
  if (cc === "NLD") cc = "NL";
  if (cc === "GRC") cc = "EL";
  if (cc === "GBR") cc = "XI";
  if (cc === "GR") cc = "EL";
  if (cc === "GB") cc = "XI";

  if (!cc) return;
  const n = countryCounts[cc] || 0;
  lyr.bindTooltip(`${cc} • ${n}`, { direction: "top", opacity: 0.9 });
},



  }).addTo(layer);
}


  const coords = Object.entries(countryCounts)
    .filter(([cc, n]) => n > 0 && COUNTRY_COORDS[cc])
    .map(([cc]) => {
      const c = COUNTRY_COORDS[cc];
      return L.latLng(c.lat, c.lon);
    });

  if (coords.length) {
    const b = L.latLngBounds(coords);
    map.fitBounds(b.pad(0.25), { animate: false, maxZoom: 4 });
  } else {
    map.setView([53.5, 10], 3, { animate: false } as any);
  }
}, [countryCounts, mapGeoVersion]);


  return (
    <>
      <div className="banner">
        <div className="banner-accent" />
        <div className="banner-inner">
          <div className="brand">
            <div className="mark" aria-hidden="true">
              <div className="mark-bars"><span /><span /><span /></div>
              <div className="mark-text">RSM</div>
            </div>
            <div className="title">VAT validation</div>
          </div>

          <div className="chipsRow" style={{ marginTop: 0, width: "100%", maxWidth: 560 }}>
            <div className="chip"><span>FR job</span><b className="nowrap">{frText}</b></div>
            <div className="chip"><span>Last update</span><b className="nowrap">{lastUpdate}</b></div>
          </div>
        </div>
      </div>

      <div className="wrap">
        <div className="grid">
          <div className="card">
            <h2>Input</h2>
            <p className="hint">
              Paste VAT numbers (1 per line). Non-FR is checked realtime. FR is queued (retry/backoff) and will update via polling.
            </p>

            <div className="row" style={{ marginTop: 6 }}>
              <input
                type="text"
                value={caseRef}
                onChange={(e) => setCaseRef(e.target.value)}
                placeholder="Client / Case (optioneel)"
                style={{ flex: 1, minWidth: 220 }}
              />
              <button className="btn btn-secondary" onClick={exportCsv} disabled={!rows.length}>Export CSV</button>
              <button className="btn btn-secondary" onClick={saveRun} disabled={!rows.length}>Save run</button>
            </div>

            {duplicatesIgnored > 0 && (
              <div className="callout" style={{ marginTop: 10 }}>
                <b>{duplicatesIgnored}</b> duplicaten genegeerd.
              </div>
            )}

            <textarea
              value={vatInput}
              onChange={(e) => setVatInput(e.target.value)}
              placeholder={`NL123456789B01\nDE123456789\nFR12345678901\n...`}
            />

            <div className="row">
              <button className="btn btn-primary" onClick={onValidate} disabled={loading}>
                {loading ? "Validating…" : "Validate"}
              </button>
              <button className="btn btn-secondary" onClick={onClear} disabled={loading}>
                Clear
              </button>

              <div style={{ flex: 1 }} />

              <div className="mono" style={{ fontSize: 12, color: "var(--muted)" }}>
                Progress: <b style={{ color: "var(--text)" }}>{progressText}</b> · <b style={{ color: "var(--text)" }}>{progressPct}%</b>
              </div>
            </div>

            <div className="progress" aria-hidden="true">
              <div className="bar" style={{ width: `${progressPct}%` }} />
            </div>

            <div className="stats">
              <div className="stat"><span>Total</span><b>{stats.total}</b></div>
              <div className="stat"><span>Done</span><b>{stats.done}</b></div>
              <div className="stat"><span>Valid</span><b style={{ color: "var(--ok)" }}>{stats.vOk}</b></div>
              <div className="stat"><span>Invalid</span><b style={{ color: "var(--bad)" }}>{stats.vBad}</b></div>
              <div className="stat"><span>Pending</span><b style={{ color: "var(--warn)" }}>{stats.pending}</b></div>
              <div className="stat"><span>Error</span><b style={{ color: "var(--bad)" }}>{stats.err}</b></div>
            </div>

            <div className="callout" style={{ marginTop: 14 }}>
              <b>Tip</b>: Use the filter to search within results. Click a column header to sort. Click a row to expand details.
            </div>
          </div>

          <div>
            <div className="card">
              <h2>Filter</h2>
              <div className="filterBox">
                <input
                  type="text"
                  value={filter}
                  onChange={(e) => setFilter(e.target.value)}
                  placeholder="Search in results…"
                />
                <div className="callout">
                  Sorting: <span className="mono">{sortLabel || "—"}</span>
                </div>
              </div>

              <div className="mapbox">
                <div className="mapbox-head">
                  <div className="mapbox-title">Input distribution</div>
                  <div className="mapbox-sub"><span className="nowrap">{mapCount}</span></div>
                </div>

                <div id="countryMap" />

                <div className="mapbox-foot">
                  <div id="mapLegend" title={mapLegend}>{mapLegend}</div>
                  <div className="map-attrib">
                    <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noreferrer">
                      © OpenStreetMap
                    </a>
                  </div>
                </div>
              </div>
            </div>

            <div className="card" style={{ marginTop: 16 }}>
              <h2>VIES status per land</h2>
              <p className="hint">Beschikbaarheid volgens VIES check-status.</p>
              <div style={{ overflow: "auto", maxHeight: 260 }}>
                <table>
                  <thead>
                    <tr>
                      <th style={{ width: 120 }}>Country</th>
                      <th style={{ width: 220 }}>Availability</th>
                    </tr>
                  </thead>
                  <tbody>
                    {viesStatus.map((c) => (
                      <tr key={c.countryCode}>
                        <td className="mono nowrap">{c.countryCode}</td>
                        <td>{c.availability}</td>
                      </tr>
                    ))}
                    {!viesStatus.length && (
                      <tr><td colSpan={2} style={{ padding: 12, color: "var(--muted)" }}>No data</td></tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="card" style={{ marginTop: 16 }}>
              <h2>Saved runs</h2>
              <div style={{ display:"flex", flexDirection:"column", gap: 8 }}>
                {savedRuns.slice(0, 8).map((r) => (
                  <button key={r.id} className="btn btn-secondary" onClick={() => loadRun(r.id)} style={{ textAlign:"left" }}>
                    {new Date(r.ts).toLocaleString("nl-NL")} — {r.caseRef || "—"} — {r.results?.length || 0} rows
                  </button>
                ))}
                {!savedRuns.length && <div className="hint">Nog geen runs opgeslagen.</div>}
              </div>
            </div>
          </div>
        </div>

        <div className="tableWrap">
          <div className="tableHeader">
            <strong>Results</strong>
            <div className="muted">
              Showing <b style={{ color: "var(--text)" }}>{filteredRows.length}</b> rows
            </div>
          </div>

          <div style={{ overflow: "auto", maxHeight: 520 }}>
            <table>
              <thead>
                <tr>
                  <th style={{ width: 160 }} onClick={() => sortByColumn(0, "State")}>State</th>
                  <th style={{ width: 180 }} onClick={() => sortByColumn(1, "VAT")}>VAT</th>
                  <th style={{ width: 280 }} onClick={() => sortByColumn(2, "Name")}>Name</th>
                  <th style={{ width: 280 }} onClick={() => sortByColumn(3, "Address")}>Address</th>
                  <th style={{ width: 240 }} onClick={() => sortByColumn(4, "Error")}>Error</th>
                  <th style={{ width: 240 }} onClick={() => sortByColumn(5, "Details")}>Details</th>
                </tr>
              </thead>

              <tbody>
                {filteredRows.map((r, idx) => {
                  const st = stateLabel(r.state);
                  const cls = stateClass(r.state);
                  const key = `${r.country_code || ""}:${r.vat_part || ""}` || `${r.vat_number || r.input || idx}`;
                  const isOpen = expandedKey === key;
                  const eta = r.next_retry_at ? formatEta(r.next_retry_at) : "";

                  return (
                    <React.Fragment key={`${key}-${idx}`}>
                      <tr onClick={() => setExpandedKey(isOpen ? null : key)} style={{ cursor: "pointer" }}>
                        <td>
                          <span className={`pill ${cls}`}>
                            <i aria-hidden="true" />
                            {st}{cls === "retry" && eta ? ` (ETA ${eta})` : ""}
                          </span>
                        </td>

                        <td className="mono nowrap" title={r.vat_number || r.input || ""}>
                          {r.vat_number || r.input || ""}
                        </td>

                        <td title={r.name || ""}>{r.name || ""}</td>
                        <td title={r.address || ""}>{r.address || ""}</td>

                        <td title={humanError(r.error_code, r.error) || ""}>
                          {humanError(r.error_code, r.error) || ""}
                        </td>

                        <td title={r.details || ""}>{r.details || ""}</td>
                      </tr>

                      {isOpen && (
                        <tr>
                          <td colSpan={6} className="rowDetails">
                            <div className="kv">
                              <span>Case</span><b>{r.case_ref || "—"}</b>
                              <span>Checked at</span><b>{r.checked_at ? new Date(r.checked_at).toLocaleString("nl-NL") : "—"}</b>
                              <span>Error code</span><b>{r.error_code || "—"}</b>
                              <span>Attempt</span><b>{typeof r.attempt === "number" ? String(r.attempt) : "—"}</b>
                              <span>Next retry</span><b>{r.next_retry_at ? new Date(r.next_retry_at).toLocaleString("nl-NL") : "—"}</b>
                              <span>Format</span><b>{r.format_ok === false ? `Bad (${r.format_reason})` : "OK"}</b>
                            </div>

                            <div className="row" style={{ marginTop: 10 }}>
                              <select
                                value={r.tag || ""}
                                onChange={(e) => updateNoteTag(r, (r.note || ""), e.target.value as any)}
                              >
                                <option value="">No tag</option>
                                <option value="whitelist">Whitelist</option>
                                <option value="blacklist">Blacklist</option>
                              </select>

                              <input
                                type="text"
                                value={r.note || ""}
                                onChange={(e) => updateNoteTag(r, e.target.value, (r.tag as any) || "")}
                                placeholder="Note (optioneel)"
                                style={{ flex: 1, minWidth: 260 }}
                              />
                            </div>
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })}

                {!filteredRows.length && (
                  <tr>
                    <td colSpan={6} style={{ padding: 16, color: "var(--muted)" }}>
                      No results
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </>
  );
}
