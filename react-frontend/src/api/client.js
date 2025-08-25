const API = import.meta.env.VITE_API_BASE || "/api";

async function get(path) {
  const res = await fetch(`${API}${path}`);
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`GET ${path} failed: ${res.status} ${text}`);
  }
  return res.json();
}

export function fetchSummaries() {
  return get("/summaries");
}

export function fetchGhiToday() {
  return get("/ghitoday");
}

async function getJson(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url} -> ${r.status}`);
  return r.json();
}

export function fetchZips() {
  return getJson(`${API}/zips`);
}

export function fetchGhiTrend(zip, days = 7) {
  const q = new URLSearchParams({ zip, days: String(days) });
  return getJson(`${API}/ghitrend?${q.toString()}`);
}
export function fetchGhiTrendRange(zip, start, end) {
  const q = new URLSearchParams({ zip, start, end });
  return getJson(`${API}/ghitrend?${q.toString()}`);
}