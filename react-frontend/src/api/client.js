// Unified API base: prod uses VITE_API_BASE, dev can proxy to /api
const API = import.meta.env.VITE_API_BASE || "/api";

function getJson(url) {
  return fetch(url).then((r) => {
    if (!r.ok) throw new Error(`${url} -> ${r.status}`);
    return r.json();
  });
}

export function fetchSummaries() {
  return getJson(`${API}/summaries`);
}

export function fetchGhiToday() {
  return getJson(`${API}/ghitoday`);
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

export function fetchForecast(zip, days = 7) {
  const q = new URLSearchParams({ zip, days: String(days) });
  return getJson(`${API}/forecast?${q.toString()}`);
}