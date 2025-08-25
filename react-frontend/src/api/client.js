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
