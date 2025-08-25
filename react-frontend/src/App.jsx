import { useEffect, useState } from "react";
import {
  fetchSummaries,
  fetchGhiToday,
  fetchZips,
  fetchGhiTrend,
  fetchGhiTrendRange,
  fetchForecast,
  fetchStatus
} from "./api/client";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
  LineChart,
  Line,
  Legend
} from "recharts";
import { downloadCsv } from "./utils/exportCsv";

export default function App() {
  const [startDate, setStartDate] = useState(""); // "YYYY-MM-DD"
  const [endDate, setEndDate] = useState("");     // "YYYY-MM-DD"
  const [summaries, setSummaries] = useState([]);
  const [ghiToday, setGhiToday] = useState([]);
  const [zips, setZips] = useState([]);
  const [zip, setZip] = useState("");
  const [trendDays, setTrendDays] = useState(7);
  const [trend, setTrend] = useState([]);
  const [forecast, setForecast] = useState([]);
  const [statusData, setStatusData] = useState(null);
  const [err, setErr] = useState("");

  // initial load (summaries, today, zips)
  useEffect(() => {
    (async () => {
      try {
        const [s, g, z] = await Promise.all([
          fetchSummaries(),
          fetchGhiToday(),
          fetchZips()
        ]);
        setSummaries(s);
        setGhiToday(g);
        setZips(z);
        if (z?.length && !zip) setZip(z[0]);
      } catch (e) {
        setErr(e.message || "Failed to load data");
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // status load (on mount + manual refresh button)
  useEffect(() => {
    (async () => {
      try {
        const s = await fetchStatus();
        setStatusData(s);
      } catch (e) {
        setErr(e.message || "Failed to load status");
      }
    })();
  }, []);

  // forecast loads only in "Days" mode (not when a custom date range is active)
  useEffect(() => {
    if (!zip || startDate || endDate) {
      setForecast([]);
      return;
    }
    (async () => {
      try {
        const f = await fetchForecast(zip, trendDays);
        setForecast(f.series || []);
      } catch (e) {
        setErr(e.message || "Failed to load forecast");
      }
    })();
  }, [zip, trendDays, startDate, endDate]);

  // trend loads on zip/days change or when date range is set
  useEffect(() => {
    if (!zip) return;
    (async () => {
      try {
        let t;
        if (startDate && endDate) {
          t = await fetchGhiTrendRange(zip, startDate, endDate);
        } else {
          t = await fetchGhiTrend(zip, trendDays);
        }
        setTrend(t.series || []);
      } catch (e) {
        setErr(e.message || "Failed to load trend");
      }
    })();
  }, [zip, trendDays, startDate, endDate]);

  return (
    <div style={{ padding: 24, maxWidth: 1000, margin: "0 auto", fontFamily: "system-ui, Arial" }}>
      <h1 style={{ marginBottom: 4 }}>Solar Potential Dashboard</h1>
      <div style={{ color: "#666" }}>Live from Snowflake via Azure Functions</div>

      {err && <div style={{ marginTop: 16, color: "crimson" }}>Error: {err}</div>}

      {/* status */}
      <section style={{ marginTop: 20, padding: 12, border: "1px solid #ddd", borderRadius: 8 }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12 }}>
          <h2 style={{ margin: 0 }}>Status</h2>
          <div style={{ fontSize: 12, color: "#666" }}>
            {statusData?.server_time_utc ? `Updated: ${statusData.server_time_utc}` : "—"}
            <button
              style={{ marginLeft: 12 }}
              onClick={async () => {
                try {
                  const s = await fetchStatus();
                  setStatusData(s);
                } catch (e) {
                  setErr(e.message || "Failed to refresh status");
                }
              }}
            >
              Refresh
            </button>
          </div>
        </div>

        {!statusData ? (
          <div style={{ color: "#666", marginTop: 8 }}>Loading…</div>
        ) : (
          <>
            <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginTop: 8 }}>
              {statusData.per_zip.map((pz) => {
                const okIngest = pz.INGEST_TODAY;
                const okFc = pz.FORECAST_7D;
                const allOk = okIngest && okFc;
                const bg = allOk ? "#e6ffed" : (okIngest || okFc) ? "#fff7e6" : "#ffe6e6";
                const badge = allOk ? "OK" : (okIngest || okFc) ? "WARN" : "FAIL";
                return (
                  <div key={pz.ZIP} style={{ padding: 10, borderRadius: 6, background: bg, minWidth: 200 }}>
                    <div style={{ fontWeight: 600 }}>{pz.ZIP} — {badge}</div>
                    <div style={{ fontSize: 13, color: "#444" }}>
                      Ingest today: {okIngest ? "Yes" : "No"} • Forecast 7d: {okFc ? "Yes" : "No"}
                    </div>
                  </div>
                );
              })}
            </div>

            <details style={{ marginTop: 10 }}>
              <summary>Details</summary>
              <div style={{ marginTop: 8, fontSize: 14 }}>
                <div><strong>Summaries today:</strong> {statusData.summaries.TODAY_SUMMARIES}</div>
                <div><strong>Last summary created:</strong> {statusData.summaries.LAST_CREATED || "—"}</div>
              </div>
              <div style={{ marginTop: 8 }}>
                <strong>Ingestion</strong>
                <ul style={{ marginTop: 4 }}>
                  {statusData.ingestion.map((r, i) => (
                    <li key={i} style={{ fontSize: 14 }}>
                      {r.ZIP}: last_obs = {r.LAST_OBS || "—"}, today_rows = {r.TODAY_ROWS}
                    </li>
                  ))}
                </ul>
              </div>
              <div style={{ marginTop: 8 }}>
                <strong>Forecast table</strong>
                <ul style={{ marginTop: 4 }}>
                  {statusData.forecast.map((f, i) => (
                    <li key={i} style={{ fontSize: 14 }}>
                      {f.ZIP}: days={f.DAYS}, range={f.FIRST_DATE || "—"} → {f.LAST_DATE || "—"}
                    </li>
                  ))}
                </ul>
              </div>
            </details>
          </>
        )}
      </section>

      {/* summaries */}
      <section style={{ marginTop: 24 }}>
        <h2>Summaries (today)</h2>
        {summaries.length === 0 ? (
          <div>No summaries yet for today.</div>
        ) : (
          <>
            <ul>
              {summaries.map((s, i) => (
                <li key={i} style={{ margin: "6px 0" }}>
                  <strong>{s.ZIP}:</strong> {s.SUMMARY_TEXT}
                </li>
              ))}
            </ul>
            <div style={{ marginTop: 8 }}>
              <button
                onClick={() => {
                  const today = new Date().toISOString().slice(0, 10);
                  const headers = ["ZIP", "SUMMARY_TEXT"];
                  const rows = summaries.map(s => [s.ZIP, s.SUMMARY_TEXT]);
                  downloadCsv(`summaries_${today}.csv`, headers, rows);
                }}
              >
                Export Summaries CSV
              </button>
            </div>
          </>
        )}
      </section>

      {/* today bar */}
      <section style={{ marginTop: 32 }}>
        <h2>Today’s Avg GHI by ZIP (W/m²)</h2>
        <div style={{ width: "100%", height: 320, background: "#0b0b0b08", borderRadius: 8 }}>
          <ResponsiveContainer>
            <BarChart data={ghiToday}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="ZIP" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="GHI_MEAN" />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <div style={{ marginTop: 8 }}>
          <button
            disabled={!ghiToday?.length}
            onClick={() => {
              const today = new Date().toISOString().slice(0, 10);
              const headers = ["ZIP", "GHI_MEAN"];
              const rows = ghiToday.map(r => [r.ZIP, (r.GHI_MEAN ?? 0).toFixed(2)]);
              downloadCsv(`ghi_today_${today}.csv`, headers, rows);
            }}
          >
            Export Today CSV
          </button>
        </div>
      </section>

      {/* trend */}
      <section style={{ marginTop: 32 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
          <h2 style={{ margin: 0 }}>GHI Trend</h2>

          <label>
            ZIP:&nbsp;
            <select value={zip} onChange={e => setZip(e.target.value)}>
              {zips.map(z => (
                <option key={z} value={z}>{z}</option>
              ))}
            </select>
          </label>

          {/* Quick "Days" mode */}
          <label>
            Days:&nbsp;
            <select
              value={trendDays}
              onChange={e => {
                setTrendDays(Number(e.target.value));
                // clear range mode if switching to days
                setStartDate(""); setEndDate("");
              }}
            >
              <option value={7}>7</option>
              <option value={14}>14</option>
              <option value={30}>30</option>
            </select>
          </label>

          {/* Range mode */}
          <span style={{ marginLeft: 8, color: "#666" }}>or pick a date range:</span>
          <input type="date" value={startDate} onChange={e => setStartDate(e.target.value)} />
          <span>to</span>
          <input type="date" value={endDate} onChange={e => setEndDate(e.target.value)} />
          <button
            onClick={() => {
              if (startDate && endDate) {
                // force a refresh by bumping trendDays to itself
                setTrendDays(d => d);
              }
            }}
            disabled={!(startDate && endDate)}
          >
            Apply
          </button>
          <button
            onClick={() => {
              setStartDate(""); setEndDate("");
              setTrendDays(7);
            }}
            disabled={!(startDate || endDate)}
          >
            Clear
          </button>

          {/* Export CSV (works for either mode) */}
          <button
            disabled={!trend?.length}
            onClick={() => {
              const label = (startDate && endDate)
                ? `${startDate}_to_${endDate}`
                : `${trendDays}d`;
              const today = new Date().toISOString().slice(0, 10);
              const headers = ["OBS_DATE", "GHI_MEAN"];
              const rows = trend.map(r => [
                r.OBS_DATE,
                typeof r.GHI_MEAN === "number" ? r.GHI_MEAN.toFixed(2) : r.GHI_MEAN
              ]);
              downloadCsv(`ghi_trend_${zip}_${label}_${today}.csv`, headers, rows);
            }}
          >
            Export CSV
          </button>
        </div>

        <div style={{ width: "100%", height: 340, marginTop: 12, background: "#0b0b0b08", borderRadius: 8 }}>
          <ResponsiveContainer>
            <LineChart data={trend}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="OBS_DATE" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="GHI_MEAN" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </section>

      {/* forecast */}
      <section style={{ marginTop: 32 }}>
        <h2>Forecast (next {trendDays} days)</h2>
        {startDate || endDate ? (
          <div style={{ color: "#666" }}>
            Pick “Days” mode (clear the date range) to view forecast. Forecast is based on a 7-day moving average.
          </div>
        ) : (
          <div style={{ width: "100%", height: 300, background: "#0b0b0b08", borderRadius: 8 }}>
            <ResponsiveContainer>
              <LineChart data={forecast}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="FCST_DATE" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Line type="monotone" dataKey="GHI_PREDICTED" dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </section>
    </div>
  );
}