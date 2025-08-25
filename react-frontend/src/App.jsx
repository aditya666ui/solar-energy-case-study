import "./styles.css";
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
  BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid,
  ResponsiveContainer, LineChart, Line, Legend
} from "recharts";
import { downloadCsv } from "./utils/exportCsv";

export default function App() {
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [summaries, setSummaries] = useState([]);
  const [ghiToday, setGhiToday] = useState([]);
  const [zips, setZips] = useState([]);
  const [zip, setZip] = useState("");
  const [trendDays, setTrendDays] = useState(7);
  const [trend, setTrend] = useState([]);
  const [forecast, setForecast] = useState([]);
  const [statusData, setStatusData] = useState(null);
  const [err, setErr] = useState("");

  // initial load
  useEffect(() => {
    (async () => {
      try {
        const [s, g, z] = await Promise.all([fetchSummaries(), fetchGhiToday(), fetchZips()]);
        setSummaries(s); setGhiToday(g); setZips(z);
        if (z?.length && !zip) setZip(z[0]);
      } catch (e) { setErr(e.message || "Failed to load data"); }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // status once
  useEffect(() => {
    (async () => {
      try { setStatusData(await fetchStatus()); }
      catch (e) { setErr(e.message || "Failed to load status"); }
    })();
  }, []);

  // forecast only in Days mode
  useEffect(() => {
    if (!zip || startDate || endDate) { setForecast([]); return; }
    (async () => {
      try { const f = await fetchForecast(zip, trendDays); setForecast(f.series || []); }
      catch (e) { setErr(e.message || "Failed to load forecast"); }
    })();
  }, [zip, trendDays, startDate, endDate]);

  // trend
  useEffect(() => {
    if (!zip) return;
    (async () => {
      try {
        const t = (startDate && endDate)
          ? await fetchGhiTrendRange(zip, startDate, endDate)
          : await fetchGhiTrend(zip, trendDays);
        setTrend(t.series || []);
      } catch (e) { setErr(e.message || "Failed to load trend"); }
    })();
  }, [zip, trendDays, startDate, endDate]);

  return (
    <div className="container">
      <header className="header">
        <div className="h1">Solar Potential Dashboard</div>
        <div className="sub">Live from Snowflake via Azure Functions</div>
      </header>

      {err && <div className="card" style={{ borderColor:"rgba(239,68,68,.5)" }}>
        <strong style={{ color:"var(--danger)" }}>Error:</strong> {err}
      </div>}

      {/* STATUS */}
      <section className="card">
        <div className="toolbar" style={{ justifyContent:"space-between" }}>
          <h2 style={{ margin:0 }}>Status</h2>
          <div style={{ fontSize:12, color:"var(--muted)" }}>
            {statusData?.server_time_utc ? `Updated: ${statusData.server_time_utc}` : "—"}
            <button className="btn" style={{ marginLeft:12 }}
              onClick={async () => {
                try { setStatusData(await fetchStatus()); }
                catch(e){ setErr(e.message || "Failed to refresh status"); }
              }}>Refresh</button>
          </div>
        </div>

        {!statusData ? (
          <div style={{ color:"var(--muted)", marginTop:8 }}>Loading…</div>
        ) : (
          <>
            <div className="toolbar" style={{ gap:16, marginTop:8, flexWrap:"wrap" }}>
              {statusData.per_zip.map((pz) => {
                const okIngest = pz.INGEST_TODAY;
                const okFc = pz.FORECAST_7D;
                const allOk = okIngest && okFc;
                const cls = allOk ? "statusChip status-ok"
                          : (okIngest || okFc) ? "statusChip status-warn"
                          : "statusChip status-fail";
                const badge = allOk ? "OK" : (okIngest || okFc) ? "WARN" : "FAIL";
                return (
                  <div key={pz.ZIP} className={cls}>
                    <div style={{ fontWeight:600 }}>{pz.ZIP} — {badge}</div>
                    <div style={{ fontSize:13, color:"var(--text)" }}>
                      Ingest today: {okIngest ? "Yes" : "No"} • Forecast 7d: {okFc ? "Yes" : "No"}
                    </div>
                  </div>
                );
              })}
            </div>

            <details style={{ marginTop:10 }}>
              <summary>Details</summary>
              <div style={{ marginTop:8, fontSize:14 }}>
                <div><strong>Summaries today:</strong> {statusData.summaries.TODAY_SUMMARIES}</div>
                <div><strong>Last summary created:</strong> {statusData.summaries.LAST_CREATED || "—"}</div>
              </div>
              <div style={{ marginTop:8 }}>
                <strong>Ingestion</strong>
                <ul style={{ marginTop:4 }}>
                  {statusData.ingestion.map((r,i)=>(
                    <li key={i} style={{ fontSize:14 }}>
                      {r.ZIP}: last_obs = {r.LAST_OBS || "—"}, today_rows = {r.TODAY_ROWS}
                    </li>
                  ))}
                </ul>
              </div>
              <div style={{ marginTop:8 }}>
                <strong>Forecast table</strong>
                <ul style={{ marginTop:4 }}>
                  {statusData.forecast.map((f,i)=>(
                    <li key={i} style={{ fontSize:14 }}>
                      {f.ZIP}: days={f.DAYS}, range={f.FIRST_DATE || "—"} → {f.LAST_DATE || "—"}
                    </li>
                  ))}
                </ul>
              </div>
            </details>
          </>
        )}
      </section>

      {/* SUMMARIES */}
      <section className="card">
        <div className="toolbar" style={{ justifyContent:"space-between" }}>
          <h2 style={{ margin:0 }}>Summaries (today)</h2>
          <button className="btn"
            disabled={!summaries?.length}
            onClick={()=>{
              const today = new Date().toISOString().slice(0,10);
              downloadCsv(`summaries_${today}.csv`,
                ["ZIP","SUMMARY_TEXT"],
                summaries.map(s=>[s.ZIP,s.SUMMARY_TEXT]));
            }}>Export CSV</button>
        </div>

        {summaries.length === 0 ? (
          <div style={{ color:"var(--muted)" }}>No summaries yet for today.</div>
        ) : (
          <ul style={{ marginTop:10 }}>
            {summaries.map((s,i)=>(
              <li key={i} style={{ margin:"6px 0" }}>
                <strong>{s.ZIP}:</strong> {s.SUMMARY_TEXT}
              </li>
            ))}
          </ul>
        )}
      </section>

      {/* TODAY BAR */}
      <section className="card">
        <div className="toolbar" style={{ justifyContent:"space-between" }}>
          <h2 style={{ margin:0 }}>Today’s Avg GHI by ZIP (W/m²)</h2>
          <button className="btn"
            disabled={!ghiToday?.length}
            onClick={()=>{
              const today = new Date().toISOString().slice(0,10);
              downloadCsv(`ghi_today_${today}.csv`,
                ["ZIP","GHI_MEAN"],
                ghiToday.map(r=>[r.ZIP,(r.GHI_MEAN??0).toFixed(2)]));
            }}>Export CSV</button>
        </div>

        <div className="chart">
          <ResponsiveContainer>
            <BarChart data={ghiToday}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="ZIP" stroke="var(--muted)"/>
              <YAxis stroke="var(--muted)"/>
              <Tooltip />
              <Bar dataKey="GHI_MEAN" fill="var(--accent)" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </section>

      {/* TREND */}
      <section className="card">
        <div className="toolbar">
          <h2 style={{ margin:0 }}>GHI Trend</h2>

          <label>ZIP:&nbsp;
            <select value={zip} onChange={e=>setZip(e.target.value)}>
              {zips.map(z=>(<option key={z} value={z}>{z}</option>))}
            </select>
          </label>

          <label>Days:&nbsp;
            <select value={trendDays}
              onChange={e=>{ setTrendDays(Number(e.target.value)); setStartDate(""); setEndDate(""); }}>
              <option value={7}>7</option>
              <option value={14}>14</option>
              <option value={30}>30</option>
            </select>
          </label>

          <span style={{ color:"var(--muted)" }}>or pick a date range:</span>
          <input type="date" value={startDate} onChange={e=>setStartDate(e.target.value)} />
          <span>to</span>
          <input type="date" value={endDate} onChange={e=>setEndDate(e.target.value)} />

          <button className="btn"
            onClick={()=>{ if (startDate && endDate) setTrendDays(d=>d); }}
            disabled={!(startDate && endDate)}>Apply</button>
          <button className="btn"
            onClick={()=>{ setStartDate(""); setEndDate(""); setTrendDays(7); }}
            disabled={!(startDate || endDate)}>Clear</button>

          <button className="btn"
            disabled={!trend?.length}
            onClick={()=>{
              const label = (startDate && endDate) ? `${startDate}_to_${endDate}` : `${trendDays}d`;
              const today = new Date().toISOString().slice(0,10);
              downloadCsv(`ghi_trend_${zip}_${label}_${today}.csv`,
                ["OBS_DATE","GHI_MEAN"],
                trend.map(r=>[r.OBS_DATE, typeof r.GHI_MEAN==="number"? r.GHI_MEAN.toFixed(2): r.GHI_MEAN]));
            }}>Export CSV</button>
        </div>

        <div className="chart">
          <ResponsiveContainer>
            <LineChart data={trend}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="OBS_DATE" stroke="var(--muted)"/>
              <YAxis stroke="var(--muted)"/>
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="GHI_MEAN" stroke="var(--primary)" strokeWidth={2} dot={false}/>
            </LineChart>
          </ResponsiveContainer>
        </div>
      </section>

      {/* FORECAST */}
      <section className="card">
        <h2 style={{ marginTop:0 }}>Forecast (next {trendDays} days)</h2>
        {startDate || endDate ? (
          <div style={{ color:"var(--muted)" }}>
            Pick “Days” mode (clear the date range) to view forecast. Forecast uses a 7-day moving average.
          </div>
        ) : (
          <div className="chart" style={{ height:300 }}>
            <ResponsiveContainer>
              <LineChart data={forecast}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="FCST_DATE" stroke="var(--muted)"/>
                <YAxis stroke="var(--muted)"/>
                <Tooltip />
                <Legend />
                <Line type="monotone" dataKey="GHI_PREDICTED" stroke="var(--accent)" strokeWidth={2} dot={false}/>
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </section>
    </div>
  );
}