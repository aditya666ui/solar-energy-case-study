import { useEffect, useState } from "react";
import { fetchSummaries, fetchGhiToday, fetchZips, fetchGhiTrend } from "./api/client";
import { BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid, ResponsiveContainer, LineChart, Line, Legend } from "recharts";

export default function App() {
  const [summaries, setSummaries] = useState([]);
  const [ghiToday, setGhiToday] = useState([]);
  const [zips, setZips] = useState([]);
  const [zip, setZip] = useState("");
  const [trendDays, setTrendDays] = useState(7);
  const [trend, setTrend] = useState([]);
  const [err, setErr] = useState("");

  // initial load
  useEffect(() => {
    (async () => {
      try {
        const [s, g, z] = await Promise.all([fetchSummaries(), fetchGhiToday(), fetchZips()]);
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

  // load trend whenever zip or days change
  useEffect(() => {
    if (!zip) return;
    (async () => {
      try {
        const t = await fetchGhiTrend(zip, trendDays);
        setTrend(t.series || []);
      } catch (e) {
        setErr(e.message || "Failed to load trend");
      }
    })();
  }, [zip, trendDays]);

  return (
    <div style={{ padding: 24, maxWidth: 1000, margin: "0 auto", fontFamily: "system-ui, Arial" }}>
      <h1 style={{ marginBottom: 4 }}>Solar Potential Dashboard</h1>
      <div style={{ color: "#666" }}>Live from Snowflake via Azure Functions</div>

      {err && <div style={{ marginTop: 16, color: "crimson" }}>Error: {err}</div>}

      {/* summaries */}
      <section style={{ marginTop: 24 }}>
        <h2>Summaries (today)</h2>
        {summaries.length === 0 ? (
          <div>No summaries yet for today.</div>
        ) : (
          <ul>
            {summaries.map((s, i) => (
              <li key={i} style={{ margin: "6px 0" }}>
                <strong>{s.ZIP}:</strong> {s.SUMMARY_TEXT}
              </li>
            ))}
          </ul>
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
      </section>

      {/* trend */}
      <section style={{ marginTop: 32 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <h2 style={{ margin: 0 }}>GHI Trend</h2>
          <label>
            ZIP:&nbsp;
            <select value={zip} onChange={e => setZip(e.target.value)}>
              {zips.map(z => (
                <option key={z} value={z}>{z}</option>
              ))}
            </select>
          </label>
          <label>
            Days:&nbsp;
            <select value={trendDays} onChange={e => setTrendDays(Number(e.target.value))}>
              <option value={7}>7</option>
              <option value={14}>14</option>
              <option value={30}>30</option>
            </select>
          </label>
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
    </div>
  );
}