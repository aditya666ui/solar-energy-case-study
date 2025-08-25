import { useEffect, useState } from "react";
import { fetchSummaries, fetchGhiToday } from "./api/client";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid, ResponsiveContainer
} from "recharts";

export default function App() {
  const [summaries, setSummaries] = useState([]);
  const [ghi, setGhi] = useState([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");

  useEffect(() => {
    async function load() {
      try {
        const [s, g] = await Promise.all([fetchSummaries(), fetchGhiToday()]);
        setSummaries(s);
        setGhi(g);
      } catch (e) {
        setErr(e.message || "Failed to load data");
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  return (
    <div style={{ padding: 24, maxWidth: 1000, margin: "0 auto", fontFamily: "system-ui, Arial" }}>
      <h1 style={{ marginBottom: 4 }}>Solar Potential Dashboard</h1>
      <div style={{ color: "#666" }}>Live from Snowflake via Azure Functions</div>

      {loading && <div style={{ marginTop: 16 }}>Loading…</div>}
      {err && <div style={{ marginTop: 16, color: "crimson" }}>Error: {err}</div>}

      {!loading && !err && (
        <>
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

          <section style={{ marginTop: 32 }}>
            <h2>Today’s Avg GHI by ZIP (W/m²)</h2>
            <div style={{ width: "100%", height: 340, background: "#0b0b0b08", borderRadius: 8 }}>
              <ResponsiveContainer>
                <BarChart data={ghi}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="ZIP" />
                  <YAxis />
                  <Tooltip />
                  <Bar dataKey="GHI_MEAN" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </section>
        </>
      )}
    </div>
  );
}
