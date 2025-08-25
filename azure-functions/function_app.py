import logging
import os
import re
import json
from datetime import date, datetime, timedelta

import requests
import snowflake.connector
import azure.functions as func

# v2 Python model
app = func.FunctionApp()

# ---- settings (from local.settings.json or Azure App Settings) ----
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "SOLAR_WH")
SNOWFLAKE_DB        = os.getenv("SNOWFLAKE_DB", "SOLAR_DB")
ZIP_LIST            = [z.strip() for z in os.getenv("ZIP_LIST", "93727,93637,95340").split(",") if z.strip()]

# ----- optional Azure OpenAI (only used if all three present) -----
AOAI_ENDPOINT    = os.getenv("AZURE_OPENAI_ENDPOINT")
AOAI_API_KEY     = os.getenv("AZURE_OPENAI_API_KEY")
AOAI_DEPLOYMENT  = os.getenv("AZURE_OPENAI_DEPLOYMENT")
USE_AOAI         = bool(AOAI_ENDPOINT and AOAI_API_KEY and AOAI_DEPLOYMENT)

# ---------- common Snowflake helper ----------
def _snowflake_conn(schema="RAW"):
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=schema,
    )

# ---------- free data fetch (no API key) ----------
def _zip_to_latlon(zip_code: str):
    r = requests.get(f"https://api.zippopotam.us/us/{zip_code.strip()}", timeout=20)
    r.raise_for_status()
    p = r.json().get("places", [])
    if not p:
        raise RuntimeError(f"No geocode for ZIP {zip_code}")
    return float(p[0]["latitude"]), float(p[0]["longitude"])

def _open_meteo_daily_means(lat: float, lon: float):
    """
    Get hourly radiation/cloud/temp and compute mean of the most recent 24 hours.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "shortwave_radiation,direct_radiation,diffuse_radiation,cloudcover,temperature_2m",
        "past_days": 1,
        "forecast_days": 1,
        "timezone": "UTC",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    h = r.json().get("hourly", {})

    def mean_last24(values):
        vals = [v for v in (values or []) if isinstance(v, (int, float))]
        if not vals:
            return 0.0
        take = min(24, len(vals))
        slice24 = vals[-take:]
        return float(sum(slice24) / len(slice24))

    return {
        "GHI":         mean_last24(h.get("shortwave_radiation")),
        "DNI":         mean_last24(h.get("direct_radiation")),
        "DHI":         mean_last24(h.get("diffuse_radiation")),
        "CLOUD_COVER": mean_last24(h.get("cloudcover")),
        "TEMP_C":      mean_last24(h.get("temperature_2m")),
    }

def fetch_daily_record(zip_code: str):
    try:
        lat, lon = _zip_to_latlon(zip_code)
        m = _open_meteo_daily_means(lat, lon)
        return {
            "OBS_DATE": date.today().isoformat(),
            "ZIP": zip_code.strip(),
            "GHI": m["GHI"],
            "DNI": m["DNI"],
            "DHI": m["DHI"],
            "CLOUD_COVER": m["CLOUD_COVER"],
            "TEMP_C": m["TEMP_C"],
            "SOURCE": "OPEN_METEO"
        }
    except Exception as e:
        logging.warning("Open-Meteo fetch failed for %s: %s -- using fallback zeros", zip_code, e)
        return {
            "OBS_DATE": date.today().isoformat(),
            "ZIP": zip_code.strip(),
            "GHI": 0.0, "DNI": 0.0, "DHI": 0.0, "CLOUD_COVER": 0.0, "TEMP_C": 20.0,
            "SOURCE": "FALLBACK"
        }

# ---------- Snowflake RAW insert ----------
def insert_rows(rows):
    if not rows:
        logging.warning("No rows to insert; skipping Snowflake write")
        return
    conn = _snowflake_conn("RAW")
    try:
        cur = conn.cursor()
        cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur.execute("USE SCHEMA RAW")
        insert_sql = """
            INSERT INTO SOLAR_OBS
            (OBS_DATE, ZIP, GHI, DNI, DHI, CLOUD_COVER, TEMP_C, SOURCE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        payload = [
            (r["OBS_DATE"], r["ZIP"], r["GHI"], r["DNI"], r["DHI"], r["CLOUD_COVER"], r["TEMP_C"], r["SOURCE"])
            for r in rows
        ]
        logging.info("Inserting %d row(s) into RAW.SOLAR_OBS ...", len(payload))
        cur.executemany(insert_sql, payload)
        conn.commit()
        logging.info("Insert committed.")
    finally:
        conn.close()

# ---------- baseline 7d-MA forecast writer into MART ----------
def upsert_forecast_7d(conn):
    cur = conn.cursor()
    cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
    cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
    cur.execute("USE SCHEMA MART")
    # delete future rows (idempotent refresh)
    cur.execute("""
        DELETE FROM SOLAR_DB.MART.FORECAST_7D
        WHERE FCST_DATE >= DATEADD(day, 1, CURRENT_DATE())
    """)
    # reinsert using last 7 days average per ZIP, for next 7 days
    cur.execute("""
        INSERT INTO SOLAR_DB.MART.FORECAST_7D (ZIP, FCST_DATE, GHI_PREDICTED, METHOD)
        WITH last7 AS (
          SELECT ZIP, AVG(GHI) AS GHI_7D_AVG
          FROM SOLAR_DB.RAW.SOLAR_OBS
          WHERE OBS_DATE >= DATEADD(day, -6, CURRENT_DATE())
          GROUP BY ZIP
        ),
        dates AS (
          SELECT DATEADD(day, seq4()+1, CURRENT_DATE()) AS FCST_DATE
          FROM TABLE(GENERATOR(ROWCOUNT => 7))
        )
        SELECT l.ZIP, d.FCST_DATE, l.GHI_7D_AVG AS GHI_PREDICTED, '7d_ma'
        FROM last7 l
        CROSS JOIN dates d
    """)

# ---------- summarization helpers ----------
def _pct_change(today_val, base_val):
    if today_val is None or base_val in (None, 0):
        return None
    try:
        return (today_val - base_val) / base_val * 100.0
    except ZeroDivisionError:
        return None

def heuristic_summary(zip_code: str, ghi_mean: float, dni_mean: float, cloud_mean: float, pct_vs_base):
    if ghi_mean is None:
        return f"{zip_code}: no data today yet."
    if pct_vs_base is None:
        return (f"{zip_code}: first day of data — mean GHI ≈ {round(ghi_mean,1)} W/m², "
                f"DNI ≈ {round(dni_mean or 0,1)} W/m², cloud ≈ {round(cloud_mean or 0,1)}%.")
    trend = "near"
    if pct_vs_base > 10: trend = "above"
    if pct_vs_base < -10: trend = "below"
    return (f"{zip_code}: today’s solar potential is {trend} the 30-day baseline "
            f"({pct_vs_base:+.1f}%). Mean GHI ≈ {round(ghi_mean or 0,1)} W/m², "
            f"DNI ≈ {round(dni_mean or 0,1)} W/m², cloud ≈ {round(cloud_mean or 0,1)}%.")

def aoai_summary_or_heuristic(zip_code, ghi_mean, dni_mean, cloud_mean, pct_vs_base):
    if not USE_AOAI:
        return heuristic_summary(zip_code, ghi_mean, dni_mean, cloud_mean, pct_vs_base)
    try:
        prompt = (
            f"One sentence (<=25 words) summarizing solar potential for US ZIP {zip_code}. "
            f"Avg GHI {round(ghi_mean or 0,1)} W/m2, DNI {round(dni_mean or 0,1)} W/m2, "
            f"cloud {round(cloud_mean or 0,1)}%. Versus baseline {pct_vs_base:+.1f}% if available."
        )
        endpoint = AOAI_ENDPOINT.rstrip("/") + f"/openai/deployments/{AOAI_DEPLOYMENT}/chat/completions?api-version=2024-08-01-preview"
        headers = {"Content-Type": "application/json", "api-key": AOAI_API_KEY}
        body = {"messages": [{"role": "user", "content": prompt}], "temperature": 0.2, "max_tokens": 60}
        r = requests.post(endpoint, headers=headers, data=json.dumps(body), timeout=15)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logging.warning("AOAI failed, using heuristic: %s", e)
        return heuristic_summary(zip_code, ghi_mean, dni_mean, cloud_mean, pct_vs_base)

# ---------- Timer: ingest (every minute locally) ----------
@app.schedule(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=True)
def fetch_solar_data(myTimer: func.TimerRequest) -> None:
    logging.info("fetch_solar_data started; ZIP_LIST=%s", ZIP_LIST)
    rows = [fetch_daily_record(z) for z in ZIP_LIST]
    insert_rows(rows)
    logging.info("fetch_solar_data finished")

# ---------- insights SQL helper ----------
def _fetch_today_vs_baseline(cur):
    sql = """
    WITH today AS (
        SELECT ZIP,
               AVG(GHI)  AS GHI_TODAY,
               AVG(DNI)  AS DNI_TODAY,
               AVG(DHI)  AS DHI_TODAY,
               AVG(CLOUD_COVER) AS CLOUD_TODAY
        FROM SOLAR_DB.RAW.SOLAR_OBS
        WHERE OBS_DATE = CURRENT_DATE()
        GROUP BY ZIP
    ),
    baseline AS (
        SELECT ZIP,
               AVG(GHI)  AS GHI_30D,
               AVG(DNI)  AS DNI_30D,
               AVG(DHI)  AS DHI_30D,
               AVG(CLOUD_COVER) AS CLOUD_30D
        FROM SOLAR_DB.RAW.SOLAR_OBS
        WHERE OBS_DATE BETWEEN DATEADD('day', -30, CURRENT_DATE()) AND DATEADD('day', -1, CURRENT_DATE())
        GROUP BY ZIP
    )
    SELECT COALESCE(t.ZIP, b.ZIP) AS ZIP,
           t.GHI_TODAY, t.DNI_TODAY, t.DHI_TODAY, t.CLOUD_TODAY,
           b.GHI_30D,  b.DNI_30D,  b.DHI_30D,  b.CLOUD_30D
    FROM today t
    FULL OUTER JOIN baseline b ON t.ZIP = b.ZIP
    ORDER BY 1;
    """
    cur.execute(sql)
    return cur.fetchall()

def _upsert_summaries(rows):
    conn = _snowflake_conn("MART")
    try:
        cur = conn.cursor()
        cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur.execute("USE SCHEMA MART")
        # delete old summaries for today
        for zip_code, _ in rows:
            cur.execute(
                "DELETE FROM SOLAR_DB.MART.SUMMARIES WHERE SUMMARY_DATE = CURRENT_DATE() AND ZIP = %s",
                (zip_code,)
            )
        insert_sql = """
            INSERT INTO SOLAR_DB.MART.SUMMARIES (SUMMARY_DATE, ZIP, SUMMARY_TEXT)
            VALUES (CURRENT_DATE(), %s, %s)
        """
        cur.executemany(insert_sql, rows)
        conn.commit()
        logging.info("Upserted %d summaries.", len(rows))
    finally:
        conn.close()

# ---------- Timer: insights (every 2 minutes locally) ----------
@app.schedule(schedule="0 */2 * * * *", arg_name="insTimer", run_on_startup=True, use_monitor=True)
def generate_insights(insTimer: func.TimerRequest) -> None:
    logging.info("generate_insights started")
    # read today & baseline
    conn = _snowflake_conn("RAW")
    try:
        cur = conn.cursor()
        cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur.execute("USE SCHEMA RAW")
        data = _fetch_today_vs_baseline(cur)
    finally:
        conn.close()

    # build per-ZIP summaries
    pairs = []
    for (zip_code, ghi_t, dni_t, _dhi_t, cloud_t, ghi_b, _dni_b, _dhi_b, _cloud_b) in data:
        pct = _pct_change(ghi_t, ghi_b)
        text = aoai_summary_or_heuristic(zip_code, ghi_t, dni_t, cloud_t, pct)
        pairs.append((zip_code, text))

    # write summaries
    _upsert_summaries(pairs)

    # refresh baseline forecast table
    conn2 = _snowflake_conn("MART")
    try:
        upsert_forecast_7d(conn2)
        logging.info("Forecast_7D refreshed")
    except Exception as e:
        logging.exception("Forecast refresh failed: %s", e)
    finally:
        conn2.close()

    logging.info("generate_insights finished")

# ---------- HTTP endpoints ----------
@app.route(route="summaries", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def summaries(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn = _snowflake_conn("MART")
        cur = conn.cursor()
        cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur.execute("USE SCHEMA MART")
        cur.execute("""
            SELECT ZIP, SUMMARY_TEXT, CREATED_AT
            FROM SOLAR_DB.MART.SUMMARIES
            WHERE SUMMARY_DATE = CURRENT_DATE()
            ORDER BY ZIP
        """)
        rows = cur.fetchall()
        data = [{"ZIP": z, "SUMMARY_TEXT": s, "CREATED_AT": str(ts)} for (z, s, ts) in rows]
        return func.HttpResponse(json.dumps(data), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.error("summaries error: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)
    finally:
        try: conn.close()
        except Exception: pass

@app.route(route="ghitoday", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ghi_today(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn = _snowflake_conn("RAW")
        cur = conn.cursor()
        cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur.execute("USE SCHEMA RAW")
        cur.execute("""
            SELECT ZIP, AVG(GHI) AS GHI_MEAN
            FROM SOLAR_DB.RAW.SOLAR_OBS
            WHERE OBS_DATE = CURRENT_DATE()
            GROUP BY ZIP
            ORDER BY ZIP
        """)
        rows = cur.fetchall()
        data = [{"ZIP": z, "GHI_MEAN": float(g or 0)} for (z, g) in rows]
        return func.HttpResponse(json.dumps(data), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.error("ghitoday error: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)
    finally:
        try: conn.close()
        except Exception: pass

@app.route(route="zips", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def list_zips(req: func.HttpRequest) -> func.HttpResponse:
    try:
        zips = sorted(set(ZIP_LIST))
        return func.HttpResponse(json.dumps(zips), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception("zips error")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)

@app.route(route="ghitrend", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ghi_trend(req: func.HttpRequest) -> func.HttpResponse:
    try:
        raw_zip = (req.params.get("zip") or "").strip()
        days_param = req.params.get("days")
        start = (req.params.get("start") or "").strip()
        end   = (req.params.get("end") or "").strip()

        allowed = ZIP_LIST
        if not raw_zip:
            raw_zip = allowed[0]
        if raw_zip not in allowed:
            return func.HttpResponse(json.dumps({"error": f"zip {raw_zip} not allowed"}), mimetype="application/json", status_code=400)

        iso = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        use_range = False
        if start and end:
            if not iso.match(start) or not iso.match(end):
                return func.HttpResponse(json.dumps({"error": "start/end must be YYYY-MM-DD"}), mimetype="application/json", status_code=400)
            d_start = datetime.strptime(start, "%Y-%m-%d").date()
            d_end   = datetime.strptime(end,   "%Y-%m-%d").date()
            if d_start > d_end:
                d_start, d_end = d_end, d_start
            if (d_end - d_start).days > 60:
                d_start = d_end - timedelta(days=60)
            start, end = d_start.isoformat(), d_end.isoformat()
            use_range = True
        else:
            try:
                days = int(days_param) if days_param is not None else 7
            except ValueError:
                days = 7
            days = max(3, min(days, 60))

        conn = _snowflake_conn("RAW")
        cur = conn.cursor()
        cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur.execute("USE SCHEMA RAW")

        if use_range:
            sql = f"""
                SELECT TO_VARCHAR(OBS_DATE) AS OBS_DATE, AVG(GHI) AS GHI_MEAN
                FROM SOLAR_DB.RAW.SOLAR_OBS
                WHERE ZIP = '{raw_zip}'
                  AND OBS_DATE BETWEEN TO_DATE('{start}') AND TO_DATE('{end}')
                GROUP BY OBS_DATE
                ORDER BY OBS_DATE
            """
        else:
            sql = f"""
                SELECT TO_VARCHAR(OBS_DATE) AS OBS_DATE, AVG(GHI) AS GHI_MEAN
                FROM SOLAR_DB.RAW.SOLAR_OBS
                WHERE ZIP = '{raw_zip}'
                  AND OBS_DATE >= DATEADD(day, -{days-1}, CURRENT_DATE())
                GROUP BY OBS_DATE
                ORDER BY OBS_DATE
            """
        cur.execute(sql)
        rows = cur.fetchall()
        data = [{"OBS_DATE": d, "GHI_MEAN": float(v or 0)} for (d, v) in rows]
        payload = {"zip": raw_zip, "series": data}
        payload.update({"start": start, "end": end} if use_range else {"days": days})
        return func.HttpResponse(json.dumps(payload), mimetype="application/json", status_code=200)

    except Exception as e:
        logging.exception("ghitrend error")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)
    finally:
        try: conn.close()
        except Exception: pass

@app.route(route="status", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def status_api(req: func.HttpRequest) -> func.HttpResponse:
    try:
        now_iso = datetime.utcnow().isoformat() + "Z"
        allowed = [z.strip() for z in os.getenv("ZIP_LIST", "").split(",") if z.strip()]

        # 1) Ingestion (RAW)
        conn_raw = _snowflake_conn("RAW")
        cur1 = conn_raw.cursor()
        cur1.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cur1.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur1.execute("USE SCHEMA RAW")
        cur1.execute("""
            SELECT ZIP,
                   MAX(OBS_DATE) AS LAST_OBS,
                   SUM(IFF(OBS_DATE = CURRENT_DATE(), 1, 0)) AS TODAY_ROWS
            FROM SOLAR_DB.RAW.SOLAR_OBS
            GROUP BY ZIP
            ORDER BY ZIP
        """)
        ing_rows = cur1.fetchall()
        ingestion = [
            {"ZIP": z, "LAST_OBS": str(d) if d is not None else None, "TODAY_ROWS": int(c or 0)}
            for (z, d, c) in ing_rows
        ]
        conn_raw.close()

        # 2) Summaries (MART)
        conn_mart = _snowflake_conn("MART")
        cur2 = conn_mart.cursor()
        cur2.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cur2.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur2.execute("USE SCHEMA MART")
        cur2.execute("""
            SELECT COUNT(*) AS TODAY_SUMMARIES,
                   MAX(CREATED_AT) AS LAST_CREATED
            FROM SOLAR_DB.MART.SUMMARIES
            WHERE SUMMARY_DATE = CURRENT_DATE()
        """)
        row = cur2.fetchone()
        summaries = {
            "TODAY_SUMMARIES": int((row[0] or 0)) if row else 0,
            "LAST_CREATED": str(row[1]) if row and row[1] is not None else None
        }

        # 3) Forecast table coverage
        cur2.execute("""
            SELECT ZIP,
                   COUNT(*) AS DAYS,
                   MIN(FCST_DATE) AS FIRST_DATE,
                   MAX(FCST_DATE) AS LAST_DATE
            FROM SOLAR_DB.MART.FORECAST_7D
            GROUP BY ZIP
            ORDER BY ZIP
        """)
        f_rows = cur2.fetchall()
        forecast = [
            {"ZIP": z, "DAYS": int(d or 0), "FIRST_DATE": str(f) if f else None, "LAST_DATE": str(l) if l else None}
            for (z, d, f, l) in f_rows
        ]
        conn_mart.close()

        # Overall quick flags (per-zip green if data today; summaries exist; 7d forecast present)
        today_str = datetime.utcnow().date().isoformat()
        per_zip = []
        for z in allowed:
            # ingestion info for this zip
            ing = next((r for r in ingestion if r["ZIP"] == z), None)
            last_obs = ing["LAST_OBS"] if ing else None
            has_today = bool(ing and ing["TODAY_ROWS"] > 0 and last_obs == today_str)
            # forecast info for this zip
            fc = next((r for r in forecast if r["ZIP"] == z), None)
            has_7d = bool(fc and fc["DAYS"] >= 7)
            per_zip.append({
                "ZIP": z,
                "INGEST_TODAY": has_today,
                "FORECAST_7D": has_7d
            })

        payload = {
            "server_time_utc": now_iso,
            "zips": allowed,
            "ingestion": ingestion,
            "summaries": summaries,
            "forecast": forecast,
            "per_zip": per_zip
        }
        return func.HttpResponse(json.dumps(payload), mimetype="application/json", status_code=200)

    except Exception as e:
        logging.exception("status_api error")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)

@app.route(route="forecast", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def forecast_api(req: func.HttpRequest) -> func.HttpResponse:
    try:
        raw_zip = (req.params.get("zip") or "").strip()
        days_param = req.params.get("days")
        try:
            days = int(days_param) if days_param is not None else 7
        except ValueError:
            days = 7
        days = max(1, min(days, 30))

        allowed = ZIP_LIST
        if not raw_zip:
            raw_zip = allowed[0]
        if raw_zip not in allowed:
            return func.HttpResponse(json.dumps({"error": f"zip {raw_zip} not allowed"}), mimetype="application/json", status_code=400)

        conn = _snowflake_conn("MART")
        cur = conn.cursor()
        cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur.execute("USE SCHEMA MART")

        if days == 7:
            sql = f"""
                SELECT TO_VARCHAR(FCST_DATE) AS FCST_DATE, GHI_PREDICTED
                FROM SOLAR_DB.MART.FORECAST_7D
                WHERE ZIP = '{raw_zip}'
                ORDER BY FCST_DATE
            """
        else:
            # on-the-fly constant 7d average for arbitrary horizon (<=30)
            sql = f"""
                WITH last7 AS (
                  SELECT AVG(GHI) AS GHI_7D_AVG
                  FROM SOLAR_DB.RAW.SOLAR_OBS
                  WHERE ZIP = '{raw_zip}'
                    AND OBS_DATE >= DATEADD(day, -6, CURRENT_DATE())
                ),
                dates AS (
                  SELECT DATEADD(day, seq4()+1, CURRENT_DATE()) AS FCST_DATE
                  FROM TABLE(GENERATOR(ROWCOUNT => {days}))
                )
                SELECT TO_VARCHAR(d.FCST_DATE) AS FCST_DATE,
                       (SELECT GHI_7D_AVG FROM last7) AS GHI_PREDICTED
                FROM dates d
                ORDER BY FCST_DATE
            """
        cur.execute(sql)
        rows = cur.fetchall()
        data = [{"FCST_DATE": d, "GHI_PREDICTED": float(v or 0)} for (d, v) in rows]
        return func.HttpResponse(json.dumps({"zip": raw_zip, "days": days, "series": data}), mimetype="application/json", status_code=200)

    except Exception as e:
        logging.exception("forecast_api error")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)
    finally:
        try: conn.close()
        except Exception: pass