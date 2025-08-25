import logging
import os
from datetime import date
import requests
import snowflake.connector
import azure.functions as func

# v2 Python model: you MUST define this
app = func.FunctionApp()

# ---- settings (from local.settings.json or Azure App Settings) ----
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "SOLAR_WH")
SNOWFLAKE_DB        = os.getenv("SNOWFLAKE_DB", "SOLAR_DB")
ZIP_LIST            = os.getenv("ZIP_LIST", "93727,93637,95340").split(",")

# ---------- data fetch helpers (free, no API key) ----------
def _zip_to_latlon(zip_code: str):
    r = requests.get(f"https://api.zippopotam.us/us/{zip_code.strip()}", timeout=20)
    r.raise_for_status()
    p = r.json().get("places", [])
    if not p:
        raise RuntimeError(f"No geocode for ZIP {zip_code}")
    return float(p[0]["latitude"]), float(p[0]["longitude"])

def _open_meteo_daily_means(lat: float, lon: float):
    """
    Get hourly radiation, cloud, temp and compute the mean of the most-recent 24 hours.
    This avoids any local/UTC date mismatch.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "shortwave_radiation,direct_radiation,diffuse_radiation,cloudcover,temperature_2m",
        "past_days": 1,       # include yesterday so we always have a full 24h window
        "forecast_days": 1,   # and some of today
        "timezone": "UTC",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    h = r.json().get("hourly", {})

    def mean_last24(values):
        vals = [v for v in (values or []) if isinstance(v, (int, float))]
        if not vals:
            return 0
        # take the most recent 24 hours (or all if fewer than 24)
        take = min(24, len(vals))
        slice24 = vals[-take:]
        return sum(slice24) / len(slice24)

    return {
        "GHI":         mean_last24(h.get("shortwave_radiation")),
        "DNI":         mean_last24(h.get("direct_radiation")),
        "DHI":         mean_last24(h.get("diffuse_radiation")),
        "CLOUD_COVER": mean_last24(h.get("cloudcover")),
        "TEMP_C":      mean_last24(h.get("temperature_2m")),
    }


    def mean_for_today(values):
        vals = [v for t, v in zip(times, values or []) if isinstance(v, (int, float)) and t.startswith(today)]
        return (sum(vals) / len(vals)) if vals else 0

    return {
        "GHI":  mean_for_today(h.get("shortwave_radiation")),
        "DNI":  mean_for_today(h.get("direct_radiation")),
        "DHI":  mean_for_today(h.get("diffuse_radiation")),
        "CLOUD_COVER": mean_for_today(h.get("cloudcover")),
        "TEMP_C": mean_for_today(h.get("temperature_2m")),
    }

def fetch_daily_record(zip_code: str):
    try:
        lat, lon = _zip_to_latlon(zip_code)
        m = _open_meteo_daily_means(lat, lon)
        rec = {
            "OBS_DATE": date.today().isoformat(),
            "ZIP": zip_code.strip(),
            "GHI": m["GHI"],
            "DNI": m["DNI"],
            "DHI": m["DHI"],
            "CLOUD_COVER": m["CLOUD_COVER"],
            "TEMP_C": m["TEMP_C"],
            "SOURCE": "OPEN_METEO"
        }
        logging.info("record %s -> %s", zip_code, rec)
        return rec
    except Exception as e:
        logging.warning("Open-Meteo fetch failed for %s: %s -- using fallback zeros", zip_code, e)
        return {
            "OBS_DATE": date.today().isoformat(),
            "ZIP": zip_code.strip(),
            "GHI": 0, "DNI": 0, "DHI": 0, "CLOUD_COVER": 0, "TEMP_C": 20,
            "SOURCE": "FALLBACK"
        }

# ---------- Snowflake insert ----------
def insert_rows(rows):
    if not rows:
        logging.warning("No rows to insert; skipping Snowflake write")
        return
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema="RAW",
    )
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

# ---------- Timer function (runs every minute locally) ----------
@app.schedule(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=True)
def fetch_solar_data(myTimer: func.TimerRequest) -> None:
    logging.info("fetch_solar_data started; ZIP_LIST=%s", ZIP_LIST)
    rows = [fetch_daily_record(z) for z in ZIP_LIST]
    logging.info("built %d row(s)", len(rows))
    insert_rows(rows)
    logging.info("fetch_solar_data finished")



# ---------- helpers for insights ----------

def _fetch_today_vs_baseline(cur):
    """
    Returns tuples per ZIP:
    (ZIP, GHI_TODAY, DNI_TODAY, DHI_TODAY, CLOUD_TODAY, GHI_30D, DNI_30D, DHI_30D, CLOUD_30D)
    Baseline is last 30 days excluding today (handles 'no history' gracefully).
    """
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

def _pct_change(today_val, base_val):
    if today_val is None or base_val in (None, 0):
        return None
    try:
        return (today_val - base_val) / base_val * 100.0
    except ZeroDivisionError:
        return None

def _build_summary(zip_code, ghi_today, dni_today, cloud_today, ghi_30d):
    # pick a friendly trend word based on % vs baseline
    pct = _pct_change(ghi_today, ghi_30d)
    if pct is None and ghi_today is None:
        return f"{zip_code}: no data today yet."
    if pct is None and ghi_today is not None and (ghi_30d is None):
        return (f"{zip_code}: first day of data — mean GHI ≈ {round(ghi_today,1)} W/m², "
                f"DNI ≈ {round(dni_today or 0,1)} W/m², cloud ≈ {round(cloud_today or 0,1)}%.")
    # choose label
    trend = "near"
    if pct > 10: trend = "above"
    if pct < -10: trend = "below"
    return (f"{zip_code}: today’s solar potential is {trend} the 30-day baseline "
            f"({pct:+.1f}%). Mean GHI ≈ {round(ghi_today or 0,1)} W/m², "
            f"DNI ≈ {round(dni_today or 0,1)} W/m², cloud ≈ {round(cloud_today or 0,1)}%.")

def _upsert_summaries(rows):
    """
    rows: iterable of (ZIP, summary_text)
    Deletes any existing summary for today/ZIP then inserts a fresh one.
    """
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema="MART",
    )
    try:
        cur = conn.cursor()
        cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur.execute("USE SCHEMA MART")
        # delete old summaries for today (idempotent)
        for zip_code, _ in rows:
            cur.execute(
                "DELETE FROM SOLAR_DB.MART.SUMMARIES WHERE SUMMARY_DATE = CURRENT_DATE() AND ZIP = %s",
                (zip_code,)
            )
        # insert new ones
        insert_sql = """
            INSERT INTO SOLAR_DB.MART.SUMMARIES (SUMMARY_DATE, ZIP, SUMMARY_TEXT)
            VALUES (CURRENT_DATE(), %s, %s)
        """
        cur.executemany(insert_sql, rows)
        conn.commit()
        logging.info("Upserted %d summaries.", len(rows))
    finally:
        conn.close()

# ---------- daily insights timer ----------
# For LOCAL TESTING you can use every 2 minutes: "0 */2 * * * *" and run_on_startup=True
@app.schedule(schedule="0 */2 * * * *", arg_name="insTimer", run_on_startup=True, use_monitor=True)
def generate_insights(insTimer: func.TimerRequest) -> None:
    logging.info("generate_insights started")
    # read aggregates
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema="RAW",  # we query RAW for today/baseline aggregates
    )
    try:
        cur = conn.cursor()
        cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur.execute("USE SCHEMA RAW")
        data = _fetch_today_vs_baseline(cur)
    finally:
        conn.close()

    # build summaries
    pairs = []
    for (zip_code, ghi_t, dni_t, dhi_t, cloud_t, ghi_b, dni_b, dhi_b, cloud_b) in data:
        text = _build_summary(zip_code, ghi_t, dni_t, cloud_t, ghi_b)
        pairs.append((zip_code, text))

    # write summaries
    _upsert_summaries(pairs)
    logging.info("generate_insights finished")



# ---------- HTTP endpoints for the frontend ----------
import json

def _snowflake_conn(schema="MART"):
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=schema,
    )

# /api/summaries  -> today's summaries
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
        try:
            conn.close()
        except Exception:
            pass

# /api/ghitoday -> avg GHI by ZIP for today (simple chart data)
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
        try:
            conn.close()
        except Exception:
            pass

# ---------- Trend endpoints ----------
import os
import json

@app.route(route="zips", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def list_zips(req: func.HttpRequest) -> func.HttpResponse:
    try:
        raw = os.getenv("ZIP_LIST", "")
        zips = [z.strip() for z in raw.split(",") if z.strip()]
        zips = sorted(set(zips))
        return func.HttpResponse(json.dumps(zips), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception("zips error")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)

@app.route(route="ghitrend", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ghi_trend(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # inputs
        raw_zip = (req.params.get("zip") or "").strip()
        days = req.params.get("days")
        try:
            days = int(days) if days is not None else 7
        except ValueError:
            days = 7
        # clamp days to a safe range
        days = max(3, min(days, 60))

        # allowed zips = from env
        raw_list = os.getenv("ZIP_LIST", "")
        allowed = [z.strip() for z in raw_list.split(",") if z.strip()]
        if not allowed:
            return func.HttpResponse(json.dumps({"error": "ZIP_LIST not configured"}), mimetype="application/json", status_code=500)

        # default to first env ZIP if none provided
        if not raw_zip:
            raw_zip = allowed[0]

        if raw_zip not in allowed:
            return func.HttpResponse(json.dumps({"error": f"zip {raw_zip} not allowed"}), mimetype="application/json", status_code=400)

        # query Snowflake
        conn = _snowflake_conn("RAW")
        cur = conn.cursor()
        cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur.execute("USE SCHEMA RAW")

        # last N days including today
        sql = f"""
            SELECT
                TO_VARCHAR(OBS_DATE) AS OBS_DATE,
                AVG(GHI) AS GHI_MEAN
            FROM SOLAR_DB.RAW.SOLAR_OBS
            WHERE ZIP = '{raw_zip}'
              AND OBS_DATE >= DATEADD(day, -{days-1}, CURRENT_DATE())
            GROUP BY OBS_DATE
            ORDER BY OBS_DATE
        """
        cur.execute(sql)
        rows = cur.fetchall()
        data = [{"OBS_DATE": d, "GHI_MEAN": float(v or 0)} for (d, v) in rows]

        return func.HttpResponse(json.dumps({"zip": raw_zip, "days": days, "series": data}), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception("ghitrend error")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)
    finally:
        try:
            conn.close()
        except Exception:
            pass