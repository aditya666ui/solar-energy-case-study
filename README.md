# Solar Energy Case Study (Snowflake + Azure Functions + SWA + React)

A **no-cost** demo that ingests free solar/weather signals, stores them in **Snowflake**, serves APIs from **Azure Functions (Python v2)**, and visualizes them in a **React** SPA hosted on **Azure Static Web Apps (SWA)**.

- **Web App:** https://green-forest-02e10980f.2.azurestaticapps.net  
- **Functions (base):** https://func-solar-15013.azurewebsites.net/api  
- **Repo:** https://github.com/aditya666ui/solar-energy-case-study

## Snowflake SQL (IaC)
All Snowflake objects are versioned in [/sql](/sql):

- `001_init_warehouse_db_schemas.sql` — warehouse, DB, schemas
- `010_raw_tables.sql` — RAW tables
- `020_mart_objects.sql` — MART tables
- `030_forecast_view.sql` — optional view for 7d MA
- `031_forecast_7d_table_and_refresh.sql` — forecast table + refresh
- `040_verify_queries.sql` — validation queries
- `050_show_objects.sql` — object listings
- `090_maintenance.sql` — cleanup helpers

```markdown
## Architecture

```mermaid
flowchart LR
  subgraph Frontend
    B[React SPA (SWA)]
  end

  subgraph Azure
    F[Azure Static Web Apps]
    A[Azure Functions<br/>(Python v2)]
  end

  subgraph Snowflake
    R[(RAW.SOLAR_OBS)]
    M[(MART.FORECAST_7D<br/>MART.SUMMARIES)]
  end

  subgraph Free_Data
    Z[Zippopotam.us<br/>ZIP->lat/lon]
    O[Open-Meteo<br/>hourly radiation]
  end

  B --> F
  F --> A

  A -->|/ghitoday /summaries /ghitrend /forecast /status| B

  A -->|INSERT| R
  A -->|READ| R
  A -->|WRITE| M
  A -->|READ| M

  A -.->|ZIP geocode| Z
  A -.->|hourly data| O

  subgraph Schedules
    T1[Timer: ingest<br/>(dev: minutely, prod: daily)]
    T2[Timer: insights +<br/>7d MA forecast]
  end

  T1 --> A
  T2 --> A
