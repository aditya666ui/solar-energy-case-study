# Solar Energy Case Study (Snowflake + Azure Functions + SWA + React)

A **no-cost** demo that ingests free solar/weather signals, stores them in **Snowflake**, serves APIs from **Azure Functions (Python v2)**, and visualizes them in a **React** SPA hosted on **Azure Static Web Apps (SWA)**.

- **Web App:** https://green-forest-02e10980f.2.azurestaticapps.net  
- **Functions (base):** https://func-solar-15013.azurewebsites.net/api  
- **Repo:** https://github.com/aditya666ui/solar-energy-case-study

---

## Architecture

```mermaid
flowchart LR
  subgraph Frontend
    B[React SPA (SWA)]
  end

  subgraph Azure
    F[Azure Static Web Apps]
    A[Azure Functions\n(Python v2)]
  end

  subgraph Snowflake
    R[(RAW.SOLAR_OBS)]
    M[(MART.FORECAST_7D\nMART.SUMMARIES)]
  end

  subgraph Free_Data
    Z[Zippopotam.us\nZIP→lat/lon]
    O[Open-Meteo\nhourly radiation]
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
    T1[Timer: ingest\n(dev: minutely, prod: daily)]
    T2[Timer: insights +\n7d MA forecast]
  end

  T1 --> A
  T2 --> A
