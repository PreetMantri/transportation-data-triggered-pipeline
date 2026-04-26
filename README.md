# 🚌 Transportation Data Triggered Pipeline

A production-grade, end-to-end data pipeline built on **Databricks Lakeflow Declarative Pipelines (DLT)** implementing the **Medallion Architecture** (Bronze → Silver → Gold) for Indian urban transportation data.

---

## 📌 Project Overview

This pipeline ingests daily trip data from Indian cities, processes it through three quality layers, and serves city-level analytical views enriched with a calendar dimension. It demonstrates real-world data engineering patterns including streaming ingestion, CDC upserts, data quality expectations, and parameterized pipeline execution.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                         │
│   city.csv (static lookup)    trips/*.csv (daily files)     │
└────────────────┬──────────────────────────┬─────────────────┘
                 │                          │
                 ▼                          ▼
┌─────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER                            │
│                                                             │
│  transportation.bronze.city        (Materialized View)      │
│  transportation.bronze.trips       (Streaming Table)        │
│                                    ← Auto Loader            │
│                                    ← cloudFiles format      │
└────────────────────────────────┬────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│                     SILVER LAYER                            │
│                                                             │
│  transportation.silver.city        (Materialized View)      │
│  transportation.silver.trips       (Streaming Table + SCD1) │
│                                    ← CDC Auto Flow          │
│                                    ← DLT Expectations       │
│  transportation.silver.calendar    (Materialized View)      │
│                                    ← Generated date spine   │
└────────────────────────────────┬────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│                      GOLD LAYER                             │
│                                                             │
│  transportation.gold.fact_trips          (Master View)      │
│  transportation.gold.fact_trips_jaipur   (City View)        │
│  transportation.gold.fact_trips_lucknow  (City View)        │
│  transportation.gold.fact_trips_chandigarh (City View)      │
│  transportation.gold.fact_trips_coimbatore (City View)      │
│  ... (and more city views)                                  │
└─────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
transportation-data-triggered-pipeline/
│
├── Transportation_Data_Pipeline/
│   └── transformations/
│       ├── bronze/
│       │   ├── city.py              # Static CSV ingestion → bronze.city
│       │   └── trips.py             # Auto Loader streaming → bronze.trips
│       │
│       ├── silver/
│       │   ├── calendar.py          # Generated date dimension (parameterized)
│       │   ├── city.py              # Cleaned city lookup → silver.city
│       │   └── trips.py             # CDC SCD Type 1 upsert → silver.trips
│       │
│       └── gold/
│           ├── trips_gold.sql       # Master fact view (trips + city + calendar)
│           ├── trips_chandigarh.sql # City-level analytical view
│           ├── trips_coimbatore.sql # City-level analytical view
│           └── ...                  # Additional city views
│
└── data/
    └── sample/
        ├── city.csv                 # 10 Indian cities reference data
        └── trip_export_2025-08-01.csv  # Sample daily trip export
```

---

## 🔄 Pipeline Details by Layer

### 🥉 Bronze — Raw Ingestion

| Table | Type | Source | Key Features |
|---|---|---|---|
| `bronze.city` | Materialized View | `city.csv` (Volumes) | PERMISSIVE mode, corrupt record capture, metadata audit columns |
| `bronze.trips` | Streaming Table | `trips/Full Load/*.csv` | Auto Loader, schema rescue, `maxFilesPerTrigger=100` |

Both tables include:
- `file_name` — source file path for lineage tracking
- `ingest_datetime` — ingestion timestamp for auditing
- Delta CDF enabled for downstream CDC consumption

---

### 🥈 Silver — Cleansed & Validated

| Table | Type | Key Features |
|---|---|---|
| `silver.city` | Materialized View | Selects and renames columns, adds `silver_processed_timestamp` |
| `silver.trips` | Streaming Table (SCD1) | DLT expectations, CDC auto-flow, deduplication on `trip_id` |
| `silver.calendar` | Materialized View | Parameterized date spine, Indian public holidays, weekend/weekday flags |

**Data Quality Rules on `silver.trips`:**
```python
@dp.expect("valid_date",            "year(business_date) >= 2020")
@dp.expect("valid_driver_rating",   "driver_rating BETWEEN 1 AND 10")
@dp.expect("valid_passenger_rating","passenger_rating BETWEEN 1 AND 10")
```

**Calendar Dimension includes:** `date_key`, `year`, `month`, `quarter`, `day_of_week`, `month_name`, `week_of_year`, `is_weekend`, `is_weekday`, `is_holiday`, `holiday_name` (Republic Day, Independence Day, Gandhi Jayanti)

**CDC Flow (silver.trips):**
- Keyed on `trip_id`
- SCD Type 1 — overwrites on change
- Sequenced by `silver_processed_timestamp`

---

### 🥇 Gold — Analytical Consumption Layer

**`fact_trips`** — Master view joining all three silver tables:
```sql
trips (silver)
  INNER JOIN city     ON city_id
  INNER JOIN calendar ON business_date
```

Exposes: `trip_id`, `business_date`, `city_id`, `city_name`, `passenger_category`, `distance_kms`, `sales_amt`, `passenger_rating`, `driver_rating`, `month`, `quarter`, `is_weekend`, `national_holiday`

**City-level views** filter `fact_trips` by `city_id` for city-specific reporting:

| View | City | Filter |
|---|---|---|
| `fact_trips_chandigarh` | Chandigarh | `city_id = 'CH01'` |
| `fact_trips_coimbatore` | Coimbatore | `city_id = 'TN01'` |
| ... | ... | ... |

---

## 🗺️ Cities Covered

| City ID | City | State |
|---|---|---|
| RJ01 | Jaipur | Rajasthan |
| UP01 | Lucknow | Uttar Pradesh |
| GJ01 | Surat | Gujarat |
| KL01 | Kochi | Kerala |
| MP01 | Indore | Madhya Pradesh |
| CH01 | Chandigarh | Punjab/Haryana |
| GJ02 | Vadodara | Gujarat |
| AP01 | Visakhapatnam | Andhra Pradesh |
| TN01 | Coimbatore | Tamil Nadu |
| KA01 | Mysore | Karnataka |

---

## 📊 Sample Data Schema

**city.csv**
```
city_id, city_name
RJ01, Jaipur
CH01, Chandigarh
...
```

**trip_export_YYYY-MM-DD.csv**
```
trip_id, date, city_id, passenger_type, distance_travelled(km), fare_amount, passenger_rating, driver_rating
TRPJAI25080119c10349, 2025-08-01, RJ01, new, 25, 337, 8, 10
```

> Note: `distance_travelled(km)` is renamed to `distance_travelled_km` in the bronze layer to ensure column name compatibility.

---

## ⚙️ How to Run

### Prerequisites
- Databricks workspace with **Unity Catalog** enabled
- A catalog named `transportation` with schemas `bronze`, `silver`, `gold`
- Databricks Volumes configured at `/Volumes/transportation/bronze/data-store/`

### 1. Upload Source Data
```
/Volumes/transportation/bronze/data-store/city/city.csv
/Volumes/transportation/bronze/data-store/trips/Full Load/<daily_files>.csv
```

### 2. Set Pipeline Parameters
In the DLT pipeline settings, configure:
```json
{
  "start_date": "2025-01-01",
  "end_date":   "2025-12-31"
}
```
These drive the calendar dimension date range.

### 3. Create and Run the DLT Pipeline
- In Databricks, go to **Workflows → Delta Live Tables → Create Pipeline**
- Add all `.py` and `.sql` files from `transformations/` as source files
- Set **Target schema** to `transportation`
- Click **Start**

### 4. Query the Gold Layer
```sql
-- All trips enriched with city and calendar
SELECT * FROM transportation.gold.fact_trips LIMIT 100;

-- City-specific analysis
SELECT * FROM transportation.gold.fact_trips_chandigarh
WHERE is_weekend = true;

-- Revenue by city and month
SELECT city_name, month_name, SUM(sales_amt) as total_revenue
FROM transportation.gold.fact_trips
GROUP BY city_name, month_name
ORDER BY total_revenue DESC;
```

---

## 🛠️ Tech Stack

| Technology | Usage |
|---|---|
| **Databricks DLT** | Pipeline orchestration and execution |
| **Lakeflow Declarative Pipelines** | `@dp.materialized_view`, `@dp.table`, `@dp.expect` |
| **Auto Loader** | Incremental file ingestion from Volumes |
| **Delta Lake** | Storage format with CDF, auto-optimize, auto-compact |
| **PySpark** | Bronze and Silver transformations |
| **Spark SQL** | Gold layer views |
| **Unity Catalog** | Three-level namespace (`catalog.schema.table`) |
| **CDC / SCD Type 1** | `create_auto_cdc_flow` for trips deduplication |

---

## 🔑 Key Design Decisions

**Why Auto Loader for trips?** Daily trip files land incrementally — Auto Loader efficiently processes only new files without rescanning the entire directory, making it suitable for production-scale ingestion.

**Why CDC + SCD Type 1 for silver trips?** Trip records can arrive late or be corrected. SCD Type 1 ensures the latest version of each trip is always current without accumulating duplicates.

**Why a generated calendar dimension?** Joining trips to a pre-built date spine enables time-intelligence queries (weekends, holidays, quarters) without complex date logic in every downstream query.

**Why city-level gold views?** Downstream consumers (BI tools, city operations teams) typically query one city at a time. Pre-filtered views reduce query complexity and improve performance for end users.

---

## 👤 Author

**Preet Mantri**  
Data Engineer | Microsoft Certified Fabric Data Engineer Associate | Databricks Certified Data Engineer Associate

---
