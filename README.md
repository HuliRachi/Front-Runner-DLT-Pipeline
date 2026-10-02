# Front Runner SA Company - DLT Pipeline on Databricks

A robust, enterprise-grade data engineering project that builds an end-to-end Medallion Architecture (Bronze, Silver, Gold layers) on Databricks using Delta Live Tables (DLT) and Databricks Asset Bundles (DAB). This pipeline ingests, cleans, processes, and serves critical customer behavior and product analysis data generated from the synthetic Front Runner South Africa e-commerce data ecosystem.

## Project Description

Front Runner SA requires a scalable, automated solution to track customer registration events and telemetry web clicks. This project orchestrates the continuous ingestion of customer Change Data Capture (CDC) logs from JSON sources and product catalogs from CSV files into high-performance Delta tables. By utilizing custom quality expectations, records containing processing anomalies or missing validation parameters are programmatically isolated into Quarantine destinations without interrupting downstream transformations. 

The processed datasets are aggregated into optimized Gold reporting layers to solve core analytical queries regarding business growth metrics, conversion behaviors, and marketing funnel dynamics.

## Data Infrastructure Architecture

The pipeline processes data through three distinct architectural layers to refine raw telemetry hits into production-ready analytical dashboards:

```text
[ Landing Zone Volumes ]
       │
       ▼
 ┌───────────┐
 │  BRONZE   │  --> Raw Append-Only Ingestion Streams
 └─────┬─────┘
       │
       ▼  (Data Quality Rules & Schema Enforcement)
 ┌───────────┐
 │  SILVER   │  --> SCD Type 1 Catalog Views & SCD Type 2 Customer History Logs
 └─────┬─────┘
       │
       ▼  (Aggregations & Business Metrics Computation)
 ┌───────────┐
 │   GOLD    │  --> Customer 360 Profiles & Product Conversion Performance
 └───────────┘
```


1. **Bronze Layer:** Acts as the raw landing repository. Data is loaded continuously via Spark Structured Streaming from cloud storage paths, preserving original schemas along with processing metadata fields (`_ingested_at`, `_source_file`).
2. **Silver Layer:** Enforces data quality rules via Delta Live Tables expectations (`expect_all`). Cleansed datasets are stored in conformed structural layouts. Changes to the core product catalog are upserted dynamically using Slowly Changing Dimensions (SCD) Type 1 logic, while history alterations to customer tiers are versioned using SCD Type 2 logic.
3. **Gold Layer:** Materialized views designed for consumption. Computes complex transformations such as customer purchase frequency, recency, product views, and category-level funnel progression tracking.

## Technical Stack Used

* **Orchestration & Compute Runtime:** Databricks Asset Bundles (DAB), Delta Live Tables (DLT)
* **Data Processing Processing Engines:** PySpark (Spark Structured Streaming, Spark SQL)
* **Storage Standard Framework:** Delta Lake (Parquet engine under Unity Catalog)
* **Automation Workflow Framework:** GitHub Actions CI/CD Pipeline
* **Testing Execution Infrastructure:** Pytest, Databricks Python SDK (v0.67.0+)

## Project Structure

```text
Front-Runner-DLT-Pipeline/
├── .github/
│   └── workflows/
├── bundle/
│   └── resources/
├── jobs/
├── monitoring/
├── pipelines/
│   ├── ingestion_pipeline/
│   │   ├── tests/
│   │   ├── transformations/
│   │   └── utilities/
│   └── transformation_pipeline/
│       ├── tests/
│       ├── transformations/
│       └── utilities/
├── setup/
└── tests/
    ├── integration/
    └── smoke/
     
```

## Entity Relationship (ER) Diagram

The logical relationship models built between conformed Silver layers inside Unity Catalog are structured as follows:

```text
  ┌────────────────────────┐               ┌────────────────────────┐
  │    SILVER_CUSTOMERS    │               │    SILVER_PRODUCTS     │
  ├────────────────────────┤               ├────────────────────────┤
  │ customer_id (PK) STRING│               │ product_id (PK)  STRING│
  │ first_name       STRING│               │ sku              STRING│
  │ last_name        STRING│               │ name             STRING│
  │ email            STRING│               │ category         STRING│
  │ loyalty_tier     STRING│               │ price_usd        DOUBLE│
  │ country          STRING│               │ weight_kg        DOUBLE│
  │ __START_AT    TIMESTAMP│               └───────────┬────────────┘
  │ __END_AT (SCD2)TIMESTAMP│                           │
  └───────────┬────────────┘                           │
              │                                        │
              │ 1                                      │ 1
              │                                        │
              │ 0..*                                   │ 0..*
 ┌────────────┴────────────────────────────────────────┴───────────┐
 │                        SILVER_CLICKSTREAM                       │
 ├─────────────────────────────────────────────────────────────────┤
 │ event_id (PK)        STRING                                     │
 │ session_id           STRING                                     │
 │ customer_id (FK)     STRING                                     │
 │ product_id (FK)      STRING                                     │
 │ event_type           STRING  (search, page_view, view, purchase)│
 │ order_id             STRING  (populated exclusively on purchase)│
 │ event_timestamp      TIMESTAMP                                  │
 │ page_url             STRING                                     │
 │ referrer             STRING                                     │
 │ device_type          STRING                                     │
 └─────────────────────────────────────────────────────────────────┘
```


### Manual Infrastructure Deployment
1. Move into the configuration bundle root directory:
   ```bash
   cd bundle
   ```
2. Validate the bundle layout logic templates:
   ```bash
   databricks bundle validate --target uat
   ```
3. Deploy the structural workflows and tables up to the target environment space:
   ```bash
   databricks bundle deploy --target uat
   ```

## Testing Strategy

The code uses a multi-tier testing framework to ensure data integrity across environments.

### 1. Local Unit Tests
Asserts pure dataframe transformation functions using isolated, mock data inputs without requiring connection dependencies.

### 2. End-to-End (E2E) Integration Tests
The integration test suite utilizes a deterministic, fast-testing paradigm. It uses the Databricks Python SDK to lookup deployed workspace entities, triggers a mock dataset generation pass (creating exactly 2 customers, 3 products, and 5 clickstream rows), pushes them up to UAT Unity Catalog Volumes, runs the cloud DLT pipeline updates, and validates row conservation.

## CI/CD Pipeline Automation

The workflow configuration handles testing and pushes automatically via GitHub Actions pipelines.

1. **`unit-test` Job:** Executes pytest checks across all logic layers inside an isolated Ubuntu instance runner.
2. **`validate-and-deploy-uat` Job:** Triggered if unit tests pass. Uses secrets to establish a secure connection to Databricks, validates the asset bundle layout, ensures the target catalog exists, deploys all components, and triggers the workspace environment bootstrap job.
3. **`integration-test` Job:** Fires after deployment. Installs testing requirements, triggers the end-to-end data conservation tests, and asserts that no records dropped across runtime execution loops.