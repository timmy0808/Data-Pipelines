# Databricks Retail Lakehouse

A production-style Databricks data engineering portfolio project that incrementally
ingests retail data, validates and transforms it through Bronze, Silver, and Gold
layers, and deploys the platform with Declarative Automation Bundles.

## What this project demonstrates

- Unity Catalog volumes and managed Delta tables
- Auto Loader incremental JSON ingestion
- Lakeflow Spark Declarative Pipelines
- Bronze, Silver, and Gold medallion architecture
- Data-quality expectations and quarantined records
- Streaming deduplication
- Dimensional and aggregate analytics tables
- Lakeflow Jobs orchestration
- Declarative Automation Bundles
- Unit testing with pytest
- GitHub Actions validation and deployment

## Architecture

```text
Generated JSON files
        |
        v
Unity Catalog Volume
        |
        v
Auto Loader
        |
        v
Bronze streaming tables
        |
        v
Silver validated streaming tables
        |
        +---------------------+
        |                     |
        v                     v
Quarantine tables       Gold materialized views
                              |
                              v
                       Databricks SQL
```

## Source entities

| Entity | Description |
|---|---|
| customers | Customer profile records |
| products | Product master records |
| orders | Order-header transactions |
| order_items | Individual products purchased in each order |

The generated data includes:
- Two incremental file batches
- Customer updates
- Duplicate order events
- Invalid customer and order rows
- Late-arriving records
- Multiple product categories and US regions

## Tables created

### Bronze

- `bronze_customers`
- `bronze_products`
- `bronze_orders`
- `bronze_order_items`

Bronze tables retain the raw business columns and add:
- `_source_file`
- `_ingested_at`
- `_rescued_data`

### Silver

- `silver_customers`
- `silver_products`
- `silver_orders`
- `silver_order_items`
- `quarantine_customers`
- `quarantine_orders`

### Gold

- `dim_customer`
- `dim_product`
- `fact_orders`
- `fact_order_items`
- `gold_daily_sales`
- `gold_customer_metrics`
- `gold_product_performance`

## Repository structure

```text
.
├── databricks.yml
├── resources/
│   ├── retail_pipeline.pipeline.yml
│   └── retail_job.job.yml
├── src/
│   ├── setup/
│   │   └── generate_source_data.py
│   ├── pipeline/
│   │   ├── 01_bronze.py
│   │   ├── 02_silver.py
│   │   └── 03_gold.py
│   └── validation/
│       └── validate_pipeline.py
├── tests/
│   └── test_transformations.py
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   ├── deployment.md
│   └── interview_talking_points.md
├── .github/workflows/
│   ├── validate.yml
│   └── deploy.yml
├── pyproject.toml
└── .gitignore
```

## Prerequisites

1. A Databricks workspace with Unity Catalog enabled.
2. Permission to use or create a catalog, schema, and volume.
3. Databricks CLI installed locally.
4. Python 3.11+ for local tests.
5. Serverless Lakeflow pipelines enabled, or edit the pipeline resource to use
   supported classic pipeline compute.
6. A SQL warehouse is optional for querying Gold tables.

## First deployment

See [docs/deployment.md](docs/deployment.md) for full instructions.

Run `sql/bootstrap.sql` once in Databricks SQL before the first deployment.

Typical commands:

```bash
databricks auth login --host https://<workspace-url>
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev retail_lakehouse_job
```

## Example analytics queries

```sql
SELECT *
FROM retail_dev.gold_daily_sales
ORDER BY order_date DESC;
```

```sql
SELECT
  product_name,
  category,
  units_sold,
  revenue
FROM retail_dev.gold_product_performance
ORDER BY revenue DESC;
```

```sql
SELECT
  customer_id,
  full_name,
  order_count,
  lifetime_value
FROM retail_dev.gold_customer_metrics
ORDER BY lifetime_value DESC;
```

## Portfolio enhancements

After the base version works, add:
1. SCD Type 2 customer history with `dp.create_auto_cdc_flow`.
2. Event-driven file notification mode.
3. A Databricks SQL dashboard.
4. Pipeline-event-log monitoring.
5. Separate dev, test, and prod catalogs.
6. Workload identity federation for GitHub Actions.
7. Terraform for account and cloud infrastructure.
