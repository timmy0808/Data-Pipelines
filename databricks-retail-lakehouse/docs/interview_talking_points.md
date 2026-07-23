# Interview Talking Points

## Thirty-second explanation

I built a Databricks retail lakehouse that incrementally ingests JSON files with
Auto Loader and processes them through Bronze, Silver, and Gold layers using
Lakeflow Spark Declarative Pipelines. Silver applies validation, quarantine, and
streaming deduplication. Gold publishes conformed dimensions, facts, and business
KPIs. I packaged the pipeline and orchestration as a Declarative Automation Bundle
and added tests and GitHub Actions deployment.

## Design decisions

### Why explicit schemas?
Explicit schemas make ingestion behavior predictable and prevent accidental type
drift. Unexpected values are rescued rather than silently discarded.

### Why quarantine instead of only dropping?
Quarantine preserves bad source records for remediation and audit analysis.

### Why materialized Gold views?
Gold outputs join and aggregate multiple trusted tables. Materialized views make
those business results declarative and incrementally maintainable by the pipeline.

### How is idempotency handled?
Auto Loader tracks processed files, Silver deduplicates by business keys, and the
job has one concurrent run. Production source files should be immutable.

### How would this scale?
Use cloud object storage, file notifications, appropriately sized or serverless
pipeline compute, liquid clustering where justified, and partition-independent
incremental processing.

### What would you change for production?
Use separate catalogs/workspaces, service-principal ownership, least privilege,
data classification, monitoring, durable source retention, and no demo-data reset.

## Known simplification

`dim_customer` is a current-state Type 1 dimension. A strong next enhancement is
an SCD Type 2 table using `dp.create_auto_cdc_flow` and sequenced customer change
events.
