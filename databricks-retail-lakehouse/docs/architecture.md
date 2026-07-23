# Architecture

## Logical flow

1. `generate_source_data.py` creates deterministic JSON batches in a Unity
   Catalog volume.
2. Auto Loader incrementally discovers JSON files.
3. Bronze streaming tables preserve source records and ingestion metadata.
4. Silver tables standardize, validate, quarantine, and deduplicate records.
5. Gold materialized views create conformed dimensions, facts, and KPIs.
6. A Lakeflow Job runs setup, pipeline refresh, and validation in dependency order.

## Why Lakeflow Spark Declarative Pipelines?

The pipeline describes the desired tables and dependencies. Databricks determines
execution order, maintains streaming state, records expectation metrics, and
handles incremental refreshes.

## Why a Unity Catalog volume?

A volume provides governed file storage inside the Unity Catalog hierarchy. It
makes the demo portable and avoids embedding cloud-specific S3, ADLS, or GCS
credentials in the repository.

## Medallion responsibilities

### Bronze
- Preserve raw data
- Add ingestion metadata
- Avoid business transformations
- Rescue unexpected fields

### Silver
- Apply types and naming standards
- Validate required fields
- Deduplicate events
- Quarantine invalid data
- Create trusted records

### Gold
- Join conformed entities
- Publish business-level facts and dimensions
- Aggregate reusable KPIs
- Optimize for analytics consumption

## Production evolution

For a real platform:
- Replace generated files with object storage landing zones.
- Use file notification mode for very large directories.
- Separate storage credentials from application code.
- Add service-principal ownership.
- Add data classification and masking.
- Send pipeline metrics to an observability platform.
- Use separate catalogs and workspaces for environments.
