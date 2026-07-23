# Deployment Guide

## 1. Prepare Databricks

You need:
- A Unity Catalog-enabled workspace
- Permission to create or use catalogs and schemas
- Permission to create volumes and pipelines
- Serverless Lakeflow pipelines, or supported classic pipeline compute

The default development deployment uses:
- Catalog: `retail_dev`
- Output schema: `retail_lakehouse`
- Source schema: `retail_sources`
- Volume: `landing`

The pipeline's catalog and target schema should exist before the first bundle
deployment. Run `sql/bootstrap.sql` in the Databricks SQL editor, or ask an
administrator to create the objects. The setup notebook also uses idempotent
`CREATE IF NOT EXISTS` statements, but those run only after the bundle has
already been deployed.

Example grant:

```sql
GRANT USE CATALOG, CREATE SCHEMA
ON CATALOG retail_dev
TO `<your-user-or-group>`;
```

You also need permission to create a volume and managed tables in the schemas.

## 2. Install the Databricks CLI

Install the current Databricks CLI for your operating system. Verify:

```bash
databricks version
```

Bundles require Databricks CLI 0.218.0 or later; use a current release.

## 3. Authenticate locally

Interactive browser authentication:

```bash
databricks auth login --host https://<your-workspace-host>
```

Choose a profile name such as `DEFAULT` or `retail-dev`.

Confirm authentication:

```bash
databricks current-user me
```

When using a non-default profile:

```bash
set DATABRICKS_CONFIG_PROFILE=retail-dev
```

PowerShell:

```powershell
$env:DATABRICKS_CONFIG_PROFILE = "retail-dev"
```

## 4. Clone or enter the repository

```bash
git clone https://github.com/<username>/Data-Pipelines.git
cd Data-Pipelines/databricks-retail-lakehouse
```

## 5. Validate locally

Run local tests:

```bash
python -m venv .venv
```

PowerShell:

```powershell
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
pytest -q
ruff check tests
```

Validate the Databricks resource definitions:

```bash
databricks bundle validate -t dev
```

If validation rejects `serverless: true`, your workspace may not support
serverless pipelines. Configure supported classic pipeline compute through the
workspace or update the pipeline resource for your environment.

## 6. Deploy development resources

```bash
databricks bundle deploy -t dev
```

This uploads source code and creates or updates:
- The Lakeflow pipeline
- The Lakeflow Job
- Bundle-managed workspace files

## 7. Run the complete workflow

```bash
databricks bundle run -t dev retail_lakehouse_job
```

The job performs:
1. Catalog/schema/volume setup and source-data generation
2. Incremental pipeline refresh
3. Integration validation

Open the Run URL printed by the CLI.

## 8. Verify results

In Databricks SQL or a notebook:

```sql
SHOW TABLES IN retail_dev.retail_lakehouse;
```

```sql
SELECT *
FROM retail_dev.retail_lakehouse.gold_daily_sales
ORDER BY order_date;
```

```sql
SELECT *
FROM retail_dev.retail_lakehouse.quarantine_orders;
```

Expected outcomes:
- Valid source records appear in Silver.
- `BAD001` appears in `quarantine_orders`.
- The invalid customer appears in `quarantine_customers`.
- Duplicate `O1004` is represented once in `silver_orders`.
- Gold KPI tables contain non-cancelled sales.

## 9. Demonstrate incremental processing

After the first successful run, add a third file to the volume:

```python
dbutils.fs.put(
  "/Volumes/retail_dev/retail_sources/landing/orders/batch_003.json",
  '{"order_id":"O1007","customer_id":"C001","order_timestamp":"2026-03-01T10:00:00Z","status":"DELIVERED","payment_method":"CARD","order_total":34.50}',
  True
)
```

Add its line item:

```python
dbutils.fs.put(
  "/Volumes/retail_dev/retail_sources/landing/order_items/batch_003.json",
  '{"order_item_id":"OI010","order_id":"O1007","product_id":"P101","quantity":1,"unit_price":34.50,"line_total":34.50}',
  True
)
```

Run again:

```bash
databricks bundle run -t dev retail_lakehouse_job
```

For a true incremental demonstration, temporarily disable the cleanup portion
of `generate_source_data.py`, or refresh only the pipeline after writing the
third files:

```bash
databricks bundle run -t dev retail_lakehouse_pipeline
```

## 10. Deploy production

Edit the `prod` target values in `databricks.yml`, then:

```bash
databricks bundle validate -t prod
databricks bundle deploy -t prod
databricks bundle run -t prod retail_lakehouse_job
```

Production recommendations:
- Use a service principal, not a personal identity.
- Remove automatic catalog creation.
- Use least-privilege grants.
- Use production object storage or governed external locations.
- Protect the GitHub environment.
- Require pull-request approval.
- Do not regenerate or delete landing data on every run.

## 11. GitHub Actions deployment

The included workflow uses GitHub OIDC authentication.

Create repository or environment variables:
- `DATABRICKS_HOST`
- `DATABRICKS_CLIENT_ID`

Configure a Databricks service principal and workload identity federation so
GitHub can obtain a short-lived identity token. The workflow then runs:

```text
bundle validate -> bundle deploy -> bundle run
```

Do not store a personal access token in the repository.

## 12. Destroy bundle-managed resources

```bash
databricks bundle destroy -t dev
```

This removes resources managed by the bundle. Unity Catalog data objects created
by the setup notebook may require separate cleanup:

```sql
DROP SCHEMA IF EXISTS retail_dev.retail_lakehouse CASCADE;
DROP SCHEMA IF EXISTS retail_dev.retail_sources CASCADE;
```
