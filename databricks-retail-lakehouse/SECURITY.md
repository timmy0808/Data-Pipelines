# Security and configuration

This repository intentionally contains no live credentials or personal environment identifiers.

## Never commit

- Databricks personal access tokens
- Azure client secrets, storage keys, or SAS tokens
- `.env`
- `sql/bootstrap.generated.sql`
- Databricks CLI profiles from `~/.databrickscfg`
- Terraform state files

Azure resource names, workspace hosts, client IDs, and access connector resource IDs are identifiers rather than passwords, but keeping personal environment values out of a public portfolio repository makes the project reusable and reduces unnecessary exposure.

## Authentication

For local development, use Databricks CLI browser authentication and a local profile. For GitHub Actions, use Databricks workload identity federation/OIDC with repository or environment variables. Do not use a personal access token in CI.

## Before pushing

Run:

```bash
python scripts/check_repository_safety.py
```

Also inspect staged changes:

```bash
git diff --cached
```
