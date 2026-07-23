"""Render a local Unity Catalog bootstrap SQL file from .env.

The generated file contains environment-specific resource names and is ignored by Git.
No secrets are required by this script.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env"
TEMPLATE_FILE = ROOT / "sql" / "bootstrap.sql.template"
OUTPUT_FILE = ROOT / "sql" / "bootstrap.generated.sql"

REQUIRED = {
    "RETAIL_CATALOG",
    "RETAIL_SOURCE_SCHEMA",
    "RETAIL_OUTPUT_SCHEMA",
    "RETAIL_VOLUME",
    "RETAIL_STORAGE_ACCOUNT",
    "RETAIL_STORAGE_CONTAINER",
    "RETAIL_STORAGE_PREFIX",
    "RETAIL_STORAGE_CREDENTIAL",
    "RETAIL_EXTERNAL_LOCATION",
    "DATABRICKS_PRINCIPAL",
}


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        raise SystemExit("Missing .env. Copy .env.example to .env and fill in the values.")

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator:
            raise SystemExit(f"Invalid .env line: {raw_line}")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def validate(values: dict[str, str]) -> None:
    missing = sorted(REQUIRED - values.keys())
    placeholders = sorted(
        key for key in REQUIRED
        if key in values and (not values[key] or "<" in values[key] or ">" in values[key])
    )
    if missing or placeholders:
        details = []
        if missing:
            details.append("missing: " + ", ".join(missing))
        if placeholders:
            details.append("unresolved placeholders: " + ", ".join(placeholders))
        raise SystemExit("Cannot render bootstrap SQL (" + "; ".join(details) + ").")


def render(template: str, values: dict[str, str]) -> str:
    pattern = re.compile(r"\$\{([A-Z0-9_]+)\}")

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            raise SystemExit(f"Template variable {key} is not defined in .env")
        return values[key]

    return pattern.sub(replace, template)


def main() -> None:
    values = {**os.environ, **load_env(ENV_FILE)}
    validate(values)
    rendered = render(TEMPLATE_FILE.read_text(encoding="utf-8"), values)
    OUTPUT_FILE.write_text(rendered, encoding="utf-8")
    print(f"Created {OUTPUT_FILE.relative_to(ROOT)}")
    print("Review it, run it in Databricks SQL, and do not commit it.")


if __name__ == "__main__":
    main()
