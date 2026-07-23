"""Fail when common secret or live-environment patterns are committed."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ALLOWLIST = {
    ".env.example",
    "scripts/check_repository_safety.py",
    "docs/deployment.md",
    "SECURITY.md",
}
PATTERNS = {
    "Azure subscription resource ID": re.compile(r"/subscriptions/[0-9a-fA-F-]{20,}"),
    "Databricks personal access token": re.compile(r"\bdapi[a-zA-Z0-9]{20,}\b"),
    "Azure storage SAS signature": re.compile(r"(?:\?|&)sig=[^\s'\"]+", re.I),
    "Azure client secret assignment": re.compile(r"AZURE_CLIENT_SECRET\s*=\s*[^<\s][^\s]*", re.I),
}


def tracked_files() -> list[Path]:
    try:
        output = subprocess.check_output(
            ["git", "ls-files"], cwd=ROOT, text=True, stderr=subprocess.DEVNULL
        )
        return [ROOT / line for line in output.splitlines() if line]
    except (subprocess.CalledProcessError, FileNotFoundError):
        return [p for p in ROOT.rglob("*") if p.is_file() and ".git" not in p.parts]


def main() -> None:
    findings: list[str] = []
    for path in tracked_files():
        relative = path.relative_to(ROOT).as_posix()
        if relative in ALLOWLIST or not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for label, pattern in PATTERNS.items():
            if pattern.search(relative) or pattern.search(text):
                findings.append(f"{relative}: {label}")

    if findings:
        raise SystemExit("Repository safety check failed:\n- " + "\n- ".join(findings))
    print("Repository safety check passed.")


if __name__ == "__main__":
    main()
