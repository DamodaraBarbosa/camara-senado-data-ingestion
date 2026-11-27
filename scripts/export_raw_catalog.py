#!/usr/bin/env python3
"""Generate catalog/raw_tables.json: one entry per registered extractor.

The manifest is the contract consumed by the dbt repo (dbt-external-tables)
to declare Glue tables with partition projection over the raw layer. It is
derived statically (ast) from each bundles/*/app/runner.py EXTRACTORS dict,
so it runs without installing the ingestion dependencies.

Usage:
    python scripts/export_raw_catalog.py           # rewrite the manifest
    python scripts/export_raw_catalog.py --check   # exit 1 if it is stale
"""
import argparse
import ast
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BUNDLES_DIR = REPO_ROOT / "bundles"
MANIFEST_PATH = REPO_ROOT / "catalog" / "raw_tables.json"


def registered_extractors(runner_path: Path) -> list:
    tree = ast.parse(runner_path.read_text(encoding="utf-8"))
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "EXTRACTORS" for t in node.targets)
            and isinstance(node.value, ast.Dict)
        ):
            return [k.value for k in node.value.keys]
    raise ValueError(f"EXTRACTORS dict not found in {runner_path}")


def build_manifest() -> dict:
    tables = []
    for runner in sorted(BUNDLES_DIR.glob("*/app/runner.py")):
        bundle = runner.parents[1].name
        for name in sorted(registered_extractors(runner)):
            prefix = f"raw/{bundle}/{name}/"
            tables.append({
                "bundle": bundle,
                "name": name,
                "table": f"{bundle}__{name}",
                "prefix": prefix,
                "location_template": prefix + "ingestion_date=${ingestion_date}/",
            })
    return {
        "format": "ndjson",
        "partition_key": {"name": "ingestion_date", "type": "date", "format": "yyyy-MM-dd"},
        "tables": tables,
    }


def render(manifest: dict) -> str:
    return json.dumps(manifest, indent=2, ensure_ascii=False) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="fail if the committed manifest is stale")
    args = parser.parse_args()

    expected = render(build_manifest())
    if args.check:
        current = MANIFEST_PATH.read_text(encoding="utf-8") if MANIFEST_PATH.exists() else ""
        if current != expected:
            print(f"{MANIFEST_PATH.relative_to(REPO_ROOT)} is stale; run scripts/export_raw_catalog.py",
                  file=sys.stderr)
            return 1
        return 0

    MANIFEST_PATH.parent.mkdir(exist_ok=True)
    MANIFEST_PATH.write_text(expected, encoding="utf-8")
    print(f"wrote {len(json.loads(expected)['tables'])} tables to {MANIFEST_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
