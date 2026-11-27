"""Regression: a new or removed extractor must not reach the raw layer without
the catalog manifest (consumed by dbt to declare the Glue tables) following."""
import importlib.util
import json
from pathlib import Path

from utils.task_io import s3_key

REPO_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "export_raw_catalog", REPO_ROOT / "scripts" / "export_raw_catalog.py"
)
export_raw_catalog = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(export_raw_catalog)


def _committed():
    return json.loads(export_raw_catalog.MANIFEST_PATH.read_text(encoding="utf-8"))


def test_committed_manifest_matches_registered_extractors():
    assert export_raw_catalog.MANIFEST_PATH.exists(), "run scripts/export_raw_catalog.py"
    assert _committed() == export_raw_catalog.build_manifest(), (
        "catalog/raw_tables.json is stale; run scripts/export_raw_catalog.py"
    )


def test_table_names_are_unique():
    tables = [t["table"] for t in _committed()["tables"]]
    assert len(tables) == len(set(tables))


def test_location_template_matches_writer_key():
    for t in _committed()["tables"]:
        key = s3_key(t["bundle"], t["name"], "run1", "2026-01-04")
        resolved = t["location_template"].replace("${ingestion_date}", "2026-01-04")
        assert key.startswith(resolved), (t["table"], key)
