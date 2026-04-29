import sys
from pathlib import Path

from bundles.deputados.app.runner import _write_output

# Allow running this script directly from the repo root without setting
# PYTHONPATH. Add both the project root and the `src/` folder to sys.path
# so imports like `clients.*` or `src.clients.*` work in local runs.
ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import asyncio
import json
import boto3

from clients.camara_client import AsyncCamaraClient
from extractors.camara.blocos.blocos import AsyncBlocosExtractor
from extractors.camara.blocos.ids import AsyncBlocosIdsExtractor
from extractors.camara.blocos.partidos import AsyncBlocosPartidosExtractor

EXTRACTORS = {
    "blocos": AsyncBlocosExtractor,
    "ids": AsyncBlocosIdsExtractor,
    "partidos": AsyncBlocosPartidosExtractor,
}

DEPENDENCIES = {
    "ids":      {"blocos": "blocos"},
    "partidos": {"blocos": "blocos"},
}


def handler(event: dict, context=None):
    return asyncio.run(_run(event))


async def _run(event: dict):
    extractor_name = event.get("extractor")
    if not extractor_name:
        raise ValueError("Field 'extractor' is required in the event.")
    
    params = event.get("params", {})
    destination = event.get("destination", {})
    run_id = event.get("run_id", "local")

    extractor_cls = EXTRACTORS.get(extractor_name)
    if not extractor_cls:
        raise ValueError(
            f"Extractor '{extractor_name}' not found. Available: {list(EXTRACTORS)}"
        )
    
    client = AsyncCamaraClient()

    resolved_params = dict(params)
    for param_name, dependency in DEPENDENCIES.get(
            extractor_name, {}
        ).items():
        if param_name not in resolved_params:
            print(
                f"[runner] Resolving dependency '{param_name}' via '{dependency}'..."
            )
            dep_cls = EXTRACTORS[dependency]
            dep_data = await dep_cls(client).extract(
                **{k: v for k, v in params.items()
                   if k in dep_cls.extract.__code__.co_varnames}
            )
            resolved_params[param_name] = dep_data
            print(
                f"[runner] Dependency '{param_name}' resolved: {len(dep_data)} records."
            )

    data = await extractor_cls(client).extract(**resolved_params)

    _write_output(data, destination, extractor_name, run_id)

    return {
        "run_id": run_id,
        "extractor": extractor_name,
        "status": "success",
        "records": len(data)
    }


def _write_output(
        data: list,
        destination: dict,
        extractor_name: str,
        run_id: str
    ):
    dest_type = destination.get("type", "local")
    content = json.dumps(data, ensure_ascii=False, indent=2)

    if dest_type == "s3":
        bucket = destination.get("bucket")
        prefix = destination.get("prefix", "").rstrip("/")
        key = (
            f"{prefix}/{extractor_name}_{run_id}.json" if prefix
            else f"{extractor_name}_{run_id}.json"
        )
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content.encode("utf-8"),
        )

    else:
        out_path = destination.get(
            "path", f"/tmp/blocos/{extractor_name}_{run_id}.json"
        )
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Saved {len(data)} records to {out_path}")


if __name__ == "__main__":
    import sys

    event_path = sys.argv[1] if len(sys.argv) > 1 else "event.json"
    with open(event_path, "r", encoding="utf-8") as f:
        event = json.load(f)
    
    result = asyncio.run(_run(event))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    