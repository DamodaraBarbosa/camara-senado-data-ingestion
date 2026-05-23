import sys
from pathlib import Path

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

from clients.camara_client import AsyncCamaraClient
from extractors.camara.orgaos.orgaos import AsyncOrgaosExtractor
from extractors.camara.orgaos.ids import AsyncOrgaosIdsExtractor
from extractors.camara.orgaos.codigo_situacao import AsyncCodigoSituacaoOrgaoExtractor
from extractors.camara.orgaos.eventos import AsyncEventosExtractor
from extractors.camara.orgaos.membros import AsyncMembrosExtractor
from extractors.camara.orgaos.situacoes_orgao import SituacoesOrgaoExtractor
from extractors.camara.orgaos.votacoes import AsyncOrgaosVotacoesExtractor

EXTRACTORS = {
    "orgaos": AsyncOrgaosExtractor,
    "ids": AsyncOrgaosIdsExtractor,
    "codigo_situacao": AsyncCodigoSituacaoOrgaoExtractor,
    "eventos": AsyncEventosExtractor,
    "membros": AsyncMembrosExtractor,
    "situacoes_orgao": SituacoesOrgaoExtractor,
    "votacoes": AsyncOrgaosVotacoesExtractor,
}

DEPENDENCIES = {
    "ids":              {"orgaos": "orgaos"},
    "eventos":          {"orgaos": "orgaos"},
    "membros":          {"orgaos": "orgaos"},
    "situacoes_orgao":  {"orgaos": "orgaos"},
    "votacoes":         {"orgaos": "orgaos"},
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
        import boto3
        bucket = destination.get("bucket")
        prefix = destination.get("prefix", "").rstrip("/")
        key = (
            f"{prefix}/{extractor_name}_{run_id}.json" if prefix
            else f"{extractor_name}_{run_id}.json"
        )
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType="application/json"
        )
        print(f"[runner] Written {len(data)} records to s3://{bucket}/{key}")

    elif dest_type == "local":
        output_path = Path(destination.get("path", f"/tmp/orgaos/{extractor_name}.json"))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"[runner] Written {len(data)} records to {output_path}")

    else:
        raise ValueError(f"Unknown destination type: {dest_type}")


if __name__ == "__main__":
    import sys
    import json

    if len(sys.argv) < 2:
        print("Usage: python runner.py <path_to_event.json>")
        sys.exit(1)

    event_path = sys.argv[1]
    with open(event_path, "r", encoding="utf-8") as f:
        event = json.load(f)

    result = handler(event)
    print(json.dumps(result, ensure_ascii=False, indent=2))
