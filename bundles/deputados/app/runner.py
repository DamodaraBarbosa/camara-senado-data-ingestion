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
import inspect
import json
import boto3

from clients.camara_client import AsyncCamaraClient
from extractors.camara.deputados.deputados import AsyncDeputadosExtractor
from extractors.camara.deputados.ids import AsyncIdsExtractor
from extractors.camara.deputados.discursos import AsyncDiscursosExtractor
from extractors.camara.deputados.despesas import AsyncDespesasExtractor
from extractors.camara.deputados.frentes import AsyncFrentesExtractor
from extractors.camara.deputados.eventos import AsyncEventosExtractor
from extractors.camara.deputados.historico import AsyncHistoricoExtractor
from extractors.camara.deputados.lideres import AsyncLideresExtractor
from extractors.camara.deputados.mesa import AsyncMesaExtractor
from extractors.camara.deputados.orgaos import AsyncOrgaosExtractor
from extractors.camara.deputados.profissoes import AsyncProfissoesExtractor
from extractors.camara.deputados.ocupacoes import AsyncOcupacoesExtractor
from extractors.camara.deputados.codigo_situacao import AsyncCodigoSituacaoExtractor
from extractors.camara.deputados.mandatos_externos import AsyncMandatosExternosExtractor

EXTRACTORS = {
    "deputados": AsyncDeputadosExtractor,
    "ids": AsyncIdsExtractor,
    "discursos": AsyncDiscursosExtractor,
    "despesas": AsyncDespesasExtractor,
    "frentes": AsyncFrentesExtractor,
    "eventos": AsyncEventosExtractor,
    "historico": AsyncHistoricoExtractor,
    "lideres": AsyncLideresExtractor,
    "mesa": AsyncMesaExtractor,
    "orgaos": AsyncOrgaosExtractor,
    "profissoes": AsyncProfissoesExtractor,
    "ocupacoes": AsyncOcupacoesExtractor,
    "codigo_situacao": AsyncCodigoSituacaoExtractor,
    "mandatos_externos": AsyncMandatosExternosExtractor,
}

DEPENDENCIES = {
    "ids":               {"deputados": "deputados"},
    "discursos":         {"deputados": "deputados"},
    "despesas":          {"deputados": "deputados"},
    "frentes":           {"deputados": "deputados"},
    "eventos":           {"deputados": "deputados"},
    "historico":         {"deputados": "deputados"},
    "profissoes":        {"deputados": "deputados"},
    "ocupacoes":         {"deputados": "deputados"},
    "mandatos_externos": {"deputados": "deputados"},
    "orgaos":            {"deputados": "deputados"},
    # lideres is independent — extract(init_legislatura, items, request_tries)
}


def handler(event: dict, context=None):
    """Main handler with timeout protection (10 min max per extraction)."""
    try:
        return asyncio.run(
            asyncio.wait_for(_run(event), timeout=600)  # 10 minutos = 600s
        )
    except asyncio.TimeoutError:
        print("[ERROR] Extração excedeu timeout de 10 minutos. Falhando para retry do Airflow.")
        raise TimeoutError("Extraction timeout exceeded 10 minutes") from None


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
    for param_name, dep_extractor_name in DEPENDENCIES.get(
            extractor_name, {}
        ).items():
        if param_name not in resolved_params:
            print(
                f"[runner] Resolving dependency '{param_name}' via '{dep_extractor_name}'..."
            )
            dep_cls = EXTRACTORS[dep_extractor_name]
            dep_data = await dep_cls(client).extract(
                **{k: v for k, v in params.items()
                   if k in dep_cls.extract.__code__.co_varnames}
            )
            resolved_params[param_name] = dep_data
            print(
                f"[runner] Dependency '{param_name}' resolved: {len(dep_data)} records."
            )

    # Filter resolved_params to match target extractor signature
    sig = inspect.signature(extractor_cls.extract)
    filtered_params = {k: v for k, v in resolved_params.items() if k in sig.parameters or any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())}
    data = await extractor_cls(client).extract(**filtered_params)

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
    content = json.dumps(data, ensure_ascii=False)

    if dest_type == "s3":
        bucket = destination.get("bucket")
        prefix = destination.get("prefix", "").rstrip("/")
        key = (
            f"{prefix}/{extractor_name}_{run_id}.json" if prefix
            else f"{extractor_name}_{run_id}.json"
        )  # noqa: E501
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content.encode('utf-8'))

    elif dest_type == "local":
        out_path = destination.get(
            "path", f"/tmp/deputados/{extractor_name}_{run_id}.json"
        )
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Saved {len(data)} records to {out_path}")

    else:
        raise ValueError(f"Unknown destination type: {dest_type}")


if __name__ == "__main__":
    import sys
    import os

    event_env = os.getenv("EVENT_PAYLOAD")
    event = {}

    if event_env:
        try:
            print("[INFO] Loading event configuration directly from environment variable 'EVENT_PAYLOAD'...")
            event = json.loads(event_env)
        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to parse EVENT_PAYLOAD as JSON: {e}")
            sys.exit(1)
    else:
        event_path = sys.argv[1] if len(sys.argv) > 1 else "event.json"
        try:
            with open(event_path, "r", encoding="utf-8") as f:
                event = json.load(f)
        except FileNotFoundError:
            print(f"[WARNING] Event file '{event_path}' not found. Using default empty event.")
            # Fallback padrão seguro para testes locais ou produção sem parâmetros
            event = {
                "extractor": "deputados",
                "params": {},
                "destination": {
                    "type": "local",
                    "path": "/tmp/deputados/deputados_output.json"
                },
                "run_id": "ecs-test"
            }

    result = asyncio.run(_run(event))
    print(json.dumps(result, ensure_ascii=False, indent=2))
