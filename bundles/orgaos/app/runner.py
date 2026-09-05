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
import os
import inspect
import json

from clients.camara_client import AsyncCamaraClient
from utils.task_io import read_dependency, resolve_ingestion_date, write_output
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


# Timeouts dimensionados pelas duracoes reais da run de producao
# scheduled__2026-08-23 (medidas no metadata DB do Airflow), nao por estimativa.
# codigo_situacao ficou perto do limite anterior (575s de 600s).
#
# Ate agora estes numeros eram ficcao: o parse do CSV rodava em asyncio.to_thread,
# que asyncio.wait_for nao consegue cancelar, entao a task ultrapassava o proprio
# timeout em silencio. Com o parse cancelavel (BulkParseTimeout), o limite passou
# a valer de verdade — e precisa refletir quanto o trabalho realmente leva, senao
# extractors que hoje terminam passariam a falhar.
_TIMEOUT_OVERRIDES = {'codigo_situacao': 1200}
_DEFAULT_TIMEOUT = 600


def handler(event: dict, context=None):
    """Main handler with per-extractor timeout protection."""
    timeout = _TIMEOUT_OVERRIDES.get(event.get("extractor"), _DEFAULT_TIMEOUT)
    # O parse do bulk aborta sozinho um pouco antes do limite duro, deixando
    # margem para escrever a saida em vez de morrer com tudo perdido.
    os.environ.setdefault("CAMARA_TASK_BUDGET_S", str(max(60, timeout - 120)))
    try:
        return asyncio.run(asyncio.wait_for(_run(event), timeout=timeout))
    except asyncio.TimeoutError:
        print(f"[ERROR] Extraction timeout exceeded {timeout}s. Failing for Airflow retry.")
        raise TimeoutError(f"Extraction timeout exceeded {timeout} seconds") from None


async def _run(event: dict):
    extractor_name = event.get("extractor")
    if not extractor_name:
        raise ValueError("Field 'extractor' is required in the event.")

    params = event.get("params", {})
    destination = event.get("destination", {})
    run_id = event.get("run_id", "local")
    # Resolvido uma vez e repassado a leitura e escrita: as duas precisam
    # concordar na particao, e elas rodam em tasks ECS diferentes.
    ingestion_date = resolve_ingestion_date(event)

    extractor_cls = EXTRACTORS.get(extractor_name)
    if not extractor_cls:
        raise ValueError(
            f"Extractor '{extractor_name}' not found. Available: {list(EXTRACTORS)}"
        )
    
    client = AsyncCamaraClient()
    bundle_name = os.getenv("BUNDLE", "orgaos")

    resolved_params = dict(params)
    for param_name, dependency in DEPENDENCIES.get(
            extractor_name, {}
        ).items():
        if param_name not in resolved_params:
            print(
                f"[runner] Resolving dependency '{param_name}' via '{dependency}'..."
            )

            # Try to load from cache first (S3 or local)
            cached_data = read_dependency(destination, bundle_name, dependency, run_id, ingestion_date)
            if cached_data is not None:
                resolved_params[param_name] = cached_data
                continue

            # Fall back to recomputation
            dep_cls = EXTRACTORS[dependency]
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
    extractor_instance = extractor_cls(client)
    data = await extractor_instance.extract(**filtered_params)

    records = await write_output(data, destination, bundle_name, extractor_name, run_id, ingestion_date)

    # Check if extraction was partial (timeout or budget exhaustion)
    status = "partial" if getattr(extractor_instance, "partial", False) else "success"

    return {
        "run_id": run_id,
        "extractor": extractor_name,
        "status": status,
        "records": records
    }


if __name__ == "__main__":
    import sys
    import json

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
                "extractor": "orgaos",
                "params": {},
                "destination": {
                    "type": "local",
                    "path": "/tmp/orgaos/orgaos_output.json"
                },
                "run_id": "ecs-test"
            }

    result = handler(event)
    print(json.dumps(result, ensure_ascii=False, indent=2))
