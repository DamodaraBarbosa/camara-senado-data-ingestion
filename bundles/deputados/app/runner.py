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
import os

from clients.camara_client import AsyncCamaraClient
from utils.task_io import read_dependency, resolve_ingestion_date, write_output
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


# despesas downloads and parses multiple years of CEAP (~750k lines). Parsing
# (CPU-bound, single-threaded) runs much slower on Fargate vCPU than on a dev
# laptop, with considerable network variance (retries/resumes observed) —
# even 1800s wasn't sufficient in real testing, so the wide margin.
# Medidos na run de producao scheduled__2026-08-23: despesas 2342s,
# frentes 1017s. frentes vinha rodando sob um limite declarado de 600s e so
# terminava porque o parse em asyncio.to_thread nao era cancelavel.
_TIMEOUT_OVERRIDES = {"despesas": 3600, "frentes": 1800}
_DEFAULT_TIMEOUT = 600


def handler(event: dict, context=None):
    """Main handler with timeout protection (10 min max per extraction, configurable per extractor)."""
    timeout = _TIMEOUT_OVERRIDES.get(event.get("extractor"), _DEFAULT_TIMEOUT)
    # O parse do bulk aborta sozinho um pouco antes do limite duro, deixando
    # margem para escrever a saida em vez de morrer com tudo perdido.
    os.environ.setdefault("CAMARA_TASK_BUDGET_S", str(max(60, timeout - 120)))
    try:
        return asyncio.run(asyncio.wait_for(_run(event), timeout=timeout))
    except asyncio.TimeoutError:
        print(f"[ERROR] Extraction timeout exceeded {timeout // 60} minutes. Failing for Airflow retry.")
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
    bundle_name = os.getenv("BUNDLE", "deputados")

    resolved_params = dict(params)
    for param_name, dep_extractor_name in DEPENDENCIES.get(
            extractor_name, {}
        ).items():
        if param_name not in resolved_params:
            print(
                f"[runner] Resolving dependency '{param_name}' via '{dep_extractor_name}'..."
            )

            # Try to load from cache first (S3 or local)
            cached_data = read_dependency(destination, bundle_name, dep_extractor_name, run_id, ingestion_date)
            if cached_data is not None:
                resolved_params[param_name] = cached_data
                continue

            # Fall back to recomputation
            dep_cls = EXTRACTORS[dep_extractor_name]
            dep_result = dep_cls(client).extract(
                **{k: v for k, v in params.items()
                   if k in dep_cls.extract.__code__.co_varnames}
            )
            # Note: if a dependency extractor becomes an async generator in the future,
            # uncomment the .isasyncgen check below. Currently no extractors in
            # DEPENDENCIES return a generator.
            dep_data = await dep_result
            resolved_params[param_name] = dep_data
            print(
                f"[runner] Dependency '{param_name}' resolved: {len(dep_data)} records."
            )

    # Filter resolved_params to match target extractor signature
    sig = inspect.signature(extractor_cls.extract)
    filtered_params = {k: v for k, v in resolved_params.items() if k in sig.parameters or any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())}
    extractor_instance = extractor_cls(client)
    result = extractor_instance.extract(**filtered_params)
    # Some extractors (e.g., despesas) return async generators for streaming
    data = result if inspect.isasyncgen(result) else await result

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
            # Safe default fallback for local tests or production without parameters
            event = {
                "extractor": "deputados",
                "params": {},
                "destination": {
                    "type": "local",
                    "path": "/tmp/deputados/deputados_output.json"
                },
                "run_id": "ecs-test"
            }

    result = handler(event)
    print(json.dumps(result, ensure_ascii=False, indent=2))
