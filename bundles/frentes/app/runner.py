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

from clients.camara_client import AsyncCamaraClient
from utils.task_io import read_dependency, write_output
from extractors.camara.frentes.frentes import AsyncFrentesExtractor
from extractors.camara.frentes.ids import AsyncFrentesIdsExtractor
from extractors.camara.frentes.membros import AsyncFrentesMembrosExtractor

EXTRACTORS = {
    "frentes": AsyncFrentesExtractor,
    "ids": AsyncFrentesIdsExtractor,
    "membros": AsyncFrentesMembrosExtractor,
}

DEPENDENCIES = {
    "ids":     {"frentes": "frentes"},
    "membros": {"frentes": "frentes"},
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
    bundle_name = os.getenv("BUNDLE", "frentes")

    resolved_params = dict(params)
    for param_name, dependency in DEPENDENCIES.get(
            extractor_name, {}
        ).items():
        if param_name not in resolved_params:
            print(
                f"[runner] Resolving dependency '{param_name}' via '{dependency}'..."
            )

            # Try to load from cache first (S3 or local)
            cached_data = read_dependency(destination, bundle_name, dependency, run_id)
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
    data = await extractor_cls(client).extract(**filtered_params)

    records = write_output(data, destination, bundle_name, extractor_name, run_id)

    return {
        "run_id": run_id,
        "extractor": extractor_name,
        "status": "success",
        "records": records
    }


if __name__ == "__main__":
    import sys
    import os
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
                "extractor": "frentes",
                "params": {},
                "destination": {
                    "type": "local",
                    "path": "/tmp/frentes/frentes_output.json"
                },
                "run_id": "ecs-test"
            }

    result = handler(event)
    print(json.dumps(result, ensure_ascii=False, indent=2))
