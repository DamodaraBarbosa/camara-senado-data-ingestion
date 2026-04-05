from runner import EXTRACTORS, DEPENDENCIES, _write_output
from clients.camara_client import AsyncCamaraClient
import json
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


EVENTS_DIR = Path(__file__).resolve().parents[1] / "events"

EXECUTION_ORDER = [
    "deputados",
    "mesa",
    "codigo_situacao",
    "lideres",
    "ids",
    "frentes",
    "profissoes",
    "ocupacoes",
    "mandatos_externos",
    "despesas",
    "historico",
    "discursos",
    "eventos",
    "orgaos"
]


async def run_all(base_destination: dict = None, run_id: str = "run-all"):
    client = AsyncCamaraClient()
    cache = {}

    for extractor_name in EXECUTION_ORDER:
        event_file = EVENTS_DIR / f"{extractor_name}.json"
        if not event_file.exists():
            print(f"[run_all] Skipping '{extractor_name}': event file not found.")
            continue

        with open(event_file, "r", encoding="utf-8") as f:
            event = json.load(f)

        params = dict(event.get("params", {}))
        destination = base_destination or event.get("destination", {})

        for param_name, dep_name in DEPENDENCIES.get(extractor_name, {}).items():
            if param_name not in params and dep_name in cache:
                params[param_name] = cache[dep_name]

        extractor_cls = EXTRACTORS[extractor_name]
        print(f"[run_all] Running '{extractor_name}'...")
        data = await extractor_cls(client).extract(**params)
        cache[extractor_name] = data

        _write_output(data, destination, extractor_name, run_id)
        print(f"[run_all] '{extractor_name}' done — {len(data)} records.\n")

    print("[run_all] All extractors finished.")


if __name__ == "__main__":
    asyncio.run(run_all())
