import sys
import os
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
from extractors.camara.proposicoes.proposicoes import AsyncProposicoesExtractor
from extractors.camara.proposicoes.ids import AsyncIdsExtractor
from extractors.camara.proposicoes.autores import AsyncAutoresExtractor
from extractors.camara.proposicoes.codigo_situacao import AsyncCodigoSituacaoExtractor
from extractors.camara.proposicoes.codigo_tema import AsyncCodigoTemaExtractor
from extractors.camara.proposicoes.codigo_tipo_autor import AsyncCodigoTipoAutorExtractor
from extractors.camara.proposicoes.codigo_tipo_tramitacao import AsyncCodigoTipoTramitacaoExtractor
from extractors.camara.proposicoes.relacionadas import AsyncRelacionadasExtractor
from extractors.camara.proposicoes.sigla_tipo import AsyncSiglaTipoExtractor
from extractors.camara.proposicoes.situacoes_proposicao import AsyncSituacoesProposicaoExtractor
from extractors.camara.proposicoes.temas import AsyncTemasExtractor
from extractors.camara.proposicoes.tipos_autor import AsyncTiposAutorExtractor
from extractors.camara.proposicoes.tipos_proposicao import AsyncTiposProposicaoExtractor
from extractors.camara.proposicoes.tipos_tramitacao import AsyncTiposTramitacaoExtractor
from extractors.camara.proposicoes.tramitacoes import AsyncTramitacoesExtractor
from extractors.camara.proposicoes.votacoes import AsyncVotacoesExtractor

EXTRACTORS = {
    "proposicoes": AsyncProposicoesExtractor,
    "ids": AsyncIdsExtractor,
    "autores": AsyncAutoresExtractor,
    "codigo_situacao": AsyncCodigoSituacaoExtractor,
    "codigo_tema": AsyncCodigoTemaExtractor,
    "codigo_tipo_autor": AsyncCodigoTipoAutorExtractor,
    "codigo_tipo_tramitacao": AsyncCodigoTipoTramitacaoExtractor,
    "relacionadas": AsyncRelacionadasExtractor,
    "sigla_tipo": AsyncSiglaTipoExtractor,
    "situacoes_proposicao": AsyncSituacoesProposicaoExtractor,
    "temas": AsyncTemasExtractor,
    "tipos_autor": AsyncTiposAutorExtractor,
    "tipos_proposicao": AsyncTiposProposicaoExtractor,
    "tipos_tramitacao": AsyncTiposTramitacaoExtractor,
    "tramitacoes": AsyncTramitacoesExtractor,
    "votacoes": AsyncVotacoesExtractor,
}

DEPENDENCIES = {
    "ids":         {"proposicoes": "proposicoes"},
    "autores":     {"proposicoes": "proposicoes"},
    "relacionadas": {"proposicoes": "proposicoes"},
    "tramitacoes": {"proposicoes": "proposicoes"},
    "votacoes":    {"proposicoes": "proposicoes"},
    "temas":       {"proposicoes": "proposicoes"},
}


def handler(event: dict, context=None):
    """
    Main handler with timeout protection.
    Máximo 10 minutos por extração para evitar tasks presas em retry loops.
    """
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
    bundle_name = os.getenv("BUNDLE", "proposicoes")

    for param_name, dependency in DEPENDENCIES.get(
            extractor_name, {}
        ).items():
        if param_name not in resolved_params:
            print(
                f"[runner] Resolving dependency '{param_name}' via '{dependency}'..."
            )

            # Try to load from cache first (S3 or local)
            cached_data = _read_dependency_output(destination, bundle_name, dependency, run_id)
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

    _write_output(data, destination, extractor_name, run_id)

    return {
        "run_id": run_id,
        "extractor": extractor_name,
        "status": "success",
        "records": len(data)
    }


def _read_dependency_output(destination, bundle_name, dependency_name, run_id):
    """
    Try to read dependency output from cache (S3 or local) instead of recomputing.
    Returns None if not found or error reading; caller falls back to recomputation.
    """
    dest_type = destination.get("type", "local")

    try:
        if dest_type == "s3":
            import boto3
            bucket = destination.get("bucket")
            prefix = destination.get("prefix", "").rstrip("/")
            key = (
                f"{prefix}/{dependency_name}_{run_id}.json" if prefix
                else f"{dependency_name}_{run_id}.json"
            )
            s3 = boto3.client("s3")
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode("utf-8")
            data = json.loads(content)
            print(f"[runner] Loaded dependency '{dependency_name}' from cache: {len(data)} records")
            return data

        elif dest_type == "local":
            output_path = Path(destination.get("path", f"/tmp/{bundle_name}/{dependency_name}.json"))
            if output_path.exists():
                with open(output_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                print(f"[runner] Loaded dependency '{dependency_name}' from cache: {len(data)} records")
                return data

    except Exception as e:
        print(f"[runner] Could not load dependency '{dependency_name}' from cache: {e}")
        print(f"[runner] Falling back to recomputation...")

    return None


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
        output_path = Path(destination.get("path", f"/tmp/proposicoes/{extractor_name}.json"))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"[runner] Written {len(data)} records to {output_path}")

    else:
        raise ValueError(f"Unknown destination type: {dest_type}")


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
                "extractor": "proposicoes",
                "params": {},
                "destination": {
                    "type": "local",
                    "path": "/tmp/proposicoes/proposicoes_output.json"
                },
                "run_id": "ecs-test"
            }

    result = handler(event)
    print(json.dumps(result, ensure_ascii=False, indent=2))
