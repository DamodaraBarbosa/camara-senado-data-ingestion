#!/usr/bin/env python3
"""Compara o resultado dos extractors bulk com o endpoint de detalhe da API.

Ferramenta de desenvolvimento, não parte do pipeline. Serve para responder a
pergunta que uma contagem de registros nunca responde: *os campos batem?*

O risco silencioso da migração é de tipo e de nome — o CSV devolve strings onde
a API devolve inteiros, e renomeia campos (`ultimoStatus_idSituacao` vs
`codSituacao`). Este script pega uma amostra pequena e diffa campo a campo.

Uso:
    venv/bin/python scripts/compare_bulk_vs_api.py votacoes --sample 20
    venv/bin/python scripts/compare_bulk_vs_api.py proposicoes --sample 10
"""
import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

import aiohttp  # noqa: E402

from clients.camara_bulk_client import CamaraBulkClient  # noqa: E402
from clients.camara_client import AsyncCamaraClient  # noqa: E402

# Campos que sabidamente não existem nos arquivos bulk (perda aceita) ou que só
# existem neles (ganho aditivo). Não são reportados como divergência.
KNOWN_BULK_ONLY = {
    "votacoes": {"votosSim", "votosNao", "votosOutros", "idLegislatura",
                 "ultimaAberturaVotacao", "ultimaApresentacaoProposicao"},
    "proposicoes": {"uriAutores"},
}
KNOWN_API_ONLY = {
    "votacoes": {"efeitosRegistrados"},
    "proposicoes": {"justificativa", "texto"},
}


def compare(api_obj: dict, bulk_obj: dict, known_bulk_only, known_api_only):
    api_keys, bulk_keys = set(api_obj), set(bulk_obj)
    report = {
        "keys_only_in_api": sorted(api_keys - bulk_keys - known_api_only),
        "keys_only_in_bulk": sorted(bulk_keys - api_keys - known_bulk_only),
        "value_mismatches": [],
    }

    for key in sorted(api_keys & bulk_keys):
        api_val, bulk_val = api_obj[key], bulk_obj[key]
        if _equivalent(api_val, bulk_val):
            continue
        report["value_mismatches"].append(
            {"campo": key, "api": _truncate(api_val), "bulk": _truncate(bulk_val)}
        )
    return report


def _equivalent(a, b):
    """Igualdade tolerante a coerção de tipo, mas não a diferença de conteúdo."""
    if a == b:
        return True
    if a in (None, "", [], {}) and b in (None, "", [], {}):
        return True
    # A API pode devolver int onde o CSV traz "123" já convertido — e vice-versa.
    if isinstance(a, (int, float)) or isinstance(b, (int, float)):
        try:
            return float(a) == float(b)
        except (TypeError, ValueError):
            return False
    if isinstance(a, str) and isinstance(b, str):
        return a.strip() == b.strip()
    return False


def _truncate(value, limit=80):
    text = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
    return text if len(text) <= limit else text[:limit] + "…"


async def compare_votacoes(sample: int):
    from extractors.camara.votacoes.ids import AsyncVotacoesIdsExtractor

    bulk_client = CamaraBulkClient()
    api = AsyncCamaraClient()

    bulk_rows = await AsyncVotacoesIdsExtractor(api, bulk_client=bulk_client).extract()
    subset = bulk_rows[:sample]

    reports = []
    async with aiohttp.ClientSession() as session:
        for row in subset:
            payload = await api.get(session, f"votacoes/{row['id']}")
            api_obj = payload.get("dados") or {}
            if not api_obj:
                continue
            reports.append(compare(
                api_obj, row, KNOWN_BULK_ONLY["votacoes"], KNOWN_API_ONLY["votacoes"]
            ))
    await bulk_client.close()
    return reports


async def compare_proposicoes(sample: int):
    from extractors.camara.proposicoes.ids import AsyncIdsExtractor

    bulk_client = CamaraBulkClient()
    api = AsyncCamaraClient()

    bulk_rows = await AsyncIdsExtractor(api, bulk_client=bulk_client).extract()
    subset = bulk_rows[:sample]

    reports = []
    async with aiohttp.ClientSession() as session:
        for row in subset:
            payload = await api.get(session, f"proposicoes/{row['id']}")
            api_obj = payload.get("dados") or {}
            if not api_obj:
                continue
            reports.append(compare(
                api_obj, row, KNOWN_BULK_ONLY["proposicoes"], KNOWN_API_ONLY["proposicoes"]
            ))
    await bulk_client.close()
    return reports


def summarize(reports, alvo: str) -> int:
    from collections import Counter

    only_api, only_bulk, mismatches = Counter(), Counter(), Counter()
    for report in reports:
        only_api.update(report["keys_only_in_api"])
        only_bulk.update(report["keys_only_in_bulk"])
        mismatches.update(m["campo"] for m in report["value_mismatches"])

    print(f"\n=== paridade bulk vs API: {alvo} ({len(reports)} amostras) ===")
    for titulo, counter in (
        ("campos só na API (perda inesperada)", only_api),
        ("campos só no bulk (ganho inesperado)", only_bulk),
        ("valores divergentes", mismatches),
    ):
        if counter:
            print(f"\n{titulo}:")
            for campo, n in counter.most_common():
                print(f"  {campo:40s} {n}/{len(reports)}")
        else:
            print(f"\n{titulo}: nenhum")

    for report in reports:
        for m in report["value_mismatches"][:2]:
            print(f"\n  exemplo [{m['campo']}]\n    api : {m['api']}\n    bulk: {m['bulk']}")
        if report["value_mismatches"]:
            break

    return 1 if (only_api or mismatches) else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("alvo", choices=["votacoes", "proposicoes"])
    parser.add_argument("--sample", type=int, default=20)
    args = parser.parse_args()

    runner = {"votacoes": compare_votacoes, "proposicoes": compare_proposicoes}[args.alvo]
    reports = asyncio.run(runner(args.sample))
    return summarize(reports, args.alvo)


if __name__ == "__main__":
    sys.exit(main())
