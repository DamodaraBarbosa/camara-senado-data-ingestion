"""Regressão do crash observado em produção na run de 2026-09-05.

Um único HTTP 504 em 37 requisições derrubava o extractor inteiro:

    [eventos/pauta] cobertura 97.3% (36/37) — falhas: CamaraServerError=1
    File "/app/src/extractors/camara/eventos/pauta.py", line 26, in extract
    AttributeError: 'NoneType' object has no attribute 'get'

`gather_aligned` devolve `None` na posição que falhou — está no seu docstring —
e o chamador consumia esse `None` direto com `.get()`.
"""
import pytest

from extractors.camara.eventos import pauta as pauta_module
from extractors.camara.eventos.pauta import AsyncEventosPautaExtractor
from utils.concurrency import InsufficientData

EVENTOS = [{"id": 1}, {"id": 2}, {"id": 3}]


def _fake_gather(aligned, coverage, errors):
    async def _gather(coros, *, label, deadline=None):
        for coro in coros:
            coro.close()          # evita "coroutine was never awaited"
        return aligned, coverage, errors
    return _gather


async def test_survives_a_failed_request(mock_client, monkeypatch):
    """36 de 37 é aproveitável: mantém o que veio e marca `partial`."""
    monkeypatch.setattr(pauta_module, "gather_aligned", _fake_gather(
        [{"dados": [{"nome": "A"}]}, None, {"dados": [{"nome": "C"}]}],
        coverage=2 / 3,
        errors=[RuntimeError("HTTP 504")],
    ))
    monkeypatch.setattr("utils.concurrency.MIN_COVERAGE", 0.5)

    extractor = AsyncEventosPautaExtractor(mock_client)
    result = await extractor.extract(eventos=EVENTOS)

    assert [r["nome"] for r in result] == ["A", "C"]
    assert [r["idEvento"] for r in result] == [1, 3]   # o None nao desalinha os ids
    assert extractor.partial is True


async def test_ids_stay_aligned_when_the_first_request_fails(mock_client, monkeypatch):
    """O ponto de `gather_aligned`: filtrar antes do zip desalinharia tudo."""
    monkeypatch.setattr(pauta_module, "gather_aligned", _fake_gather(
        [None, {"dados": [{"nome": "B"}]}, {"dados": [{"nome": "C"}]}],
        coverage=2 / 3,
        errors=[RuntimeError("HTTP 504")],
    ))
    monkeypatch.setattr("utils.concurrency.MIN_COVERAGE", 0.5)

    result = await AsyncEventosPautaExtractor(mock_client).extract(eventos=EVENTOS)

    assert [r["idEvento"] for r in result] == [2, 3]


async def test_degraded_coverage_fails_instead_of_writing_a_subset(mock_client, monkeypatch):
    """Tolerar sem limite grava um subconjunto marcado como `success`."""
    monkeypatch.setattr(pauta_module, "gather_aligned", _fake_gather(
        [{"dados": [{"nome": "A"}]}, None, None],
        coverage=1 / 3,
        errors=[RuntimeError("HTTP 504")] * 2,
    ))

    with pytest.raises(InsufficientData, match="abaixo do minimo"):
        await AsyncEventosPautaExtractor(mock_client).extract(eventos=EVENTOS)
