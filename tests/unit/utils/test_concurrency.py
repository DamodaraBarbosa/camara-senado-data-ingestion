"""Testes da política de cobertura.

`gather_aligned` tolera falhas individuais de propósito — uma requisição ruim
não deve derrubar uma extração de 600. Mas tolerar sem limite transforma a
extração num subconjunto silencioso: o runner marca `status: "partial"`, o
container sai com código 0, e o Airflow registra `success`. Dado incompleto
indistinguível de dado completo é a mesma classe de problema que o arquivo
vazio de `scheduled__2026-08-23`.

`assert_usable` é onde essa linha é traçada.
"""
import pytest

from utils import concurrency
from utils.concurrency import InsufficientData, assert_usable, gather_aligned

LABEL = "bundle/extractor"


# --------------------------------------------------------------------------
# assert_usable
# --------------------------------------------------------------------------

def test_full_coverage_passes():
    assert_usable([{"id": 1}], coverage=1.0, errors=[], label=LABEL)


def test_empty_without_errors_is_legitimate():
    """Zero registro sem nenhuma falha é ausência de dado, não falha.

    Quem barra esse caso é `task_io._guard_empty`, que conhece o par
    (bundle, extractor). Aqui não há informação para distinguir.
    """
    assert_usable([], coverage=1.0, errors=[], label=LABEL)


def test_empty_with_errors_raises():
    """O caso `proposicoes/tipos_autor`: a API caiu e a saída seria `[]`."""
    with pytest.raises(InsufficientData, match="nenhum registro obtido"):
        assert_usable([], coverage=0.0, errors=[RuntimeError("timeout")], label=LABEL)


def test_isolated_failure_is_tolerated():
    """O caso real: `eventos/pauta` com 36 de 37 (97,3%) na run de 2026-09-05.

    Perder a pauta de um evento em 37 não justifica descartar os outros 36.
    """
    assert_usable([{"id": 1}], coverage=36 / 37, errors=[RuntimeError("504")], label=LABEL)


def test_coverage_below_threshold_raises():
    with pytest.raises(InsufficientData, match="abaixo do minimo"):
        assert_usable(
            [{"id": 1}], coverage=0.80, errors=[RuntimeError("504")] * 5, label=LABEL
        )


def test_explicit_minimum_overrides_the_default():
    """Um extractor que não tolera lacuna nenhuma pode exigir cobertura total."""
    assert_usable([{"id": 1}], coverage=0.97, errors=[], label=LABEL)  # passa no default

    with pytest.raises(InsufficientData, match="abaixo do minimo"):
        assert_usable([{"id": 1}], coverage=0.97, errors=[], label=LABEL, minimum=1.0)


def test_small_batches_reject_any_failure():
    """Em `blocos/partidos` são 4 requisições: 3 de 4 é 75%, e reprova.

    Um percentual se comporta de forma diferente conforme a escala. Aqui isso
    joga a favor: perder 1 de 4 blocos é uma lacuna grande.
    """
    with pytest.raises(InsufficientData):
        assert_usable([{"id": 1}], coverage=0.75, errors=[RuntimeError()], label=LABEL)


def test_threshold_is_configurable_by_env(monkeypatch):
    monkeypatch.setattr(concurrency, "MIN_COVERAGE", 0.5)
    assert_usable([{"id": 1}], coverage=0.6, errors=[RuntimeError()], label=LABEL)


# --------------------------------------------------------------------------
# gather_aligned: o contrato que os 10 extractors quebravam
# --------------------------------------------------------------------------

async def test_gather_aligned_puts_none_where_the_request_failed():
    """Contrato documentado, e a razão de existir a guarda nos chamadores."""
    async def ok(value):
        return value

    async def boom():
        raise RuntimeError("HTTP 504")

    aligned, coverage, errors = await gather_aligned(
        [ok("a"), boom(), ok("c")], label=LABEL
    )

    assert aligned == ["a", None, "c"]
    assert coverage == pytest.approx(2 / 3)
    assert len(errors) == 1
