"""Execução concorrente com cobertura mínima e respeito a prazo.

Dois problemas motivam este módulo:

1. 36 sites usavam ``asyncio.gather(*tasks)`` sem ``return_exceptions=True``:
   uma requisição que esgota os retries mata o extractor inteiro.
2. Os que usavam ``return_exceptions=True`` engoliam as falhas sem checar nada.
   ``votacoes/votacoes`` teve 10 dos 14 períodos falhando com HTTP 504 e ainda
   assim reportou ``SUCCESS``.

``gather_with_coverage`` resolve os dois: nunca propaga falha de um membro, mas
devolve a cobertura para o chamador decidir se o resultado é aceitável.
"""
import asyncio
from collections import Counter


class InsufficientData(RuntimeError):
    """Nenhum dado obtido apesar de erros — nunca gravar saída vazia."""


async def gather_with_coverage(
    coros,
    *,
    label: str,
    deadline=None,
    cancel_pending: bool = True,
):
    """Executa corrotinas concorrentemente, tolerando falhas individuais.

    Diferente de ``asyncio.gather(..., return_exceptions=True)``, respeita um
    prazo: o que não terminar a tempo é cancelado e o que já terminou é
    aproveitado. Era exatamente o que faltava quando um lote de ``votacoes``
    rodou 601s e descartou 569s de trabalho concluído.

    Args:
        coros: corrotinas a executar.
        label: prefixo dos logs, ex. ``"votacoes"``.
        deadline: ``utils.budget.Deadline`` opcional limitando a espera.
        cancel_pending: cancelar o que não terminou dentro do prazo.

    Returns:
        ``(resultados_ok, cobertura, erros)`` — cobertura em [0.0, 1.0].
    """
    tasks = [asyncio.ensure_future(c) for c in coros]
    if not tasks:
        return [], 1.0, []

    timeout = None
    if deadline is not None:
        timeout = max(0.0, deadline.remaining)

    done, pending = await asyncio.wait(tasks, timeout=timeout)

    if pending and cancel_pending:
        for task in pending:
            task.cancel()
        # Aguarda o cancelamento efetivo para não deixar tarefas órfãs.
        await asyncio.gather(*pending, return_exceptions=True)

    results, errors = [], []
    for task in tasks:
        if task not in done:
            continue
        exc = task.exception()
        if exc is not None:
            errors.append(exc)
        else:
            results.append(task.result())

    total = len(tasks)
    coverage = len(results) / total if total else 1.0

    if errors or pending:
        histogram = Counter(type(e).__name__ for e in errors)
        if pending:
            histogram["TimedOut"] = len(pending)
        summary = ", ".join(f"{name}={count}" for name, count in histogram.most_common())
        print(
            f"[{label}] cobertura {coverage:.1%} "
            f"({len(results)}/{total}) — falhas: {summary}"
        )
        for exc in errors[:3]:
            print(f"[{label}]   exemplo: {type(exc).__name__}: {exc}")

    return results, coverage, errors


async def gather_aligned(
    coros,
    *,
    label: str,
    deadline=None,
):
    """Como ``gather_with_coverage``, mas preservando índices.

    Devolve uma lista do mesmo tamanho da entrada, com ``None`` onde houve
    falha ou estouro de prazo. Necessário para os chamadores que fazem
    ``zip(ids, results)`` ou ``enumerate(results)`` — filtrar os resultados ali
    desalinharia silenciosamente cada registro do seu id.

    Returns:
        ``(resultados_alinhados, cobertura, erros)``
    """
    tasks = [asyncio.ensure_future(c) for c in coros]
    if not tasks:
        return [], 1.0, []

    timeout = max(0.0, deadline.remaining) if deadline is not None else None
    done, pending = await asyncio.wait(tasks, timeout=timeout)

    if pending:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    aligned, errors = [], []
    for task in tasks:
        if task not in done:
            aligned.append(None)
            continue
        exc = task.exception()
        if exc is not None:
            aligned.append(None)
            errors.append(exc)
        else:
            aligned.append(task.result())

    ok = sum(1 for item in aligned if item is not None)
    coverage = ok / len(tasks)

    if errors or pending:
        histogram = Counter(type(e).__name__ for e in errors)
        if pending:
            histogram["TimedOut"] = len(pending)
        summary = ", ".join(f"{name}={count}" for name, count in histogram.most_common())
        print(f"[{label}] cobertura {coverage:.1%} ({ok}/{len(tasks)}) — falhas: {summary}")
        for exc in errors[:3]:
            print(f"[{label}]   exemplo: {type(exc).__name__}: {exc}")

    return aligned, coverage, errors


def assert_usable(records: list, coverage: float, errors: list, *, label: str):
    """Impede gravar saída vazia quando houve erro.

    Zero registros sem nenhum erro é um resultado legítimo (não há dados no
    período). Zero registros *com* erros significa que a extração falhou e
    gravar o arquivo vazio faria o downstream ler isso como "sem dados".
    """
    if not records and errors:
        raise InsufficientData(
            f"[{label}] nenhum registro obtido com {len(errors)} falha(s); "
            f"cobertura {coverage:.1%}"
        )
