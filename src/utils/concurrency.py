"""Concurrent execution with coverage tracking and deadline respect.

Two problems motivate this module:

1. 36 sites used ``asyncio.gather(*tasks)`` without ``return_exceptions=True``:
   one request exhausting retries kills the extractor.
2. Those using ``return_exceptions=True`` swallowed failures without checking.
   ``votacoes/votacoes`` had 10 of 14 periods failing with HTTP 504 and still
   reported ``SUCCESS``.

``gather_with_coverage`` solves both: never propagates one member's failure, but
returns coverage so the caller can decide if the result is acceptable.
"""
import asyncio
from collections import Counter


class InsufficientData(RuntimeError):
    """No data obtained despite errors — never write empty output."""


async def gather_with_coverage(
    coros,
    *,
    label: str,
    deadline=None,
    cancel_pending: bool = True,
):
    """Execute coroutines concurrently, tolerating individual failures.

    Unlike ``asyncio.gather(..., return_exceptions=True)``, respects a
    deadline: what doesn't finish in time is canceled, but what already finished
    is retained. That's exactly what was missing when a ``votacoes`` batch
    ran 601s and discarded 569s of completed work.

    Args:
        coros: Coroutines to execute.
        label: Log prefix, e.g. ``"votacoes"``.
        deadline: Optional ``utils.budget.Deadline`` limiting wait.
        cancel_pending: Cancel what didn't finish within the deadline.

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
