"""Shared time budget across extractors.

The runner wraps each extraction in ``asyncio.wait_for(..., timeout=600)``. If an
extractor consumes all 600s, the ``TimeoutError`` kills the process and
*all* work already done is lost (that's what happened with
``votacoes/votacoes``: 601s, zero records saved).

So extractors work against a smaller budget, returning partial data before the hard limit.
The value used to be copied as ``540`` across 10 files; 540s left only 60s margin,
insufficient when a single batch overruns. 480s gives 120s real buffer.
"""
import os
import time

# Hard limit of the handler (asyncio.wait_for in runners).
HARD_TIMEOUT_S = 600

# Extractor budget, with margin to write output before the hard limit.
TASK_BUDGET_S = float(os.getenv("CAMARA_TASK_BUDGET_S", 480))

# Minimum budget checks per extraction. The ``batch_size`` is reduced to fit
# this many batches, ensuring the check never becomes dead code — the bug in
# ``votacoes/votacoes``, where batch_size=50 over 14 periods produced a single
# iteration and the check ran once with elapsed~=0.
MIN_CHECKPOINTS = 4


class Deadline:
    """Monotonic deadline for an extraction."""

    def __init__(self, budget_s: float = None):
        self.budget_s = TASK_BUDGET_S if budget_s is None else budget_s
        self.start = time.monotonic()

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.start

    @property
    def remaining(self) -> float:
        return self.budget_s - self.elapsed

    @property
    def expired(self) -> bool:
        return self.remaining <= 0

    def __repr__(self):
        return f"<Deadline elapsed={self.elapsed:.0f}s remaining={self.remaining:.0f}s>"


def clamp_batch_size(batch_size: int, total: int, min_checkpoints: int = MIN_CHECKPOINTS) -> int:
    """Reduz ``batch_size`` para garantir ao menos ``min_checkpoints`` lotes.

    Sem isso, um batch_size maior que o total produz uma única iteração e a
    checagem de orçamento nunca roda de verdade.
    """
    if total <= 0:
        return max(1, batch_size)
    ceiling = -(-total // min_checkpoints)  # ceil division
    return max(1, min(batch_size, ceiling))


def task_budget_s() -> float:
    """Orçamento corrente, lido do ambiente a cada chamada.

    Diferente da constante ``TASK_BUDGET_S`` (fixada no import), esta função
    enxerga o valor que o runner define em ``CAMARA_TASK_BUDGET_S`` a partir do
    seu próprio timeout. Sem isso, um extractor com override de 3600s ainda
    seria abortado no orçamento default de 480s.
    """
    return float(os.getenv("CAMARA_TASK_BUDGET_S", TASK_BUDGET_S))


_task_deadline = None


def task_deadline():
    """Deadline única do processo, criada na primeira chamada.

    Cada task ECS roda um extractor num processo próprio, então um deadline por
    processo é exatamente o escopo certo: mede desde o primeiro uso até o
    limite do runner, atravessando downloads e parses.
    """
    global _task_deadline
    if _task_deadline is None:
        _task_deadline = Deadline(task_budget_s())
    return _task_deadline


def reset_task_deadline():
    """Zera o deadline do processo. Existe para os testes."""
    global _task_deadline
    _task_deadline = None
