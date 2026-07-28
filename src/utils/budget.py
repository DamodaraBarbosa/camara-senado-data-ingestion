"""Orçamento de tempo compartilhado entre extractors.

O runner envolve cada extração em ``asyncio.wait_for(..., timeout=600)``. Se um
extractor consumir os 600s inteiros, o ``TimeoutError`` mata o processo e
*todo* o trabalho já feito é perdido (foi o que aconteceu com
``votacoes/votacoes``: 601s, zero registros salvos).

Por isso os extractors trabalham contra um orçamento menor, devolvendo dados
parciais antes do limite duro. O valor vivia copiado como ``540`` em 10
arquivos; 540s deixava apenas 60s de margem, insuficiente quando um único lote
estoura. 480s dá 120s reais de folga.
"""
import os
import time

# Limite duro do handler (asyncio.wait_for nos runners).
HARD_TIMEOUT_S = 600

# Orçamento dos extractors, com margem para escrever a saída antes do limite duro.
TASK_BUDGET_S = float(os.getenv("CAMARA_TASK_BUDGET_S", 480))

# Mínimo de checagens de orçamento por extração. O ``batch_size`` é reduzido até
# caber esta quantidade de lotes, garantindo que a checagem nunca vire código
# morto — o bug de ``votacoes/votacoes``, onde batch_size=50 sobre 14 períodos
# produzia uma única iteração e a checagem rodava uma vez com elapsed~=0.
MIN_CHECKPOINTS = 4


class Deadline:
    """Prazo monotônico para uma extração."""

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
