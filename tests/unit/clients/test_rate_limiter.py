"""Testes do rate limiter e do circuit breaker.

Contexto: a medição mostrou que 2 tasks concorrentes rendiam 2,04x MENOS que
execução serial, com 6x mais 429s. A causa era o sleep de backoff acontecer
fora do semáforo, criando um pool ilimitado de corrotinas que voltavam todas
juntas. O breaker substitui esse backoff individual por uma pausa global única.
"""
import asyncio
import time

import pytest

from clients.camara_client import CircuitBreaker, RateLimiter


@pytest.mark.asyncio
async def test_rate_limiter_paces_requests():
    """N aquisições devem levar pelo menos N/rate segundos."""
    limiter = RateLimiter(rate=20, burst=1)
    start = time.monotonic()
    for _ in range(10):
        await limiter.acquire()
    elapsed = time.monotonic() - start
    assert elapsed >= 0.4  # ~9 esperas de 1/20s após o burst inicial


@pytest.mark.asyncio
async def test_rate_limiter_burst_is_immediate():
    """O burst inicial não deve ser retardado."""
    limiter = RateLimiter(rate=1, burst=5)
    start = time.monotonic()
    for _ in range(5):
        await limiter.acquire()
    assert time.monotonic() - start < 0.1


@pytest.mark.asyncio
async def test_breaker_pauses_all_waiters():
    """Um único 429 pausa todos os workers, não cada um por si."""
    breaker = CircuitBreaker()
    await breaker.wait()
    breaker.trip(0.3)

    start = time.monotonic()
    await asyncio.gather(*(breaker.wait() for _ in range(20)))
    elapsed = time.monotonic() - start

    assert 0.25 < elapsed < 0.9


@pytest.mark.asyncio
async def test_trip_extends_but_never_shortens():
    """Um trip menor durante uma pausa maior não pode encurtá-la."""
    breaker = CircuitBreaker()
    await breaker.wait()
    breaker.trip(0.5)
    breaker.trip(0.1)

    start = time.monotonic()
    await breaker.wait()
    assert time.monotonic() - start > 0.4


@pytest.mark.asyncio
async def test_trip_extends_with_longer_pause():
    breaker = CircuitBreaker()
    await breaker.wait()
    breaker.trip(0.2)
    await asyncio.sleep(0.05)
    breaker.trip(0.5)

    start = time.monotonic()
    await breaker.wait()
    assert time.monotonic() - start > 0.35


@pytest.mark.asyncio
async def test_breaker_reopens_and_is_reusable():
    breaker = CircuitBreaker()
    breaker.trip(0.1)
    await breaker.wait()

    start = time.monotonic()
    await breaker.wait()  # já reaberto: retorno imediato
    assert time.monotonic() - start < 0.05
