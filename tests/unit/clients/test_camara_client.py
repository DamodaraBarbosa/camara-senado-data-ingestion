import pytest
import aiohttp
from clients.camara_client import AsyncCamaraClient

@pytest.mark.asyncio
async def test_client_get_empty_endpoint():
    client = AsyncCamaraClient()
    async with aiohttp.ClientSession() as session:
        with pytest.raises(ValueError, match="The endpoint parameter must be not empty."):
            await client.get(session, "")

@pytest.mark.asyncio
async def test_client_semaphore_exists():
    client = AsyncCamaraClient()
    assert client.semaphore is not None
    # Test property cache
    sem1 = client.semaphore
    sem2 = client.semaphore
    assert sem1 is sem2
