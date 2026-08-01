import pytest
from unittest.mock import AsyncMock
from extractors.camara.eventos.eventos import AsyncEventosExtractor

@pytest.mark.asyncio
async def test_extract_eventos_success(mock_client):
    # Setup mock returns
    mock_client.get = AsyncMock(return_value={'dados': [{'id': 56}]})
    mock_client.get_all_pages = AsyncMock(return_value=[{'id': 1, 'nome': 'Evento 1'}])

    extractor = AsyncEventosExtractor(mock_client)
    result = await extractor.extract(init_legislatura=56)

    assert len(result) == 1
    assert result[0]['id'] == 1
    assert result[0]['idLegislatura'] == 56

@pytest.mark.asyncio
async def test_extract_eventos_empty(mock_client):
    mock_client.get = AsyncMock(return_value={'dados': [{'id': 56}]})
    mock_client.get_all_pages = AsyncMock(return_value=[])

    extractor = AsyncEventosExtractor(mock_client)
    result = await extractor.extract(init_legislatura=56)

    assert len(result) == 0
