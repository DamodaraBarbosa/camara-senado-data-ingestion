import pytest
from unittest.mock import AsyncMock
from extractors.camara.deputados.deputados import AsyncDeputadosExtractor

@pytest.mark.asyncio
async def test_extract_deputados_success(mock_client):
    # Setup mock returns
    mock_client.get = AsyncMock(return_value={'dados': [{'id': 56}]})
    mock_client.get_all_pages = AsyncMock(return_value=[{'id': 1, 'nome': 'Deputado 1'}])

    extractor = AsyncDeputadosExtractor(mock_client)
    result = await extractor.extract(init_legislatura=56)

    assert len(result) == 1
    assert result[0]['nome'] == 'Deputado 1'
    assert mock_client.get.called
    assert mock_client.get_all_pages.called

@pytest.mark.asyncio
async def test_extract_deputados_multiple_legislaturas(mock_client):
    mock_client.get = AsyncMock(return_value={'dados': [{'id': 56}, {'id': 57}]})
    mock_client.get_all_pages = AsyncMock(side_effect=[
        [{'id': 1}],  # legislatura 56
        [{'id': 2}],  # legislatura 57
    ])

    extractor = AsyncDeputadosExtractor(mock_client)
    # Start from 56 to current (57)
    result = await extractor.extract(init_legislatura=56)

    assert len(result) == 2
    ids = [d['id'] for d in result]
    assert 1 in ids
    assert 2 in ids
