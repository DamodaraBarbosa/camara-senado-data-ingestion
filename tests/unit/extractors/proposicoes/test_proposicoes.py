import pytest
from unittest.mock import AsyncMock
from datetime import datetime
from extractors.camara.proposicoes.proposicoes import AsyncProposicoesExtractor

@pytest.mark.asyncio
async def test_extract_proposicoes_by_year(mock_client):
    current_year = datetime.now().year

    # Mocking:
    mock_client.get = AsyncMock(return_value={'dados': [{'id': 56, 'dataInicio': f'{current_year}-02-01'}]})
    mock_client.get_all_pages = AsyncMock(return_value=[{'id': 101, 'ementa': 'Projeto 1'}])

    extractor = AsyncProposicoesExtractor(mock_client)
    result = await extractor.extract(init_legislatura=56)

    assert len(result) == 1
    assert result[0]['id'] == 101
    assert mock_client.get.called
    assert mock_client.get_all_pages.called
