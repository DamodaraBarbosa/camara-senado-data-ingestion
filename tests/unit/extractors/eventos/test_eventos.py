import pytest
from extractors.camara.eventos.eventos import AsyncEventosExtractor

@pytest.mark.asyncio
async def test_extract_eventos_success(mock_client):
    # Setup mock returns
    # 1. legislaturas return
    # 2. _fetch_pages through mock_client.get
    mock_client.get.side_effect = [
        {'dados': [{'id': 56}]},  # legislaturas
        {'dados': [{'id': 1, 'nome': 'Evento 1'}]},  # page 1
        {'dados': []},  # page 2 (empty count 1)
        {'dados': []},  # empty count 2
        {'dados': []},  # empty count 3
        {'dados': []},  # empty count 4 (stop)
    ]

    extractor = AsyncEventosExtractor(mock_client)
    result = await extractor.extract(init_legislatura=56, request_tries=4)

    assert len(result) == 1
    assert result[0]['id'] == 1
    assert result[0]['idLegislatura'] == 56

@pytest.mark.asyncio
async def test_extract_eventos_empty(mock_client):
    mock_client.get.side_effect = [
        {'dados': [{'id': 56}]},
        {'dados': []},  # page 1
        {'dados': []},
        {'dados': []},
        {'dados': []},
    ]

    extractor = AsyncEventosExtractor(mock_client)
    result = await extractor.extract(init_legislatura=56, request_tries=4)

    assert len(result) == 0
