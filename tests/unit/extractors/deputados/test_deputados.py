import pytest
from extractors.camara.deputados.deputados import AsyncDeputadosExtractor

@pytest.mark.asyncio
async def test_extract_deputados_success(mock_client):
    # Setup mock returns
    mock_client.get.side_effect = [
        {'dados': [{'id': 56}]},  # legislaturas list
        {'dados': [{'id': 1, 'nome': 'Deputado 1'}]},  # page 1
        {'dados': []},  # page 2 (empty count 1)
        {'dados': []},  # empty count 2
        {'dados': []},  # empty count 3
        {'dados': []},  # empty count 4 (stop)
    ]

    extractor = AsyncDeputadosExtractor(mock_client)
    result = await extractor.extract(init_legislatura=56, request_tries=4)

    assert len(result) == 1
    assert result[0]['nome'] == 'Deputado 1'
    assert mock_client.get.call_count >= 2

@pytest.mark.asyncio
async def test_extract_deputados_multiple_legislaturas(mock_client):
    mock_client.get.side_effect = [
        {'dados': [{'id': 56}, {'id': 57}]},  # legislaturas
        {'dados': [{'id': 1}]},  # L56 P1
        {'dados': []}, {'dados': []}, {'dados': []}, {'dados': []}, # L56 empty
        {'dados': [{'id': 2}]},  # L57 P1
        {'dados': []}, {'dados': []}, {'dados': []}, {'dados': []}, # L57 empty
    ]

    extractor = AsyncDeputadosExtractor(mock_client)
    # Start from 56 to current (57)
    result = await extractor.extract(init_legislatura=56, request_tries=4)

    assert len(result) == 2
    ids = [d['id'] for d in result]
    assert 1 in ids
    assert 2 in ids
