import pytest
from datetime import datetime
from extractors.camara.proposicoes.proposicoes import AsyncProposicoesExtractor

@pytest.mark.asyncio
async def test_extract_proposicoes_by_year(mock_client):
    current_year = datetime.now().year
    
    # Mocking:
    # 1. legislaturas (to get dataInicio)
    # 2. Year data (only current year for simplicity in this mock)
    mock_client.get.side_effect = [
        {'dados': [{'id': 56, 'dataInicio': f'{current_year}-02-01'}]}, # legislatura lookup
        {'dados': [{'id': 101, 'ementa': 'Projeto 1'}]},             # current year P1
        {'dados': []}, {'dados': []}, {'dados': []}, {'dados': []},    # end of year
    ]

    extractor = AsyncProposicoesExtractor(mock_client)
    result = await extractor.extract(init_legislatura=56, request_tries=4)

    assert len(result) == 1
    assert result[0]['id'] == 101

    # Check if correct params were sent (ano should be current_year)
    # Using ANY for the session argument
    mock_client.get.assert_any_call(ANY, 'proposicoes', params=DictSubset({'ano': current_year}))

# Custom matcher for dict subset since requests params vary
from unittest.mock import ANY

class DictSubset:
    def __init__(self, subset):
        self.subset = subset
    def __eq__(self, other):
        return all(item in other.items() for item in self.subset.items())

pytest.dict_is_subset = DictSubset
