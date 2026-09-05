"""`grupos/membros` achatava errado e produzia NDJSON ilegível.

A linha usava `all_membros.append(membros_data)` — anexando a *lista* — enquanto
`grupos/historico`, de forma idêntica, usa `extend`. Efeito na saída de produção
de 2026-08-30: cada linha do arquivo era um array JSON, que o SerDe do Glue não
lê; a contagem de registros contava grupos em vez de membros; e grupo sem membro
produzia uma linha `[]`.
"""
from extractors.camara.grupos import membros as membros_module
from extractors.camara.grupos.membros import AsyncGruposMembrosExtractor

GRUPOS = [{"id": 10}, {"id": 20}]


def _fake_gather(aligned):
    async def _gather(coros, *, label, deadline=None):
        for coro in coros:
            coro.close()
        return aligned, 1.0, []
    return _gather


async def test_each_membro_is_its_own_record(mock_client, monkeypatch):
    monkeypatch.setattr(membros_module, "gather_aligned", _fake_gather([
        {"dados": [{"nome": "A"}, {"nome": "B"}]},
        {"dados": [{"nome": "C"}]},
    ]))

    result = await AsyncGruposMembrosExtractor(mock_client).extract(grupos=GRUPOS)

    # Achatado: 3 membros, nao 2 grupos. Cada item e um dict, nunca uma lista.
    assert len(result) == 3
    assert all(isinstance(item, dict) for item in result)
    assert [r["idGrupo"] for r in result] == [10, 10, 20]


async def test_a_group_with_no_members_adds_no_line(mock_client, monkeypatch):
    """Antes, um grupo vazio virava a linha `[]` no NDJSON."""
    monkeypatch.setattr(membros_module, "gather_aligned", _fake_gather([
        {"dados": []},
        {"dados": [{"nome": "C"}]},
    ]))

    result = await AsyncGruposMembrosExtractor(mock_client).extract(grupos=GRUPOS)

    assert result == [{"nome": "C", "idGrupo": 20}]
