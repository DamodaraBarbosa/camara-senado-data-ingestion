"""Testes do parse dos arquivos bulk.

Princípio: **mockar o download, nunca o parse.** O leitor CSV real, a
verificação de cabeçalho real e o transform real rodam em todos os testes — são
justamente eles que podem quebrar silenciosamente quando o arquivo upstream
muda.
"""
import shutil
from pathlib import Path

import pytest

from clients.camara_bulk_client import (
    DATASETS,
    BulkSchemaChanged,
    CamaraBulkClient,
    _content_range_total,
)

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "bulk"


@pytest.fixture
def bulk_client(tmp_path, monkeypatch):
    """Cliente real com apenas o download substituído por cópia de fixture."""
    client = CamaraBulkClient(cache_dir=tmp_path)

    async def fake_ensure_file(dataset, partition=None):
        dest = client.local_path(dataset, partition)
        shutil.copy(FIXTURES / dest.name, dest)
        return dest

    monkeypatch.setattr(client, "ensure_file", fake_ensure_file)
    return client


@pytest.mark.asyncio
async def test_strips_bom_from_first_column(bulk_client):
    """UTF-8 BOM não pode virar parte do nome da primeira coluna."""
    rows = await bulk_client.read_rows("votacoesOrientacoes", 2025)
    assert "idVotacao" in rows[0]
    assert not any(key.startswith("﻿") for key in rows[0])


@pytest.mark.asyncio
async def test_parses_embedded_delimiter(bulk_client):
    """`;` dentro de aspas não pode quebrar a linha — o caso que um split() ingênuo erra."""
    rows = await bulk_client.read_rows("votacoesOrientacoes", 2025)
    assert ";" in rows[1]["descricao"]
    assert rows[1]["orientacao"] == "Sim"


@pytest.mark.asyncio
async def test_parses_embedded_newline(bulk_client):
    """Quebra de linha dentro de aspas é um único registro, não dois."""
    rows = await bulk_client.read_rows("votacoesOrientacoes", 2025)
    assert len(rows) == 3
    assert "\n" in rows[2]["descricao"]


@pytest.mark.asyncio
async def test_row_filter_and_transform(bulk_client):
    rows = await bulk_client.read_rows(
        "votacoesOrientacoes", 2025,
        row_filter=lambda r: r["siglaOrgao"] == "PLEN",
        transform=lambda r: {"b": r["siglaBancada"]},
    )
    assert rows == [{"b": "Maioria"}, {"b": "PT"}]


@pytest.mark.asyncio
async def test_missing_required_column_raises(bulk_client, tmp_path, monkeypatch):
    """Rename upstream deve falhar alto, não gerar um arquivo de None."""
    bad = tmp_path / "votacoesOrientacoes-2025.csv"
    bad.write_text(
        "﻿\"idVotacao\";\"siglaOrgao\"\n\"1\";\"PLEN\"\n", encoding="utf-8"
    )

    async def fake_ensure_file(dataset, partition=None):
        return bad

    monkeypatch.setattr(bulk_client, "ensure_file", fake_ensure_file)

    with pytest.raises(BulkSchemaChanged, match="orientacao"):
        await bulk_client.read_rows("votacoesOrientacoes", 2025)


def test_content_range_total():
    assert _content_range_total("bytes 0-0/12345") == 12345
    assert _content_range_total(None) is None
    assert _content_range_total("bytes 0-0/*") is None


def test_dataset_url_construction():
    assert DATASETS["votacoes"].url(2025).endswith("/votacoes-2025.csv")
    assert DATASETS["orgaosDeputados"].url(57).endswith("/orgaosDeputados-L57.csv")
    # Dataset não particionado ignora o argumento.
    assert DATASETS["frentesDeputados"].url().endswith("/frentesDeputados.csv")


def test_partitioned_dataset_requires_partition():
    with pytest.raises(ValueError, match="partition"):
        DATASETS["votacoes"].url()
