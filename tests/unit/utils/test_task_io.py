"""Testes do I/O de dependências.

Bug que motivou o módulo: 6 runners gravavam `{nome}.json` mas liam
`{nome}_{run_id}.json`. O cache errava 100% das vezes e cada task dependente
re-extraía o upstream inteiro — 21 misses por execução, queimando quota de uma
API limitada a 10 req/s.
"""
import json

import pytest

from utils import task_io
from utils.task_io import DependencyCacheMiss, cache_path, read_dependency, s3_key, write_output


@pytest.fixture
def dest(tmp_path):
    return {"type": "local", "cache_dir": str(tmp_path)}


def test_write_read_roundtrip(dest):
    data = [{"id": 1, "nome": "acentuação"}, {"id": 2, "nome": "ção"}]
    assert write_output(data, dest, "orgaos", "orgaos", "r1") == 2
    assert read_dependency(dest, "orgaos", "orgaos", "r1") == data


def test_read_and_write_agree_on_path(dest, tmp_path):
    """O bug original: leitura e escrita divergiam no run_id."""
    write_output([{"id": 1}], dest, "orgaos", "orgaos", "run-abc")
    expected = tmp_path / "orgaos" / "orgaos_run-abc.json"
    assert expected.exists()
    assert cache_path("orgaos", "orgaos", "run-abc", str(tmp_path)) == expected


def test_run_id_namespacing_isolates_runs(dest):
    write_output([{"v": "antigo"}], dest, "orgaos", "orgaos", "r1")
    write_output([{"v": "novo"}], dest, "orgaos", "orgaos", "r2")
    assert read_dependency(dest, "orgaos", "orgaos", "r1") == [{"v": "antigo"}]
    assert read_dependency(dest, "orgaos", "orgaos", "r2") == [{"v": "novo"}]


def test_explicit_path_does_not_corrupt_cache_key(dest, tmp_path):
    """`destination["path"]` gera cópia extra, mas nunca move a chave de cache.

    O segundo bug latente: 8 leitores usavam o `path` da task *corrente* como
    caminho da *dependência*.
    """
    explicit = tmp_path / "saida_custom.json"
    dest_with_path = {**dest, "path": str(explicit)}

    write_output([{"id": 1}], dest_with_path, "orgaos", "orgaos", "r1")

    assert explicit.exists()
    assert read_dependency(dest_with_path, "orgaos", "orgaos", "r1") == [{"id": 1}]
    assert (tmp_path / "orgaos" / "orgaos_r1.json").exists()


def test_accepts_iterator_for_streaming(dest):
    """Grava sem materializar tudo — importa para votacoesVotos (~1,1M linhas)."""
    assert write_output(({"i": i} for i in range(5)), dest, "b", "n", "r1") == 5
    assert len(read_dependency(dest, "b", "n", "r1")) == 5


def test_empty_list_produces_valid_json(dest, tmp_path):
    write_output([], dest, "b", "n", "r1")
    assert json.loads((tmp_path / "b" / "n_r1.json").read_text()) == []


def test_strict_mode_raises_on_miss(dest, monkeypatch):
    """Miss deve falhar alto — recomputar em silêncio é o que queimava quota."""
    monkeypatch.setattr(task_io, "STRICT_DEPENDENCY_CACHE", True)
    with pytest.raises(DependencyCacheMiss):
        read_dependency(dest, "orgaos", "inexistente", "r1")


def test_non_strict_mode_returns_none(dest, monkeypatch):
    monkeypatch.setattr(task_io, "STRICT_DEPENDENCY_CACHE", False)
    assert read_dependency(dest, "orgaos", "inexistente", "r1") is None


def test_s3_key_is_stable():
    assert s3_key("orgaos", "votacoes", "r1") == "raw/orgaos/votacoes/votacoes_r1.json"
