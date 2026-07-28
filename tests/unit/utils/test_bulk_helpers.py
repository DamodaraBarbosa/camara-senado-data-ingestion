"""Testes dos helpers de transformação de linhas bulk.

O risco silencioso desta migração é de tipo: o CSV devolve tudo como string e
usa `""` onde a API devolve `null`. Uma contagem de registros nunca detecta
isso — só asserção de tipo detecta.
"""
import pytest

from utils.bulk import (
    id_from_uri,
    normalize_cnpj,
    nullify,
    to_float,
    to_int,
    unflatten,
)


class TestNullify:
    def test_empty_string_becomes_none(self):
        assert nullify("") is None
        assert nullify("   ") is None

    def test_preserves_content(self):
        assert nullify("PLEN") == "PLEN"
        assert nullify("  PLEN  ") == "PLEN"

    def test_none_stays_none(self):
        assert nullify(None) is None


class TestToInt:
    def test_converts(self):
        assert to_int("123") == 123
        assert to_int(" 456 ") == 456

    def test_empty_and_garbage_become_none(self):
        assert to_int("") is None
        assert to_int("não é número") is None
        assert to_int(None) is None

    def test_zero_is_preserved_not_nulled(self):
        # `aprovacao` e `votosSim` são legitimamente 0; confundir com None
        # inverteria o significado do dado.
        assert to_int("0") == 0


class TestToFloat:
    def test_ponto_decimal(self):
        assert to_float("899.50") == 899.50

    def test_virgula_decimal_pt_br(self):
        assert to_float("1.234,56") == 1234.56
        assert to_float("899,50") == 899.50

    def test_invalid_becomes_none(self):
        assert to_float("") is None
        assert to_float("R$ x") is None


class TestIdFromUri:
    def test_extracts_trailing_id(self):
        assert id_from_uri("https://dadosabertos.camara.leg.br/api/v2/orgaos/4") == 4
        assert id_from_uri("https://x/deputados/178937") == 178937

    def test_tolerates_trailing_slash(self):
        assert id_from_uri("https://x/orgaos/180/") == 180

    def test_non_numeric_tail_becomes_none(self):
        # orgaosDeputados só traz URIs; se o formato mudar, precisamos detectar
        # em vez de emitir ids nulos silenciosamente.
        assert id_from_uri("https://x/orgaos/abc") is None
        assert id_from_uri("") is None


class TestNormalizeCnpj:
    def test_strips_formatting(self):
        # CEAP formata; a API devolve só dígitos.
        assert normalize_cnpj("085.324.290/0013-1") == "08532429000131"

    def test_already_bare_is_unchanged(self):
        assert normalize_cnpj("08532429000131") == "08532429000131"

    def test_empty_becomes_none(self):
        assert normalize_cnpj("") is None


class TestUnflatten:
    def test_dot_separator_after_underscore(self):
        # frentesDeputados usa `deputado_.id`. O candidato `deputado_` casaria
        # antes e deixaria `.id` como chave — daí a ordenação por
        # especificidade.
        row = {"deputado_.id": "7", "deputado_.nome": "X", "outro": "y"}
        assert unflatten(row, "deputado") == {"id": "7", "nome": "X"}

    def test_underscore_separator(self):
        # votacoesVotos usa `deputado_id`.
        assert unflatten({"deputado_id": "7"}, "deputado") == {"id": "7"}

    def test_dot_separator(self):
        # eventos usa `localCamara.nome`.
        row = {"localCamara.nome": "Plenário", "localCamara.sala": ""}
        assert unflatten(row, "localCamara") == {"nome": "Plenário", "sala": None}

    def test_ignores_unrelated_keys(self):
        assert unflatten({"id": "1", "titulo": "T"}, "deputado") == {}


class TestToFk:
    """FKs usam `0` como sentinela de nulo nos arquivos bulk; a API devolve null."""

    def test_zero_becomes_none(self):
        from utils.bulk import to_fk
        assert to_fk("0") is None

    def test_real_id_preserved(self):
        from utils.bulk import to_fk
        assert to_fk("67053") == 67053

    def test_empty_becomes_none(self):
        from utils.bulk import to_fk
        assert to_fk("") is None

    def test_to_int_still_preserves_zero(self):
        """Contraste deliberado: em aprovacao/votosSim o zero é legítimo."""
        assert to_int("0") == 0
