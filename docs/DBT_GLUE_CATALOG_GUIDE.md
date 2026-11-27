# Guia: catálogo Glue da camada raw no repositório dbt

Objetivo: o repositório dbt cria e versiona as tabelas Glue da camada `raw/`. A ingestão semanal não tem nenhuma etapa de catálogo.

> **Não use `dbt-external-tables`.** O pacote não lista Athena entre os adapters suportados. Este guia usa uma macro própria.

> **Nada aqui foi executado contra o Athena.** Faça o passo 0 antes de construir o resto.

Placeholders: `<BUCKET>` segue o padrão `<prefixo>-camara-<env>-db`. O database raw é `<BUCKET>-raw` (definido no repo de infra).

## Quando o Glue muda

| Evento | Ação |
|---|---|
| Ingestão semanal (partição nova) | Nenhuma: partition projection |
| Extractor novo ou removido | Atualizar o manifesto e o `sources.yml` |
| Coluna nova, removida ou com outro tipo | Atualizar o `sources.yml` e recriar a tabela |
| Bucket ou caminho mudou | Recriar as tabelas |

## O contrato

`catalog/raw_tables.json`, no repo de ingestão, lista 60 tabelas. O nome é `{bundle}__{name}` e os dados ficam em:

```
raw/{bundle}/{name}/ingestion_date=YYYY-MM-DD/{name}_{run_id}.json   (NDJSON sem compressão)
```

O DAG agenda 56 delas. `deputados__discursos`, `deputados__eventos`, `deputados__historico` e `deputados__mandatos_externos` não são agendadas, então podem não ter dados. Deixe-as por último.

## Passo 0: prova de conceito com uma tabela

Os nomes dos databases têm hífen, e não confirmei na documentação como o Athena e o `dbt-athena` lidam com isso. No console do Athena, em dev:

```sql
CREATE EXTERNAL TABLE `<BUCKET>-raw`.`poc_votacoes` (
  `id` string,
  `data` string,
  `idOrgao` bigint,
  `ultimaApresentacaoProposicao`
    struct<dataHoraRegistro:string,descricao:string,uriProposicaoCitada:string>
)
PARTITIONED BY (`ingestion_date` string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://<BUCKET>/raw/votacoes/votacoes/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.ingestion_date.type'='date',
  'projection.ingestion_date.format'='yyyy-MM-dd',
  'projection.ingestion_date.range'='2025-01-01,NOW',
  'storage.location.template'='s3://<BUCKET>/raw/votacoes/votacoes/ingestion_date=${ingestion_date}/'
);

SELECT ingestion_date, count(*) FROM "<BUCKET>-raw"."poc_votacoes" GROUP BY 1;
CREATE VIEW "<BUCKET>-staging"."poc_view" AS SELECT * FROM "<BUCKET>-raw"."poc_votacoes";
```

Critérios de sucesso:
- [ ] As partições aparecem sem `MSCK REPAIR`.
- [ ] Os campos `idOrgao` e `ultimaApresentacaoProposicao` não vêm nulos quando há valor no JSON.
- [ ] A view funciona. Depois, valide também um `dbt run` com uma view e uma table.

Se os hífens causarem erro, resolva no repo de infra (databases com `_`) antes de continuar. Ao terminar, apague a view e a tabela de teste.

Ajuste o início do `range` para a data da sua primeira carga.

## Passo 1: estrutura do repositório

```
dbt_project.yml
catalog/raw_tables.json          # cópia do repo de ingestão
macros/create_raw_tables.sql
macros/generate_schema_name.sql
models/sources/raw.yml
models/staging/
scripts/infer_columns.py
scripts/check_sources.py
.github/workflows/ci.yml
```

Configuração base:

```yaml
# profiles.yml
camara:
  target: dev
  outputs:
    dev:
      type: athena
      region_name: us-east-1
      database: awsdatacatalog
      schema: "<BUCKET>-staging"
      s3_staging_dir: "s3://<ATHENA_RESULTS_BUCKET>/staging/"
      s3_data_dir: "s3://<BUCKET>/dbt/"
      threads: 4
```

```yaml
# dbt_project.yml (trecho)
vars:
  raw_bucket: "<BUCKET>"
  raw_start_date: "2025-01-01"
```

Por padrão o dbt concatena o schema do target com o schema customizado. Como os databases já existem com nomes fixos, use o nome informado:

```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}{{ target.schema }}
  {%- else -%}{{ custom_schema_name | trim }}{%- endif -%}
{%- endmacro %}
```

Para marts e intermediate, configure `+schema` com o nome completo do database em cada ambiente. Em um ambiente compartilhado, isso faz todos os desenvolvedores gravarem no mesmo schema.

## Passo 2: declarar as sources

Gere um rascunho das colunas a partir de um arquivo real de cada tabela:

```python
# scripts/infer_columns.py  (uso: python scripts/infer_columns.py amostra.json)
import json
import sys


def merge(a, b):
    if a is None:
        return b
    if b is None or a == b:
        return a
    if isinstance(a, dict) and isinstance(b, dict):
        return {k: merge(a.get(k), b.get(k)) for k in {**a, **b}}
    if {a if isinstance(a, str) else "", b if isinstance(b, str) else ""} == {"bigint", "double"}:
        return "double"
    return "string"


def infer(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "bigint"
    if isinstance(value, float):
        return "double"
    if isinstance(value, dict) and value:
        return {k: infer(v) for k, v in value.items()}
    return "string"


def render(t):
    if isinstance(t, dict):
        return "struct<" + ",".join(f"{k}:{render(v)}" for k, v in t.items()) + ">"
    return t or "string"


columns = {}
with open(sys.argv[1], encoding="utf-8") as f:
    for line in f:
        if line.strip():
            for key, value in json.loads(line).items():
                columns[key] = merge(columns.get(key), infer(value))

for name, kind in columns.items():
    print(f'          - name: "{name}"\n            data_type: "{render(kind)}"')
```

```bash
aws s3 ls s3://<BUCKET>/raw/votacoes/votacoes/ --recursive
aws s3 cp s3://<BUCKET>/<chave> /caminho/temporario/amostra.json
python scripts/infer_columns.py /caminho/temporario/amostra.json
```

Revise o rascunho, porque ele reflete só a amostra:

- **Extractors bulk** (dict fixo no código, como `votacoes_from_row`): confira os tipos no código. São os mais estáveis.
- **Extractors que repassam a API** (como `deputados/historico.py`): seja conservador. Na dúvida, use `string` e faça o cast no staging com `try_cast`.
- **IDs:** confira um a um. `votacoes.id` é `string` (`"2458405-38"`), não inteiro.
- **Arrays:** o script os marca como `string`. Teste a leitura. Se precisar do conteúdo estruturado, declare `array<...>` à mão.
- **Objetos declarados como `string`:** valide com uma query que o texto chega como JSON utilizável.

```yaml
# models/sources/raw.yml
version: 2
sources:
  - name: raw
    schema: "<BUCKET>-raw"   # ou monte com var('raw_bucket') ~ '-raw'
    tables:
      - name: votacoes__votacoes
        columns:
          - name: "id"
            data_type: "string"
          - name: "idOrgao"
            data_type: "bigint"
          - name: "ultimaApresentacaoProposicao"
            data_type: "struct<dataHoraRegistro:string,descricao:string,uriProposicaoCitada:string>"
```

Não declare `ingestion_date` em `columns`: a macro a cria como chave de partição.

Sugestão de ordem: migre um bundle por PR.

## Passo 3: macro que cria as tabelas

```sql
-- macros/create_raw_tables.sql
{% macro create_raw_tables(select=none, recreate=true) %}
  {% if execute %}
    {% set ns = namespace(count=0) %}
    {% for src in graph.sources.values() if src.source_name == 'raw' %}
      {% if select is none or src.name == select %}
        {% if not src.columns %}
          {{ exceptions.raise_compiler_error('Source sem colunas: ' ~ src.name) }}
        {% endif %}

        {% set parts = src.name.split('__') %}
        {% set location = 's3://' ~ var('raw_bucket') ~ '/raw/' ~ parts[0] ~ '/' ~ parts[1] ~ '/' %}
        {% set relation = '`' ~ src.schema ~ '`.`' ~ src.identifier ~ '`' %}

        {% set cols = [] %}
        {% for col in src.columns.values() %}
          {% if not col.data_type %}
            {{ exceptions.raise_compiler_error(src.name ~ '.' ~ col.name ~ ' sem data_type') }}
          {% endif %}
          {% do cols.append('`' ~ col.name ~ '` ' ~ col.data_type) %}
        {% endfor %}

        {% if recreate %}
          {% do run_query('DROP TABLE IF EXISTS ' ~ relation) %}
        {% endif %}

        {% set ddl %}
CREATE EXTERNAL TABLE IF NOT EXISTS {{ relation }} (
  {{ cols | join(',\n  ') }}
)
PARTITIONED BY (`ingestion_date` string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '{{ location }}'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.ingestion_date.type'='date',
  'projection.ingestion_date.format'='yyyy-MM-dd',
  'projection.ingestion_date.range'='{{ var("raw_start_date") }},NOW',
  'storage.location.template'='{{ location }}ingestion_date=${ingestion_date}/'
)
        {% endset %}

        {% do run_query(ddl) %}
        {% do log('tabela raw aplicada: ' ~ src.name, info=true) %}
        {% set ns.count = ns.count + 1 %}
      {% endif %}
    {% endfor %}
    {% if ns.count == 0 %}
      {{ exceptions.raise_compiler_error('Nenhuma source raw selecionada') }}
    {% endif %}
  {% endif %}
{% endmacro %}
```

```bash
dbt run-operation create_raw_tables --args '{select: votacoes__votacoes}'
dbt run-operation create_raw_tables
```

- **Por que `recreate=true`:** `CREATE ... IF NOT EXISTS` ignora mudanças de coluna em uma tabela que já existe.
- **É seguro para os dados:** em uma tabela externa como esta, o `DROP TABLE` remove só o metadado. Confirme no passo 0 e nunca aponte a macro para tabelas Iceberg.
- **Efeito colateral:** a tabela fica ausente por alguns instantes. Não rode o deploy junto com um `dbt build` agendado.
- **JSON malformado:** não ligue `ignore.malformed.json`. O plano original sugeria ligar, mas isso pode esconder dados corrompidos. É melhor a query falhar.

## Passo 4: modelos de staging

Cada execução é um **snapshot completo**. Sem filtro, os dados se repetem a cada semana. Uma reexecução na mesma data com outro `run_id` também deixa dois arquivos na partição.

Pegue o arquivo mais recente da partição mais recente:

```sql
-- models/staging/stg_camara__votacoes.sql
{{ config(materialized='table') }}

with source as (
    select
        *,
        "$path" as _file_path,
        "$file_modified_time" as _file_modified_at
    from {{ source('raw', 'votacoes__votacoes') }}
),

latest_file as (
    select _file_path
    from source
    order by ingestion_date desc, _file_modified_at desc
    limit 1
)

select
    source.id as votacao_id,
    source.idorgao as orgao_id,
    source.ultimaapresentacaoproposicao.descricao as ultima_apresentacao_descricao,
    source.ingestion_date,
    source._file_path
from source
inner join latest_file on source._file_path = latest_file._file_path
```

- **Use `table`, não `view`:** o Athena não aceita `"$file_modified_time"` em views.
- **Não ordene só por `"$path"`:** `scheduled__...` sempre vem depois de `manual__...`.
- **Custo:** a query lê todo o histórico da tabela. Em tabelas grandes (CEAP/`despesas`), filtre antes, por exemplo `where ingestion_date >= date_format(current_date - interval '21' day, '%Y-%m-%d')`. Atenção: se não houver carga nesse intervalo, o resultado fica vazio.
- **Histórico:** se precisar dele, crie um modelo incremental que deduplica por `ingestion_date`.
- **Dados parciais:** o arquivo mais recente pode ser menor que o anterior, porque alguns extractors aceitam cobertura parcial. Teste o volume.

Testes mínimos: `unique` e `not_null` na chave de cada staging (confirme antes qual é a chave real) e um teste de volume mínimo. Para frescor, faça um teste singular que falhe quando `max(ingestion_date)` tiver mais de 8 dias.

## Passo 5: CI

### 5.1 Checar o contrato

Copie o `catalog/raw_tables.json` para o repo dbt. O script abaixo exige correspondência total. Durante a migração por bundle, rode-o com `--allow-missing`.

```python
# scripts/check_sources.py
import json
import sys

import yaml

with open("catalog/raw_tables.json", encoding="utf-8") as f:
    expected = {t["table"] for t in json.load(f)["tables"]}
with open("models/sources/raw.yml", encoding="utf-8") as f:
    raw = next(s for s in yaml.safe_load(f)["sources"] if s["name"] == "raw")
declared = {t["name"] for t in raw["tables"]}

unknown = declared - expected
missing = expected - declared
print("fora do manifesto:", sorted(unknown))
print("sem source:", sorted(missing))
sys.exit(1 if unknown or (missing and "--allow-missing" not in sys.argv) else 0)
```

Para detectar uma cópia desatualizada, adicione um job que compare o arquivo com o original (o repo de ingestão é público). Ele só funciona depois que o manifesto chegar à branch usada:

```bash
curl -fsSL https://raw.githubusercontent.com/<OWNER>/camara-senado-data-ingestion/<BRANCH>/catalog/raw_tables.json \
  | diff - catalog/raw_tables.json
```

Use `develop` para o ambiente dev e `main` para prod.

### 5.2 Workflow

| Momento | Passos |
|---|---|
| PR | `dbt deps` → `check_sources.py` → `dbt parse` |
| Merge em `develop` | `create_raw_tables` (dev) → `dbt build` (dev) |
| Merge em `main` | `create_raw_tables` (prod) → `dbt build` (prod) |
| Semanal | `dbt build`, **sem** `create_raw_tables` |

A ingestão de prod começa no domingo às 06:00 UTC. Agende o `dbt build` com folga, com base na duração real das execuções, e mantenha o teste de frescor como proteção.

### 5.3 IAM

Crie, no repo de infra, um role OIDC para o repo dbt, um por ambiente, com acesso mínimo:

- **Athena:** executar e acompanhar queries no workgroup do projeto.
- **Glue:** ler databases, tabelas e partições. Criar, alterar e apagar tabelas **somente** nos databases do projeto. A exclusão é necessária por causa do `recreate`.
- **S3:** leitura em `raw/`, sem escrita nem exclusão. Leitura e escrita nos prefixos do dbt e dos resultados do Athena.

Não verifiquei se o repo de infra já tem um workgroup do Athena ou um bucket de resultados. Confira antes. Se o Lake Formation estiver ativo, serão necessárias permissões adicionais.

## Passo 6: quando o schema mudar

A API pode mudar sem nenhum commit. Com o OpenX SerDe, uma coluna declarada que sumiu do JSON tende a virar `NULL` sem erro, e um campo novo é ignorado.

- Adicione testes `not_null` nas colunas importantes.
- Opcionalmente, rode um job semanal que compare o resultado do `infer_columns.py` na amostra mais recente com o `raw.yml` e avise sobre diferenças.
- Para corrigir: PR no `raw.yml`, e o CI recria a tabela.

## Checklist

- [ ] No repo de ingestão, o manifesto está commitado e com merge feito
- [ ] Passo 0 validado em dev, inclusive os hífens
- [ ] Role OIDC, workgroup e bucket de resultados prontos
- [ ] Macro testada com uma tabela
- [ ] `votacoes__votacoes` completa: source, staging e testes
- [ ] Demais bundles migrados, um por PR
- [ ] `--allow-missing` removido do CI
- [ ] Após uma nova ingestão, a partição aparece sem nenhum deploy
- [ ] Deploy em prod

## Referências

- [dbt-external-tables: bancos suportados](https://github.com/dbt-labs/dbt-external-tables/blob/main/README.md)
- [PR do adapter Athena (fechado)](https://github.com/dbt-labs/dbt-external-tables/pull/133)
- [Athena: tipos de partition projection](https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html)
- [Athena: regras de nomes](https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html)
- [Athena: limitações de views](https://docs.aws.amazon.com/athena/latest/ug/considerations-limitations-views.html)
- [dbt: setup do Athena](https://docs.getdbt.com/docs/core/connect-data-platform/athena-setup)
