# Auditoria de produção — 2026-09-02

Levantamento do estado real do fluxo de extração/ingestão da Câmara na AWS, feito contra
a conta `904464083417` (us-east-1), os dois repositórios e os últimos 10 merges em
`develop`/`main` de `camara-senado-data-ingestion` e `camara-senado-data-infra`.

**Motivação:** a modelagem com dbt exige cargas semanais confiáveis. A pergunta era
"falta algo mandatório em produção que impeça o fluxo?". Resposta: o fluxo roda, mas não
é confiável o suficiente, e a camada que o dbt consome não existe.

**Escopo executado:** apenas a Sprint 0 (ver adiante). Todo o resto está registrado aqui
para consulta posterior. O recorte de infraestrutura está em
`camara-senado-data-infra/AUDITORIA_PRODUCAO_2026-09-02.md`.

> **Status em 2026-09-03 03:30 UTC — Sprint 0 concluída e verificada na AWS.** Os sete
> itens entraram em produção. O que muda daqui para frente: a imagem de produção deixou de
> estar congelada, uma falha passa a gerar e-mail, e uma sobrescrita acidental no S3 passa a
> ser recuperável. O primeiro teste real é a run de **domingo 2026-09-06 06:00 UTC** — o
> primeiro disparo autônomo que o host de produção executa de ponta a ponta. Os detalhes
> verificados estão em cada achado abaixo e no checklist da Sprint 0.

---

## O que já estava certo

- Terraform de infra: `main` == `develop`, CI "Terraform Prod" verde e aplicando.
  Buckets, 16 Glue DBs, ECR + lifecycle, cluster ECS, task definition
  `dataplatform-ingestion-task-prod:2`, roles e OIDC — tudo existe.
- IAM completo e correto: `dataplatform_airflow_ec2` tem `iam:PassRole` para os quatro
  roles (incluindo os de prod); `dataplatform_airflow_prod` tem
  `dataplatform-s3-read-write-prod`.
- Bucket prod com SSE-AES256 e Public Access Block completo.
- Host `i-0e11709bd1c1dae07`: `.env` presente e válido, `docker compose config` OK,
  `postgres-airflow`/`scheduler`/`triggerer` up há 47h, git em `d100aaa`,
  137MB de RAM disponível, load 0.14, DAG de prod **despausada**, zero erros de import.
- Execução deferrable + triggerer: comprovada, 56/56 tasks em 42 min.

---

## Achados

### P0-1 — Deploy de produção quebrado há um mês, reportando verde  ✅ CORRIGIDO (Sprint 0)

`camara-ingestion:prod` apontava para `prod-f30b4398` (commit de **2026-07-31**, imagem
empurrada em 2026-08-21), enquanto `main` estava em `d100aaa` (2026-09-01) — **30 commits
à frente**. Nenhuma tag `prod-*` posterior existia no ECR.

Causa raiz, no log do job (run `33466065098`):

```
Changes will be detected between develop and main
git merge-base refs/remotes/origin/develop refs/remotes/origin/main
Detected 0 changed files
Filter deploy = false
```

`dorny/paths-filter@v3` sem `base:` usa o **branch default do repositório** — que aqui é
`develop`, não o `before` SHA do push. Como `main` é sempre fast-forward de `develop`, o
merge-base é o próprio HEAD: zero arquivos alterados, sempre. O job "Deploy (production)"
pulava build e push e terminava **verde**, em 25 segundos.

Impacto: `7d57b10 fix: make the bulk CSV parse cancellable, and the timeouts honest`
(10 runners + `src/clients/camara_bulk_client.py` + `src/utils/budget.py`, 277 linhas)
estava em `main` e **não rodava em produção**. Toda run semanal executava código de julho.

**Correção aplicada:** `base: ${{ github.event.before }}` nos dois jobs de deploy em
`.github/workflows/ci.yml` (PR #62 → #63, `main` em `843aabb`).

**Verificado.** O log do job "Deploy (production)" no push do merge em `main`
(run `33709736625`) mostra o comportamento novo:

```
Changes will be detected between d100aaaef189d2ad6c24e32b2981c53dba851834 and main
Detected 5 changed files
Filter deploy = false
```

Compare com o antigo — `between develop and main`, `Detected 0 changed files`. Agora ele
compara contra o SHA anterior de `main` e enxerga os arquivos de verdade; `deploy = false`
aqui é a decisão **correta**, porque nenhum dos 5 (`ci.yml`, a DAG, o compose, o runbook e
este `.md`) casa com `bundles/**`, `src/**`, `Dockerfile` ou `requirements.txt`.

**Imagem republicada** via `workflow_dispatch` (run `33711264029`, todos os jobs verdes,
Deploy (production) construindo e empurrando de fato):

```
prod, prod-843aabbc   sha256:0cf17c31bd08...   70 MB   2026-09-03 03:25:40 UTC
```

O sufixo bate com o `main` HEAD (`843aabbc31d9c55233c6feb8dc66c9ec7065e7e7`) e as duas tags
apontam para o mesmo digest. A task definition referencia a tag mutável `:prod`, então a
próxima task Fargate já sobe com esta imagem — **`7d57b10` finalmente roda em produção**.

### P0-2 — Um rerun sobrescreve dado bom com vazio, e reporta sucesso  ⚠️ PARCIAL

Na run `scheduled__2026-08-23` (executada 2026-09-01), **4 datasets terminaram com `[]`**
(2 bytes) apesar das 56 tasks estarem `success`:

| chave S3 | 08-09 / 08-16 | 08-23 |
|---|---|---|
| `raw/votacoes/votacoes/` | 40.785.717 B (42.050 reg.) | **2 B** |
| `raw/partidos/ids/` | 22.792 B | **2 B** |
| `raw/proposicoes/tipos_autor/` | 4.644 B | **2 B** |
| `raw/proposicoes/tipos_tramitacao/` | 21.199 B | **2 B** |

O CloudWatch mostra as duas escritas na **mesma chave**, na mesma dagrun:

```
[runner] 42050 registros gravados em .../raw/votacoes/votacoes/votacoes_scheduled__2026-08-23...json (5 partes)
[runner]     0 registros gravados em .../raw/votacoes/votacoes/votacoes_scheduled__2026-08-23...json (1 partes)
```

`head-object` confirma `LastModified 2026-09-01T02:14:32Z`, `ContentLength 2`.

Três defeitos somados:

1. `src/utils/task_io.py::_write_s3` escreve numa chave **determinística por `run_id`** —
   a retry (`retries: 1`) sobrescreve o resultado bom da tentativa anterior.
2. Os runners marcam `status: success` **independente da contagem de registros**, então
   0 registros é indistinguível de sucesso.
3. O bucket **não tinha versionamento** — o dado sobrescrito era irrecuperável.

**Feito na Sprint 0:** versionamento ligado (rede de segurança). Confirmado na AWS —
`get-bucket-versioning` retorna `Enabled` em `dataplatform-camara-prod-db` e
`dataplatform-senado-prod-db`. A partir de agora, uma sobrescrita como a de 08-23 deixa a
versão boa recuperável via `list-object-versions` em vez de destruí-la.

**Pendente (Sprint 1):** escrita idempotente e contagem-zero-vira-falha — a **causa**
continua intacta. Ver backlog.

### P0-3 — Zero alertas  ✅ CORRIGIDO (Sprint 0)

- `aws cloudwatch describe-alarms` → **zero alarmes**
- `aws sns list-topics` → **zero tópicos**
- `email_on_failure: False` no `DEFAULT_ARGS`, sem SMTP configurado no compose de prod
- retenção de `/ecs/dataplatform-ingestion-task-prod` era de **7 dias** — menor que o
  intervalo entre duas execuções semanais, então o post-mortem de uma falha já expirava
  antes da run seguinte

Com cadência semanal, uma falha silenciosa custa uma semana inteira de dados.

**Correção aplicada e verificada ponta a ponta:**

| elo | estado |
|---|---|
| tópico | `arn:aws:sns:us-east-1:904464083417:dataplatform-alerts-prod` criado |
| assinatura | confirmada (`SubscriptionArn` = `...:6de0ee52-...`, não mais `PendingConfirmation`) |
| permissão | `sns:Publish` na policy inline `dataplatform-airflow-ec2-ecs` |
| callback | `notify_failure` por task e por dagrun |
| variável no container | `CAMARA_ALERT_SNS_TOPIC_ARN` presente no `airflow-scheduler-1` |
| retenção do log group | `7` → `30` dias |

O e-mail de confirmação da AWS caiu no **spam** — vale marcar `no-reply@sns.amazonaws.com`
como remetente confiável. Um alerta que vai para o spam é funcionalmente idêntico a não ter
alerta nenhum, que é exatamente o problema que este item veio resolver.

> **Correção a um erro do primeiro diagnóstico.** Eu havia reportado que a run de
> `2026-08-30` "nunca aconteceu e foi perdida para sempre". **Isso estava errado.** O
> Airflow 2 cria o dagrun só depois que o intervalo de dados fecha: o
> `scheduled__2026-08-23` (data interval 08-23 → 08-30) *é* a execução disparada em 30/08.
> Ela começou em 2026-09-01T02:11 porque o host estava fora do ar, e terminou com sucesso
> às 02:53. `airflow dags details` confirma `next_dagrun 2026-08-30`,
> `next_dagrun_create_after 2026-09-06`. Nada foi perdido. O que continua verdadeiro é que
> **não havia como saber** — e que **domingo 2026-09-06 06:00 UTC é o primeiro disparo
> autônomo que o host de produção executa de ponta a ponta**: o metadata DB tem
> exatamente uma dagrun de prod.

### P0-4 — Host do Airflow: single point of failure fora do IaC  ❌ PENDENTE

- Instância, security group, role, instance profile, swapfile, `.env` e os dois crons
  existem **apenas** como checklist manual em `docs/PROD_AIRFLOW_EC2_RUNBOOK.md`. O
  diretório `modules/` do repo de infra está vazio.
- Metadata DB (Postgres com o estado de pause das DAGs e o histórico) num volume Docker
  no EBS da instância, **sem snapshot nem backup**.
- `t3.micro`, 913MB. Margem medida na run bem-sucedida: **31MB de RAM livre no pico**,
  807MB de swap.
- Sem CloudWatch agent → **não existe métrica de memória ou disco** para essa instância,
  exatamente as duas que a derrubaram (5 OOM-kills entre 30 e 31/08, host inacessível
  por 4 dias).
- Sem alarme de auto-recovery do EC2.

**Status:** continua **pendente** — nada deste achado foi resolvido na Sprint 0, exceto a
verificação do landmine abaixo. A instância segue fora do IaC, sem snapshot do metadata DB,
sem métrica de memória e sem auto-recovery. É o item de maior risco em aberto: se o host
morrer numa sexta, a run de domingo não acontece e a recuperação é manual.

**Landmine que estava ativo e foi verificado:** `9437f56` passou a exigir
`AIRFLOW_FERNET_KEY`, `AIRFLOW_ADMIN_PASSWORD` e `POSTGRES_PASSWORD` com sintaxe
`${VAR:?}` no compose de produção, e o cron `git pull` já havia trazido esse arquivo para
o host. Se `airflow/.env` não existisse, o próximo `up -d`/reboot falharia por completo.
**Verificado via SSM em 2026-09-03: o arquivo existe, com as 4 chaves, e
`docker compose config` passa.** Sem ação necessária.

---

## O que falta para o dbt (não bloqueia a ingestão; bloqueia a modelagem)

O alvo natural é **dbt-athena** — os 16 Glue databases
`dataplatform-{camara,senado}-{dev,prod}-db_{raw,staging,intermediate,marts}` já existem
em `environments/prod/main.tf` exatamente com essa forma. Mas:

**D-1. O formato dos arquivos é ilegível para o Athena.** `_write_s3` grava um array JSON
*pretty-printed*:

```
[
  {"id": "584", "uri": "https://...", "nome": "..."},
  {"id": "585", ...}
]
```

O JSON SerDe do Glue/Athena (Hive e OpenX) exige **um objeto JSON completo por linha**,
sem array externo e sem vírgula no fim da linha. Nenhuma tabela sobre esses arquivos
funciona. Correção: emitir **NDJSON** — em `_write_s3`, trocar `"[\n  "` / `",\n  "` /
`"\n]"` por `json_str + "\n"` (e `""` em vez de `"[]"` no caso vazio). Alternativa mais
cara e mais rápida de consultar: converter para Parquet num passo posterior.

**D-2. Zero tabelas e zero crawlers.** `get-tables` retorna 0 nos 16 databases;
`list-crawlers` retorna vazio. Nada registra o `raw/` no catálogo. Preferir DDL versionada
a crawler — o schema é conhecido e estável.

**D-3. Não há particionamento; todas as semanas caem no mesmo prefixo.** O layout é
`raw/{bundle}/{extractor}/{extractor}_{run_id}.json`, com o `run_id` só no *nome do
arquivo*. Uma tabela sobre `raw/votacoes/votacoes/` hoje já faria `UNION` de 4 execuções,
e cresce uma por semana. Para carga incremental semanal, o layout precisa ser
Hive-particionado: `raw/{bundle}/{extractor}/ingestion_date=YYYY-MM-DD/`.

**D-4. Athena sem configuração.** Só existe o workgroup `primary`, com
`ResultConfiguration` **vazio** — não há `s3_staging_dir` para o `profiles.yml` do dbt,
nem workgroup dedicado, nem limite de bytes escaneados. Somado a arquivos únicos de
150–240 MB em JSON, cada `dbt run` full-refresh varre o dataset inteiro.

**Ordem recomendada:** D-1 e D-3 primeiro — os dois mudam o layout do `raw/`, então quanto
antes, menos histórico para reprocessar. Só depois D-2 e D-4.

---

## Itens P2 (não bloqueiam, mas cobram juros)

- Sem lifecycle policy no bucket prod; ~600 MB brutos por semana acumulando em Standard
  indefinidamente.
- `NETWORK_CONFIGURATION` em `airflow/dags/camera_ingestion_dag.py` tem 6 subnets e 1
  security group **hardcoded**, compartilhados entre dev e prod, com
  `assignPublicIp: ENABLED`. Sem isolamento de rede entre ambientes.
- Variáveis mortas de EMR em `environments/{dev,prod}/variables.tf`
  (`emr_release_label`, `cluster_*`) — nenhum recurso EMR é criado.
- `docs/PROD_AIRFLOW_EC2_RUNBOOK.md` passo 7 ainda instrui login `airflow`/`airflow` na
  UI, já contradito pelo passo 7b.
- `airflow/docker-compose-airflow.prod.yml` ainda declara `version: '3.8'`, obsoleto —
  o docker compose no host emite warning a cada invocação.
- O venv local do repo é Python 3.8.10, abaixo da matriz do CI (3.10/3.11/3.12): 8 testes
  de `tests/unit/clients/test_camara_bulk_client.py` falham localmente por isso, e passam
  no CI. Vale alinhar o venv para o ambiente local parar de dar falso negativo.

---

## Backlog

### Sprint 0 — concluída em 2026-09-03

Todos os itens verificados contra a AWS, não apenas mergeados.

- [x] Diagnóstico read-only do host via SSM — `.env` presente e válido, três containers up,
      DAG despausada, sem erro de import. O landmine do `.env` não existia.
- [x] `base:` no `dorny/paths-filter` dos dois jobs de deploy — comprovado no log do CI
- [x] `on_failure_callback` → SNS na DAG, por task e por dagrun
- [x] `CAMARA_ALERT_SNS_TOPIC_ARN` no compose **e no container** — o scheduler e o triggerer
      foram recriados via SSM (`up -d --force-recreate scheduler triggerer`), preservando o
      `postgres-airflow`, que é o container cujo volume guarda o metadata DB sem snapshot.
      Sem esse recreate a variável ficaria só no arquivo: o cron de sync só faz `git pull`.
- [x] Tópico SNS + assinatura **confirmada**, versionamento `Enabled` nos dois buckets prod,
      retenção 30 dias — `Plan: 4 to add, 1 to change, 0 to destroy`
- [x] `sns:Publish` na policy inline do role da instância + runbook corrigido
- [x] Republicação de `camara-ingestion:prod` → `prod-843aabbc`, digest `sha256:0cf17c31...`

**Merges:** ingestão `843aabb` (PRs #62, #63), infra `6e8fd47` (PRs #28, #29). Zero drift
entre `develop` e `main` nos dois repositórios.

**O que observar na segunda-feira 2026-09-07**, depois da primeira run autônoma:

1. Os 56 objetos com `run_id` `scheduled__2026-08-30` apareceram em
   `s3://dataplatform-camara-prod-db/raw/`?
2. Algum veio com 2 bytes? Se sim, é a confirmação de que P0-2 continua mordendo — com a
   diferença de que agora `list-object-versions` permite recuperar a versão boa.
3. Chegou algum e-mail do `dataplatform-alerts-prod`? Se a run falhou e **não** chegou
   e-mail, o problema é o canal, não o pipeline.

### Sprint 1 — confiabilidade da carga

- [ ] **Escrita idempotente** em `src/utils/task_io.py::_write_s3`: gravar numa chave
      temporária e só promover para a canônica no `complete_multipart_upload` bem-sucedido,
      recusando a promoção quando `count == 0` e já existir objeto não-vazio na chave.
- [ ] **Contagem zero vira falha** nos runners (`bundles/*/app/runner.py`), ou
      `status: "empty"` explícito, para os extractors que nunca devem ser vazios.
- [ ] Alarme CloudWatch "sem carga nova há mais de 8 dias" (task final na DAG publicando
      `PutMetricData`, alarme sobre essa métrica).
- [ ] Alarme de auto-recovery do EC2 + CloudWatch agent (memória e disco).
- [ ] **Re-executar `scheduled__2026-08-23`** para recuperar os 4 datasets zerados —
      só depois de P0-2 corrigido, senão o rerun repete o problema.

### Sprint 2 — habilitar o dbt

- [ ] D-1: `_write_s3` → NDJSON.
- [ ] D-3: layout `raw/{bundle}/{extractor}/ingestion_date=YYYY-MM-DD/`.
- [ ] D-2: tabelas externas no Glue (`..._raw`) via DDL versionada.
- [ ] D-4: workgroup Athena dedicado com `ResultConfiguration` e limite de bytes;
      `profiles.yml` apontando para `..._staging` / `..._intermediate` / `..._marts`.

### Sprint 3 — endurecer

- [ ] Host do Airflow para Terraform (`modules/airflow_host/` no repo de infra).
- [ ] Snapshot DLM do EBS da instância (metadata DB do Airflow).
- [ ] Lifecycle policy no bucket prod.
- [ ] Isolamento de rede dev/prod (VPC/subnets/SG próprios, sem hardcode na DAG).
