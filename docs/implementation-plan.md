# Implementation Plan

## Objetivo

Transformar o `apex-lakehouse` de scaffold arquitetural e documental em uma plataforma operável de ingestão, modelagem, serving e observabilidade para dados públicos de fundos no Brasil.

## Princípios de execução

- Construir em ordem de dependência real, não em ordem de visibilidade.
- Fechar um fluxo vertical mínimo antes de expandir horizontalmente para novas fontes.
- Materializar contratos, quality e replay desde o primeiro fluxo.
- Tratar `staging` como alvo de verdade e ambiente local como sandbox.
- Não abrir novos workstreams enquanto o anterior não tiver critério de saída verificável.

## Fases de execução

### Fase 0: Foundation Setup

Objetivo: sair do repositório documental para um repositório executável.

Escopo:

- criar `pyproject.toml` com dependências e ferramentas base;
- configurar `ruff`, `mypy`, `pytest` e `pre-commit`;
- adicionar `Makefile` ou `justfile` com comandos padrão;
- criar `.env.example`;
- estruturar configuração compartilhada para ambientes local, staging e prod;
- adicionar `docker-compose.yml` para sandbox local.

Critérios de saída:

- ambiente local sobe sem intervenção manual fora de um fluxo documentado;
- lint, typing e testes conseguem rodar localmente;
- configurações sensíveis ficam fora do código.

### Fase 1: Control Plane e Ingestão CVM

Objetivo: fechar o primeiro fluxo vertical de ingestão incremental.

Escopo:

- implementar `source_file_registry`, `ingestion_state` e `pipeline_run_log`;
- criar conectores CVM para informe diário e cadastro de fundos;
- persistir payloads em `raw`;
- registrar hash, data de negócio, origem e status por arquivo;
- criar DAGs do Airflow para coleta e replay;
- implementar parser inicial para `bronze`.

Critérios de saída:

- reexecução da mesma competência é idempotente;
- novos arquivos são detectados corretamente;
- falhas de ingestão ficam auditáveis por `pipeline_run_id`.

### Fase 2: Silver, Contracts e Quarantine

Objetivo: estabilizar a camada conformada para o primeiro domínio funcional.

Escopo:

- modelar `silver.fundos` e `silver.fundos_informe_diario`;
- normalizar CNPJ, datas e valores monetários;
- implementar deduplicação por chave de negócio;
- criar regras críticas em Great Expectations;
- materializar `quarantine.*` e `ops.data_quality_results`;
- publicar documentação de contratos por dataset.

Critérios de saída:

- registros inválidos não alcançam `silver`;
- regras críticas ficam rastreáveis por execução;
- contratos de grain e schema do domínio CVM ficam explícitos.

### Fase 3: Gold e MVP Operacional

Objetivo: entregar o primeiro produto de dados consultável.

Escopo:

- modelar `gold.dim_fundo`, `gold.dim_tempo` e `gold.fato_fundo_diario`;
- calcular rentabilidade diária, captação líquida e variação de patrimônio;
- criar endpoints `GET /health`, `GET /ops/pipelines/latest` e `GET /quality/latest`;
- criar dashboard operacional mínimo em Superset;
- publicar runbook de replay e falha comum.

Critérios de saída:

- o fluxo CVM roda ponta a ponta até `gold`;
- endpoints operacionais respondem com dados reais;
- dashboard operacional mostra freshness, volume e falha.

### Fase 4: Analytical MVP

Objetivo: expandir o produto com contexto macroeconômico e serving analítico.

Escopo:

- adicionar ingestão BCB para Selic e IPCA;
- adicionar ingestão B3 para índices prioritários;
- modelar `gold.fato_macro_diaria` e `gold.fato_indice_mercado`;
- criar endpoints `GET /funds/{cnpj}/performance`, `GET /funds/{cnpj}/flows`, `GET /funds/ranking` e `GET /market/macro/{serie}`;
- montar dashboard de mercado com ranking, captação e correlação.

Critérios de saída:

- BI e API consomem a mesma camada `gold`;
- métricas analíticas reconciliam com a origem;
- o projeto entrega um MVP utilizável por consumidor não técnico.

### Fase 5: Production Hardening

Objetivo: endurecer a operação para `staging` e `prod`.

Escopo:

- adicionar OpenMetadata;
- instrumentar Prometheus, Grafana e Loki;
- provisionar infraestrutura com Terraform;
- empacotar deployment com Helm;
- configurar CI/CD para lint, testes, build e deploy;
- formalizar secret management e access control.

Critérios de saída:

- deploy é repetível e com rollback controlado;
- datasets críticos têm owner, contrato e lineage;
- observabilidade cobre ingestão, transformação, API e serving.

### Fase 6: Productization

Objetivo: transformar a base operacional em produto analítico diferenciado.

Escopo:

- modelar `gold.alertas_resgate_anormal`;
- introduzir cache seletivo na API;
- adicionar paginação, filtros e limites de contrato;
- publicar scorecards de confiança por dataset;
- consolidar camada semântica de métricas.

Critérios de saída:

- alertas possuem explicabilidade mínima;
- mesma definição de métrica vale para API, BI e docs;
- contratos de consumo ficam explícitos.

## Workstreams

### 1. Platform Engineering

- configuração de ambiente;
- empacotamento;
- CI/CD;
- IaC;
- segredos e políticas de acesso.

### 2. Ingestion Engineering

- conectores por fonte;
- controle de incrementalidade;
- parsing e persistência em `raw` e `bronze`;
- replay e backfill.

### 3. Analytics Engineering

- modelos `silver` e `gold`;
- testes de transformação;
- contratos de grain;
- documentação analítica.

### 4. Data Reliability

- quality checks;
- quarantine;
- observabilidade;
- runbooks;
- indicadores operacionais.

### 5. Data Product

- FastAPI;
- dashboards;
- contratos de serving;
- cache;
- alertas e scorecards.

## Ordem recomendada de implementação

1. Fase 0
2. Fase 1
3. Fase 2
4. Fase 3
5. Fase 4
6. Fase 5
7. Fase 6

## Dependências críticas

- API analítica depende de `gold` estável e contratos de serving.
- Dashboards dependem de grain definido e métricas reconciliadas.
- OpenMetadata depende de datasets e pipelines minimamente materializados.
- Observabilidade de produção depende de workloads reais para instrumentação útil.
- Alertas de anomalia dependem de histórico suficiente em `gold`.

## Definição de pronto por fase

Uma fase só fecha quando:

- código, configuração e documentação foram materializados;
- existe teste ou validação verificável do que foi entregue;
- existe critério operacional de sucesso;
- a próxima fase não depende de decisões ainda abertas da fase atual.

## Riscos de execução

- começar por dashboards antes de estabilizar ingestão e contratos;
- introduzir múltiplas fontes simultaneamente cedo demais;
- acoplar API diretamente a tabelas instáveis;
- deixar quality como atividade posterior;
- tratar ambiente local como arquitetura final.

## Primeiros entregáveis recomendados

1. `pyproject.toml`, `docker-compose.yml` e `.env.example`
2. config compartilhada da aplicação
3. metadata tables do control-plane
4. conector CVM informe diário
5. DAG inicial de ingestão
6. modelo `bronze.cvm_informe_diario`
7. modelo `silver.fundos_informe_diario`
8. regras críticas de quality
9. `gold.fato_fundo_diario`
10. endpoint `GET /health`
