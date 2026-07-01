# Stack

## Diretriz

A stack canônica do projeto é `production-first`, orientada a lakehouse em larga escala. O ambiente local existe para desenvolvimento, mas o desenho principal é o de uma plataforma operável em `staging` e `prod` com múltiplos consumidores e backfills confiáveis.

## Stack canônica

| Domínio | Tecnologia | Papel no projeto | Observação |
| --- | --- | --- | --- |
| Linguagem principal | Python | Ingestão, automação operacional e API | Base única para conectores e serviços |
| Orquestração | Airflow | Agendamento, dependências, retries e backfills | Control-plane padrão |
| Object storage | S3 compatível / MinIO | Persistência de `raw` e tabelas do lake | MinIO no dev, storage gerenciado em prod |
| Formato lakehouse | Apache Iceberg | Padrão para `bronze`, `silver` e `gold` | Snapshot, schema evolution e manutenção |
| Batch compute | Apache Spark | Parsing pesado, joins amplos e backfills | Engine principal de transformação batch |
| Query interativa | Trino | BI, exploração analítica e serving SQL | Leitura interativa sobre Iceberg |
| Modelagem analítica | dbt Core | Modelos, testes, docs e marts | Preferencialmente sobre Trino, com suporte a modelos orientados a Spark quando necessário |
| Data quality | Great Expectations | Regras de schema, integridade e qualidade | Integrado ao pipeline e à quarantine |
| Catálogo e lineage | OpenMetadata | Catálogo, owners, glossary e lineage | Sistema de metadados principal |
| Observabilidade | Prometheus + Grafana + Loki | Métricas, dashboards e logs | Base de monitoração operacional |
| API | FastAPI | Endpoints analíticos e operacionais | Publicação de consumo externo |
| Cache | Redis | Redução de latência em endpoints quentes | Opcional no início, padrão a partir do Nível 4 |
| BI | Superset | Dashboards exploratórios e operacionais | Conectado ao Trino |
| Banco operacional | PostgreSQL | Metadados de controle, estado e execução | Não é o mart analítico principal |
| Containers | Docker | Build e ambiente local | Base de empacotamento |
| Orquestração de containers | Kubernetes | Execução em staging e produção | Isolamento, escalabilidade e rollout |
| Infraestrutura como código | Terraform | Provisionamento de cloud e serviços | Reprodutibilidade de ambientes |
| CI/CD | GitHub Actions + deploy controller | Lint, teste, build e deploy | Pode ser Argo CD, Helm pipeline ou equivalente |
| Gestão de segredos | Vault ou secret manager | Segredos de storage, banco e APIs | Obrigatório fora do ambiente local |
| Testes | `pytest` | Testes unitários e de integração | Para conectores e regras críticas |
| Qualidade estática | `ruff` + `mypy` | Lint e typing | Disciplina de manutenção |
| Hooks locais | `pre-commit` | Enforcement pré-commit | Uniformidade local |

## Perfis de ambiente

### Sandbox local

- Docker Compose ou `kind`
- MinIO
- Spark standalone
- Trino
- Airflow
- PostgreSQL
- FastAPI
- Superset

### Staging

- Kubernetes
- object storage gerenciado
- Spark
- Trino
- Airflow
- OpenMetadata
- Prometheus/Grafana/Loki
- secret manager

### Produção

- Kubernetes gerenciado com namespaces e políticas por domínio
- object storage gerenciado
- Spark com autoscaling
- Trino com resource groups
- Airflow altamente disponível
- OpenMetadata
- Prometheus/Grafana/Loki
- Redis para cache seletivo
- CI/CD e IaC mandatórios

## Padrões de engenharia obrigatórios

- contratos de dados versionados;
- `quarantine` e `ops.data_quality_results` desde o Nível 1;
- particionamento e estratégia de manutenção de tabelas Iceberg;
- deploy automatizado com rollback;
- segregação entre control-plane, data-plane e serving;
- testes unitários, testes de transformação e data quality checks;
- gestão de segredos fora do repositório.

## Decisões por nível

### Nível 1: Foundation Platform

- Python, Airflow, S3/MinIO, Iceberg, Spark, Trino, PostgreSQL, Great Expectations, FastAPI e Superset.
- Entrega fundação operacional, quality, quarantine, endpoints operacionais, dashboard mínimo e primeiros marts CVM.

### Nível 2: Analytical MVP

- expandir FastAPI e Superset com consumo analítico e adicionar ingestões BCB/B3.
- Entrega o MVP analítico consumível por API e BI.

### Nível 3: Production Hardening

- adicionar OpenMetadata, Prometheus/Grafana/Loki, Terraform, secret manager e deploy automatizado.
- Endurece a operação para staging e produção.

### Nível 4: Productization

- adicionar Redis, camada semântica, alertas avançados e anomalias.
- Expande consumo e diferenciação do produto de dados.

## O que evitar

- tratar PostgreSQL como mart analítico principal;
- manter Parquet solto em vez de tabela governada;
- acoplar API diretamente a datasets não curados;
- introduzir streaming sem SLA que exija baixa latência;
- adicionar mais de uma engine batch sem necessidade real.
