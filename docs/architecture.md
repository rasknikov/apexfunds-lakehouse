# Arquitetura

## Objetivo

Construir uma plataforma de dados de produção para inteligência de fundos de investimento no Brasil, com capacidade de escalar volume histórico, número de datasets, concorrência de consumo e exigências operacionais sem reescrever o núcleo da arquitetura.

## Princípios arquiteturais

- Production-first: o alvo arquitetural principal é `staging` e `prod`; o ambiente local existe para desenvolvimento e validação.
- Raw imutável: todo payload de origem deve ser preservado para auditoria e replay.
- Iceberg como padrão do lakehouse: `bronze`, `silver` e `gold` devem usar tabelas com suporte a schema evolution, snapshots e manutenção operacional.
- Separação de planos: controle, dados, metadados, serving e plataforma devem ser desacoplados.
- Compute especializado: Spark para processamento batch pesado; Trino para consulta interativa, BI e leitura analítica da API.
- Contract-first: grain, chaves, schema e regras de qualidade precisam ser versionados.
- Idempotência e replay: reprocessamento por partição é requisito nativo, não exceção.
- Quarantine desde a fundação: dado inválido não pode bloquear o pipeline inteiro nem contaminar `silver` e `gold`.
- Observabilidade e segurança by default: métricas, logs, lineage e controle de acesso fazem parte do desenho-base.

## Escopo arquitetural

O sistema é dividido em seis planos:

1. Fontes externas: CVM, BCB e B3.
2. Plano de controle: Airflow, agendamento, dependências, retries e backfills.
3. Plano de dados: ingestão, armazenamento, transformação, quality e quarantine.
4. Plano de metadados: catálogo, contratos, lineage e auditoria.
5. Plano de serving: SQL interativo, API, BI, cache e datasets consumíveis.
6. Plano de plataforma: Kubernetes, CI/CD, IaC, observabilidade e gestão de segredos.

## Fluxo ponta a ponta

```text
CVM / BCB / B3
   |
   v
Python ingestion jobs orchestrated by Airflow
   |
   +--> file_registry / ingestion_state / pipeline_run_log
   |
   v
Raw Zone in S3-compatible storage
   |
   v
Bronze Iceberg tables
   |
   v
Spark + dbt transformations
   |
   +--> Great Expectations / contract checks
   +--> quarantine.* / ops.data_quality_results
   |
   v
Silver Iceberg tables
   |
   v
Gold Iceberg tables / semantic marts
   |
   +--> Trino
   |      +--> Superset
   |      +--> analytical consumers
   |
   +--> FastAPI
          +--> optional Redis cache
          +--> downstream integrations
   
Cross-cutting:
- OpenMetadata for catalog and lineage
- Prometheus / Grafana / Loki for observability
- PostgreSQL for control-plane and operational metadata
```

## Componentes

### 1. Ingestão

Responsável por download, controle de estado, hashing, persistência do arquivo original e publicação em `raw`.

Componentes esperados:

- `ingestion/cvm`: conectores para informe diário, cadastro e perfil mensal.
- `ingestion/bcb`: conectores para séries SGS/BCData.
- `ingestion/b3`: conectores para índices e boletins públicos.
- `source_file_registry`: catálogo de arquivos vistos, hash, período, origem e status.
- `ingestion_state`: última competência carregada por dataset.
- `pipeline_run_log`: execução, duração, retries, erro e volume processado.

Pontos obrigatórios:

- download com hash do arquivo;
- detecção de arquivo novo, alterado ou duplicado;
- persistência do payload original sem mutação;
- retries com backoff;
- replay e backfill por competência ou janela temporal.

### 2. Storage e formato de tabela

O armazenamento canônico deve ser:

- object storage compatível com S3 para `raw` e artefatos de lake;
- Apache Iceberg para `bronze`, `silver` e `gold`;
- PostgreSQL para metadados operacionais do plano de controle;
- Redis para cache de leitura na camada de API quando necessário.

Layout recomendado:

```text
raw/
  cvm/informe_diario/ano=YYYY/mes=MM/arquivo.zip
  cvm/cadastro_fundos/ano=YYYY/mes=MM/arquivo.csv
  bcb/serie=selic/ano=YYYY/mes=MM/payload.json
  b3/indice=ibovespa/ano=YYYY/mes=MM/arquivo.csv

lakehouse/
  bronze/<dataset>
  silver/<dataset>
  gold/<dataset>
```

Metadados mínimos por objeto:

- `source_system`
- `dataset_name`
- `business_date`
- `ingestion_timestamp`
- `file_hash`
- `pipeline_run_id`
- `content_type`

### 3. Processamento

Objetivo: materializar camadas confiáveis com escalabilidade para backfills, janelas históricas longas e múltiplos consumidores.

Motores e responsabilidades:

- Spark: parsing pesado, normalização, deduplicação, joins amplos, manutenção de partições e jobs batch.
- dbt Core: modelagem analítica, testes, documentação e composição de marts.
- Trino: consulta interativa sobre Iceberg para BI, exploração e leitura orientada a consumo.

Camadas:

- `bronze`: dados crus tipados com metadados técnicos.
- `silver`: dados conformados, deduplicados e validados.
- `gold`: fatos, dimensões, métricas e produtos analíticos.

### 4. Quality e quarantine

Quality é requisito de fundação da plataforma.

Capacidades obrigatórias:

- testes de schema, completude, unicidade, faixa e integridade referencial;
- severidade por regra (`info`, `warn`, `error`, `critical`);
- bloqueio seletivo por criticidade;
- persistência de resultados em `ops.data_quality_results`;
- roteamento de inválidos para `quarantine.*`.

Quarantine deve registrar:

- motivo da falha;
- regra violada;
- dataset de origem;
- payload do registro;
- `pipeline_run_id`;
- timestamp de quarentena;
- status de tratamento.

### 5. Metadados, lineage e auditoria

O projeto deve expor rastreabilidade em três níveis:

- técnico: arquivo, job, tabela, coluna e execução;
- de negócio: definição da métrica, grain, owner e criticidade;
- operacional: tempo de execução, freshness, backlog e incidentes.

Artefatos mínimos:

- catálogo de datasets;
- lineage por dataset e por coluna crítica quando aplicável;
- glossário de métricas;
- histórico de schemas;
- trilha de auditoria por `pipeline_run_id`.

### 6. Serving e consumo

A camada de consumo deve separar consulta analítica, leitura operacional e integração externa.

Canais:

- Trino para SQL interativo e exploração de datasets `gold`;
- Superset para BI e painéis operacionais;
- FastAPI para publicação de endpoints analíticos;
- Redis para cache de respostas de alto uso e baixa mutabilidade.

Padrão recomendado:

- Iceberg como source of truth;
- Trino como engine principal de leitura;
- FastAPI consultando camadas curadas ou views de serving;
- materializações específicas para API quando o perfil de latência exigir.

### 7. Plataforma e deployment

Topologia-alvo de produção:

- Kubernetes para execução dos serviços;
- Helm ou equivalente para empacotamento;
- Terraform para infraestrutura;
- CI/CD com esteiras separadas para teste, build e deploy;
- gestão de segredos por Vault ou secret manager do provedor;
- segregação de ambientes `dev`, `staging` e `prod`.

## Ambientes

### Desenvolvimento local

Sandbox para validação funcional e debugging:

- Docker Compose ou `kind`;
- MinIO;
- Spark standalone;
- Trino;
- Airflow;
- PostgreSQL;
- FastAPI;
- Superset.

Observação: o ambiente local replica a topologia lógica, mas não a escala ou as garantias operacionais de produção.

### Staging

Ambiente de pré-produção com a mesma topologia lógica de `prod`:

- object storage compatível com S3;
- Airflow;
- Spark;
- Trino;
- OpenMetadata;
- Prometheus/Grafana/Loki;
- secret manager;
- CI/CD com deploy automatizado.

### Produção

Ambiente de larga escala com requisitos de resiliência e isolamento:

- Kubernetes gerenciado;
- object storage gerenciado;
- Spark com autoscaling e filas segregadas por workload;
- Trino com pools de consulta ou resource groups;
- catálogo, lineage e observabilidade centralizados;
- deploy imutável e rollback controlado;
- runbooks e SLOs formalizados.

## Segurança e governança

- Segredos fora do repositório.
- Controle de acesso por papel para operações, consumo analítico e administração.
- Criptografia em trânsito e em repouso.
- Logs de auditoria para alterações de schema, deploys e execuções críticas.
- Versionamento de contratos de dados e regras de qualidade.
- Classificação de dados e revisão de aderência a LGPD.

## Decisões recomendadas

- Adotar Iceberg desde o Nível 1 para evitar migração estrutural prematura.
- Usar Spark como engine batch principal e Trino como engine de serving analítico.
- Reservar PostgreSQL para control-plane e metadados operacionais, não como data mart principal.
- Tratar quarantine e quality results como parte obrigatória da fundação da plataforma.
- Fechar o MVP analítico no fim do Nível 2; Nível 1 sozinho entrega fundação operacional, não produto analítico completo.
