# Spec

## Visão do produto

`Apex Lakehouse` é uma plataforma de inteligência de fundos brasileiros desenhada para produção em larga escala. O sistema consolida dados públicos regulatórios e macroeconômicos, publica produtos analíticos confiáveis e opera com contratos, qualidade, rastreabilidade e observabilidade formais.

## Objetivos

- Consolidar dados de fundos brasileiros a partir de CVM, BCB e B3.
- Permitir análise histórica confiável de performance, fluxo e exposição ao cenário macro.
- Disponibilizar produtos de dados consistentes para BI, API e integrações analíticas.
- Demonstrar uma arquitetura de lakehouse operável em escala, com qualidade, lineage, governança e reprocessamento controlado.

## Usuários-alvo

- Engenheiros de dados.
- Analytics engineers.
- Analistas de investimento e produto.
- Consumidores internos e externos via API.
- Times de governança e operação de dados.

## Escopo funcional

### FR-01: ingestão incremental

O sistema deve ingerir dados das seguintes fontes:

- CVM informe diário de fundos;
- CVM cadastro de fundos;
- CVM perfil mensal de fundos;
- BCB séries macroeconômicas;
- B3 índices e estatísticas públicas.

Requisitos:

- rastrear última competência carregada por dataset;
- detectar arquivos novos e arquivos alterados;
- registrar hash, origem, período e status;
- suportar reprocessamento por partição temporal.

### FR-02: persistência `raw`

Todo payload original deve ser salvo sem mutação com metadados técnicos suficientes para auditoria e replay.

### FR-03: transformação em camadas

O sistema deve materializar dados em tabelas Iceberg organizadas em:

- `bronze` para persistência crua tipada;
- `silver` para conformidade, deduplicação e enriquecimento;
- `gold` para fatos, dimensões e métricas prontas para consumo.

### FR-04: qualidade e quarantine

O sistema deve:

- validar schema, unicidade, completude, faixa e integridade referencial;
- impedir promoção silenciosa de dados inválidos para `silver` e `gold`;
- desviar registros inválidos para `quarantine`;
- registrar resultados em `ops.data_quality_results`;
- permitir replay e tratamento posterior dos registros quarentenados.

Observação: `quarantine` é obrigatória desde o Nível 1.

### FR-05: métricas analíticas

O sistema deve calcular, no mínimo:

- rentabilidade diária;
- rentabilidade mensal;
- captação líquida;
- variação de patrimônio líquido;
- variação de cotistas;
- volatilidade rolling;
- drawdown;
- correlação com Selic, IPCA e Ibovespa;
- rankings por classe, gestor e janela temporal.

### FR-06: APIs por fase

#### Fase operacional: Nível 1

- `GET /health`
- `GET /ops/pipelines/latest`
- `GET /quality/latest`

#### MVP analítico: Nível 2

- `GET /funds/{cnpj}/performance`
- `GET /funds/{cnpj}/flows`
- `GET /funds/ranking`
- `GET /market/macro/{serie}`

#### Produto avançado: Nível 4

- `GET /alerts/funds/redemptions`

### FR-07: dashboards por fase

#### Nível 1

- sanidade de pipeline;
- volume carregado por dataset;
- qualidade por execução;
- freshness básica.

#### Nível 2

- visão geral do mercado de fundos;
- ranking por rentabilidade;
- maiores captações líquidas;
- maiores resgates;
- correlação com indicadores macro.

#### Nível 3

- latência por camada;
- backlog de processamento;
- lineage operacional;
- score operacional por domínio.

### FR-08: metadados e lineage

O sistema deve expor:

- definição de datasets;
- owners e criticidade;
- lineage entre fonte, tabela, modelo e métrica;
- histórico de execução por `pipeline_run_id`;
- histórico de schemas e contratos.

### FR-09: operação e observabilidade

O sistema deve medir:

- duração de ingestão por fonte;
- volume por dataset e por dia;
- percentual de erro por pipeline;
- freshness por camada;
- tamanho da quarantine;
- taxa de sucesso de execuções;
- tempo de promoção entre `bronze`, `silver` e `gold`.

## Fontes e estratégia de carga

| Fonte | Dataset | Cadência esperada | Chave incremental |
| --- | --- | --- | --- |
| CVM | informe diário | diária | data de competência + arquivo |
| CVM | cadastro de fundos | mensal ou quando houver publicação | arquivo + data de referência |
| CVM | perfil mensal | mensal | competência |
| BCB | séries macro | diária ou sob demanda | código da série + data da observação |
| B3 | índices públicos | diária | índice + data do pregão |

## Modelo de dados

### Entidades centrais

| Entidade | Grain | Observações |
| --- | --- | --- |
| Fundo | `cnpj_fundo` | dimensão mestre do produto financeiro |
| Informe diário | `cnpj_fundo + data_competencia` | fatos operacionais e de valor da cota |
| Perfil mensal | `cnpj_fundo + competencia_mensal` | atributos periódicos e classificação |
| Série macro | `codigo_serie + data` | Selic, IPCA e correlatas |
| Índice de mercado | `indice + data` | Ibovespa e outros índices públicos |

### Tabelas alvo

| Camada | Tabela | Grain |
| --- | --- | --- |
| Bronze | `bronze.cvm_informe_diario` | linha original do informe diário |
| Bronze | `bronze.cvm_cadastro_fundos` | linha original do cadastro |
| Bronze | `bronze.cvm_perfil_mensal` | linha original do perfil mensal |
| Bronze | `bronze.bcb_series_macro` | `codigo_serie + data` |
| Bronze | `bronze.b3_indices_mercado` | `indice + data_pregao` |
| Silver | `silver.fundos` | `cnpj_fundo` |
| Silver | `silver.fundos_informe_diario` | `cnpj_fundo + data_competencia` |
| Silver | `silver.fundos_perfil_mensal` | `cnpj_fundo + competencia_mensal` |
| Silver | `silver.series_macro` | `codigo_serie + data` |
| Silver | `silver.indices_mercado` | `indice + data_pregao` |
| Silver | `silver.administradores` | `cnpj_administrador` |
| Silver | `silver.gestores` | `cnpj_gestor` |
| Gold | `gold.dim_fundo` | `fund_key` |
| Gold | `gold.dim_tempo` | `date_key` |
| Gold | `gold.dim_administrador` | `administrator_key` |
| Gold | `gold.dim_gestor` | `manager_key` |
| Gold | `gold.fato_fundo_diario` | `fund_key + date_key` |
| Gold | `gold.fato_fundo_mensal` | `fund_key + month_key` |
| Gold | `gold.fato_macro_diaria` | `macro_key + date_key` |
| Gold | `gold.fato_indice_mercado` | `market_index_key + date_key` |
| Gold | `gold.alertas_resgate_anormal` | `fund_key + date_key + alert_type` |

## Contratos de dados

Todo dataset promovido para `silver` ou `gold` deve declarar:

- grain;
- chaves de negócio;
- chaves substitutas, quando houver;
- tipos esperados;
- campos obrigatórios;
- regras de faixa;
- owner técnico;
- criticidade;
- política de reprocessamento;
- política de retenção;
- política de SLA/SLO.

## Regras mínimas de qualidade

- `cnpj_fundo` não pode ser nulo em `silver.fundos`.
- `valor_cota` deve ser maior que zero em fatos diários válidos.
- `patrimonio_liquido` não pode ser negativo, salvo exceção documentada.
- `data_competencia` não pode estar no futuro.
- não pode existir duplicidade de `cnpj_fundo + data_competencia`.
- séries macro não podem retroceder datas dentro da mesma carga.
- fundos sem correspondência cadastral devem seguir para `quarantine`.

## Requisitos não funcionais

### NFR-01: freshness

- informe diário da CVM disponível em `gold` até D+1 da publicação;
- séries macro críticas atualizadas no mesmo dia útil da coleta;
- indicadores operacionais atualizados a cada execução.

### NFR-02: idempotência

Reexecutar o mesmo job para o mesmo período deve produzir o mesmo resultado lógico, sem duplicidade.

### NFR-03: recuperabilidade

O sistema deve suportar:

- retry automático;
- replay por dataset;
- backfill por janela temporal;
- reconstrução de `gold` a partir de `silver`.

### NFR-04: auditabilidade

Toda carga deve registrar:

- origem;
- arquivo ou período consultado;
- hash;
- row count;
- status;
- timestamps de início e fim;
- `pipeline_run_id`.

### NFR-05: segurança

- segredos externos ao código;
- mínimo privilégio para storage, banco e API;
- segregação entre usuários de operação, administração e leitura;
- trilha de auditoria para ações administrativas;
- criptografia em trânsito e em repouso.

### NFR-06: operabilidade

O projeto deve ser reproduzível em ambiente local e implantável em `staging` e `prod` com pipeline automatizado.

### NFR-07: SLOs operacionais

- sucesso de execuções agendadas maior ou igual a 99 por cento em janela de 30 dias, excluindo indisponibilidade comprovada da fonte externa;
- datasets `gold` diários atualizados em até 24 horas após publicação da origem;
- datasets mensais atualizados em até 48 horas após disponibilidade da competência;
- regras críticas de qualidade com taxa de aprovação de 100 por cento para promoção a `gold`;
- runbooks e procedimentos de replay disponíveis para incidentes críticos.

### NFR-08: escalabilidade e performance

- suportar expansão de histórico sem reprocessamento full desnecessário;
- suportar múltiplos consumidores concorrentes em BI e API;
- suportar manutenção de partições e compactação sem indisponibilidade total do consumo;
- desacoplar workloads batch e workloads interativos.

### NFR-09: disponibilidade e recuperação

- ambientes `staging` e `prod` segregados;
- deploy com rollback controlado;
- política de backup e recuperação para metadados operacionais;
- RTO e RPO definidos para o control-plane e para os contratos de serving.

## Critérios de aceite por nível

### Nível 1: Foundation Platform

- ingestão incremental da CVM funcionando de ponta a ponta;
- tabelas Iceberg em `bronze`, `silver` e `gold` para fundos diários;
- `quarantine` e `ops.data_quality_results` ativos;
- endpoints `GET /health`, `GET /ops/pipelines/latest` e `GET /quality/latest` ativos;
- dashboard operacional mínimo disponível;
- documentação de arquitetura, stack, spec e roadmap concluída.

### Nível 2: MVP analítico

- ingestões BCB e B3 ativas para séries e índices prioritários;
- métricas centrais de fundos publicadas em `gold`;
- pelo menos dois endpoints analíticos ativos;
- pelo menos um dashboard analítico de negócio ativo;
- consumo por API e BI sobre a mesma camada curada.

### Nível 3: Production hardening

- observabilidade centralizada ativa;
- catálogo e lineage publicados;
- deploy automatizado para `staging` e `prod`;
- gestão de segredos e controles de acesso formalizados.

## Fora de escopo inicial

- streaming em tempo real com SLA de segundos;
- trading ou execução de ordens;
- multi-região active-active;
- model serving online de baixa latência;
- dados privados individualizados de cotistas;
- motor de precificação intraday.
