# Nível 1: Foundation Platform

## Objetivo

Estabelecer a base operacional do lakehouse com stack e contratos compatíveis com produção, sem ainda exigir um produto analítico completo.

## Escopo

- estruturar o repositório e os padrões de engenharia;
- provisionar sandbox local e base de `staging`;
- implantar Airflow, object storage, Iceberg, Spark, Trino e PostgreSQL operacional;
- implementar ingestão incremental do informe diário da CVM;
- implementar ingestão do cadastro de fundos;
- persistir `raw`, `bronze`, `silver` e `gold`;
- ativar quality checks, `quarantine` e `ops.data_quality_results`;
- publicar endpoints operacionais;
- publicar documentação base.

## Entregáveis

- estrutura de pastas do projeto;
- conectores CVM com controle de estado e hash;
- tabelas Iceberg iniciais;
- `quarantine.*` e `ops.data_quality_results`;
- endpoints `GET /health`, `GET /ops/pipelines/latest` e `GET /quality/latest`;
- dashboard operacional simples;
- `README` e docs técnicas.

## Critérios de saída

- reexecução idempotente da mesma competência;
- replay e backfill por intervalo de datas;
- métricas básicas de execução visíveis;
- quality e quarantine ativas desde o primeiro fluxo;
- documentação suficiente para outro engenheiro subir o ambiente e operar um replay.

## Riscos se pular esta etapa

- dívida estrutural na ingestão;
- ausência de trilha operacional para incidentes;
- retrabalho caro ao migrar para `staging` e `prod`;
- falsa sensação de maturidade sem base de produção.
