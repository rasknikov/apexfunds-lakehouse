# Nível 3: Production Hardening

## Objetivo

Endurecer a plataforma para execução previsível em `staging` e `prod`, com observabilidade, governança e automação de deployment.

## Escopo

- publicar catálogo e lineage em OpenMetadata;
- instrumentar Prometheus, Grafana e Loki;
- automatizar lint, testes, build e deploy;
- provisionar infraestrutura com Terraform;
- integrar secret manager;
- definir políticas de backup, rollback e recuperação;
- isolar workloads batch e interativos no ambiente de execução.

## Entregáveis

- catálogo com definições de datasets e métricas;
- lineage navegável para fatos e métricas principais;
- painéis de latência, freshness, volume, erro e backlog;
- pipeline de CI/CD para `staging` e `prod`;
- runbook operacional e de disaster recovery;
- controles de acesso e segredos formalizados.

## Critérios de saída

- todo dataset crítico com owner e contrato explícito;
- incidentes simulados com procedimento de recuperação documentado;
- deploy repetível com rollback controlado;
- execução observável do ponto de vista técnico e de negócio.

## Riscos se pular esta etapa

- maturidade sênior fica apenas no discurso;
- debugging lento e dependente de conhecimento tácito;
- deploy frágil e não auditável;
- baixa confiança para operar em produção.
