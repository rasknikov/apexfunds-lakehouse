# Apex Lakehouse Docs

Este diretório consolida a documentação base do projeto `apex-lakehouse`, derivada do `blueprint.md` e expandida para um alvo de produção em larga escala compatível com um projeto sênior de engenharia de dados.

## Documentos

- [`architecture.md`](./architecture.md): visão arquitetural, componentes, fluxos, ambientes e decisões estruturais.
- [`implementation-plan.md`](./implementation-plan.md): plano sequencial de implementação, workstreams, dependências e critérios de saída.
- [`stack.md`](./stack.md): stack recomendada, papel de cada tecnologia, padrões de engenharia e caminhos de evolução.
- [`spec.md`](./spec.md): especificação funcional e não funcional da plataforma, incluindo entidades, pipelines, APIs e critérios operacionais.
- [`roadmap.md`](./roadmap.md): visão consolidada do roadmap e dos níveis de maturidade.
- [`roadmap/`](./roadmap/README.md): detalhamento do roadmap em níveis sequenciais.

## Materializado na Fase 0

- `pyproject.toml` com build, `pytest`, `ruff` e `mypy`;
- `.env.example` para sandbox local;
- `docker-compose.yml` com serviços core e perfis para topologia completa;
- pacote Python compartilhado em `src/apex_lakehouse`;
- `FastAPI` de smoke em `api/app/main.py`;
- `Makefile`, `.pre-commit-config.yaml` e script de inspeção de config.

## Expansões adicionadas além do blueprint

O blueprint cobre bem a visão de produto e o desenho medallion, mas um projeto sênior em produção precisa também de:

- contratos de dados e estratégia de reprocessamento;
- SLOs de freshness, disponibilidade e qualidade;
- critérios explícitos de idempotência e incrementalidade;
- operação com quarantine, observabilidade e lineage;
- padrão de CI/CD, IaC, testes e gestão de segredos;
- definição clara entre fundação da plataforma, MVP analítico e hardening de produção.
