# Roadmap

O roadmap foi reorganizado para um alvo explícito de produção em larga escala. A progressão separa fundação da plataforma, MVP analítico, hardening operacional e productização avançada.

## Níveis

1. Nível 1: Foundation Platform.
2. Nível 2: Analytical MVP.
3. Nível 3: Production Hardening.
4. Nível 4: Productization and Intelligence.

## Regra de leitura

- Nível 1 fecha a base operacional do lakehouse.
- Nível 2 fecha o MVP analítico do produto.
- Nível 3 endurece a operação para `staging` e `prod`.
- Nível 4 amplia consumo, alertas e diferenciação analítica.

## Leitura detalhada

- [`roadmap/level-1-foundation.md`](./roadmap/level-1-foundation.md)
- [`roadmap/level-2-analytics.md`](./roadmap/level-2-analytics.md)
- [`roadmap/level-3-operations.md`](./roadmap/level-3-operations.md)
- [`roadmap/level-4-productization.md`](./roadmap/level-4-productization.md)

## Gate de avanço

Um nível só deve ser considerado concluído quando:

- os pipelines daquele nível estiverem reproduzíveis;
- documentação e runbook estiverem atualizados;
- testes mínimos estiverem verdes;
- métricas operacionais do nível estiverem visíveis;
- o nível seguinte não estiver compensando dívida estrutural do nível atual.
