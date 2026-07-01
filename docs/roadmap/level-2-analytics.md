# Nível 2: Analytical MVP

## Objetivo

Fechar o MVP analítico do produto, transformando a fundação operacional em consumo real por BI e API.

## Escopo

- adicionar ingestão de séries BCB prioritárias, como Selic e IPCA;
- adicionar ingestão de índices públicos da B3;
- enriquecer `silver` com conformidade temporal e dimensões auxiliares;
- expandir `gold` com métricas de performance, fluxo e correlação;
- expor os primeiros endpoints analíticos FastAPI;
- montar dashboards de mercado e de comparação macro.

## Entregáveis

- `gold.fato_macro_diaria`;
- `gold.fato_indice_mercado`;
- rankings por classe de fundo;
- endpoint de performance por fundo;
- endpoint de fluxo por fundo;
- endpoint de ranking;
- dashboard analítico de mercado.

## Critérios de saída

- métricas principais reconciliadas com dados de origem;
- consumo por API e BI utilizando a mesma camada `gold`;
- cobertura mínima de testes para regras de cálculo;
- MVP analítico utilizável por um consumidor não técnico.

## Riscos se pular esta etapa

- projeto vira apenas plataforma sem produto;
- ausência de contexto macro reduz relevância do caso de uso;
- BI e API ficam sem modelo curado de referência.
