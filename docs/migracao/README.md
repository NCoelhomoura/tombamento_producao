# Documentação da migração CORE ← ERP

Esta pasta concentra documentos de **sequência**, **regras** e **especificações** da migração, para manter o repositório organizado.

## Arquivos

| Arquivo | Descrição |
|---------|-----------|
| [SEQUENCIA_EXECUCAO.md](SEQUENCIA_EXECUCAO.md) | Ordem obrigatória de execução dos módulos (inclui **billings após contracts**) |
| [FILTRO_ESCOPO_ORQUESTRACAO.md](FILTRO_ESCOPO_ORQUESTRACAO.md) | **Fonte única:** `contracts_filter_main.json`, parâmetros do `orchestrator_tasks.py`, quem lê/grava por módulo |
| [PONTOS_A_FECHAR.json](PONTOS_A_FECHAR.json) | Decisões por categoria; `meta.referencia_*`, `meta.escopo_excluido` — **editar em JSON** |
| [../../billing/billing_dictionary.txt](../../billing/billing_dictionary.txt) | Mapeamento **financial.billings** / **gmfinancial.billings** (origem + XLSX) |
| [DEDUPLICACAO_CNPJ_CLIENTES.md](DEDUPLICACAO_CNPJ_CLIENTES.md) | **Regra fixa:** deduplicação por CNPJ, canônico, merge de colunas, `legacy_ids`, mapa de IDs e validação |
| [CONTRACTS_LEGACY_TITLE.md](CONTRACTS_LEGACY_TITLE.md) | **Pendente:** `legacy_customer_id` = `IdCliente` (não IdOrcamento); `title` = `ViewOrcamentosLojas.NomeCliente` |

Documentos adicionais (regras detalhadas, specs por domínio) podem ser incluídos aqui conforme forem criados.

## Relação com outros arquivos do projeto

- Dependências detalhadas por módulo e steps: `ORDEM_DEPENDENCIAS.md` (raiz do repositório)
- Regras operacionais (TRUNCATE, filtros, `--clear-data`): `rules.txt` (raiz)
