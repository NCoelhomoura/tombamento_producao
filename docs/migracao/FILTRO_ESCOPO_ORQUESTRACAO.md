# Escopo de migração: JSON, orquestrador e módulos

Documento **único** para não confundir **onde** o filtro de escopo é definido, **gravado** e **lido**. Qualquer implementação nova (incluindo **billings**) deve **reutilizar o mesmo arquivo e a mesma lógica de parâmetros** que o restante do pipeline.

---

## 1. Fonte de verdade no repositório

| Artefato | Caminho |
|----------|---------|
| **Arquivo de escopo agregado** | `contracts/contracts_filter_main.json` |
| **Orquestrador** | `orchestrator_tasks.py` (raiz do projeto) |
| **Conexão / destino** | `utils/database_connection.py` (`--destino`, `MIGRATION_DESTINO`) |

Não existe um segundo JSON paralelo de “escopo global”: **customers** e **stores** apontam explicitamente para `contracts/contracts_filter_main.json` (pasta `contracts` relativa ao módulo). **contracts** usa o mesmo nome no próprio diretório.

---

## 2. Estrutura esperada de `contracts_filter_main.json`

- **`filters_applied`**: eco dos filtros da última execução que gerou/atualizou o arquivo (`id_orcamento`, datas, `status_pedido`, `limit_rows`, `clear_data`, etc.).
- **`aggregated_ids`**: IDs já consolidados para uso em SQL e joins:
  - `IdOrcamento`, `IdCliente`, `IdEstabelecimento`, `IdBandeira`, `IdRede`

Quem **popula** esse JSON: principalmente o fluxo de **contracts** e/ou métodos `save_filter_json_from_view` em **customers** e **stores** quando rodam com filtro de `IdOrcamento` ou após coleta na view.

---

## 3. Orquestrador: parâmetros que devem bater com o JSON

Na linha de comando, os mesmos critérios devem ser passados para **todas** as tasks quando se usa escopo restrito:

| Parâmetro | Exemplo | Efeito |
|-----------|---------|--------|
| `--id-orcamento` | `6192,6193` | Lista de `IdOrcamento`; propagado para `run_customers_task`, `run_stores_task`, `run_contracts_task` |
| `--data-aviso-previo` | `2026-01-01` | Filtro de data (quando aplicável ao módulo) |
| `--data-inicio-operacao` | `2026-03-30` | Idem |
| `--status-pedido` | `6,7,8` | Filtro de status do pedido/orçamento |
| `--limit` | `100` | Limite de linhas por task |
| `--destino` | `HML` ou `PRD` | Schema destino (`gmcore` vs `core`, etc.) |
| `--clear-data` | flag | TRUNCATE / limpeza conforme regras do módulo |

**Regra de validação:** se você rodou com `--id-orcamento A,B`, o `contracts_filter_main.json` resultante deve refletir esse escopo (ou um subconjunto coerente após a view). **Não** misturar uma execução “full” de customers com contracts filtrado sem regerar o JSON.

---

## 4. Quem lê o quê (mapa rápido)

| Módulo | Arquivo Python | Uso do JSON / filtro |
|--------|----------------|----------------------|
| Customers | `customers/customers_to_core.py` | `filter_json_path` → `contracts_filter_main.json`; `id_orcamento_filter`; `load_filter_json` / escrita agregada |
| Stores | `stores/stores_to_core.py` | Idem |
| Contracts | `contracts/contracts_to_core.py` | `contracts_filter_main.json` no diretório `contracts`; grava e lê `aggregated_ids` |
| Users | `users/users_to_core.py` | Não centraliza o mesmo JSON da mesma forma; contracts depende de users existentes (ver `orchestrator_tasks.py`) |
| **Billings (futuro)** | *a definir em `billing/`* | **Deve:** (1) restringir a `IdOrcamento` ∈ `aggregated_ids.IdOrcamento` **e** (2) interseção com `contratos_ativos.xlsx` na raiz do repo (`id_orcamento`), conforme `billing/billing_dictionary.txt` e `PONTOS_A_FECHAR.json` (BILL-002) |

---

## 5. Ordem de execução e o JSON

A ordem macro está em `docs/migracao/SEQUENCIA_EXECUCAO.md`. O arquivo `contracts_filter_main.json` é o **elo** entre módulos: contracts consolida escopo; customers/stores/billings **não** devem assumir outro conjunto de IDs sem atualizar esse arquivo ou sem passar o mesmo `--id-orcamento`.

---

## 6. Checklist antes de validar uma carga

- [ ] Mesmo `--destino` (HML/PRD) em todas as tasks do lote.
- [ ] Se uso de `--id-orcamento`: mesma lista em customers, stores e contracts da mesma janela de migração.
- [ ] `contracts_filter_main.json` existe e contém `aggregated_ids` após o passo que gera escopo.
- [ ] Para **billings**: `IdOrcamento` finais ⊆ interseção **view (`ViewOrcamentosLojas`) ∩ XLSX** e consistentes com `aggregated_ids` quando o escopo vier do JSON.

---

**Última atualização:** 2026-03-29 — alinhado a `PONTOS_A_FECHAR.json` (`escopo_requisitos`, BILL-002).
