# Sequência de execução da migração

Documento de referência rápida para **ordem dos módulos**. Detalhes por step, filtros e troubleshooting permanecem em `ORDEM_DEPENDENCIAS.md` na raiz do repositório.

---

## Ordem obrigatória (visão macro)

Execute **nesta ordem** para respeitar dependências de dados (clientes, lojas, contratos antes de faturamento vinculado a contrato/cliente).

```
1. Users          (opcional — quando houver referência a usuários em contracts)
2. Customers      (obrigatório antes de contracts)
3. Stores         (obrigatório antes de contracts)
4. Contracts      (módulo principal; gera/atualiza filtros compartilhados)
5. Billings       (obrigatório depois de contracts)
```

### Por que billings vem depois de contracts

- Billings no destino costuma referenciar **contratos** e/ou **clientes** já existentes.
- A migração de contracts consolida escopo e IDs (`contracts_filter_main.json`, relacionamentos com `customer_id` / contratos).
- Executar billings **após** contracts evita FKs quebradas e alinha filtros (mesmo conjunto de `IdOrcamento` / clientes em escopo).

---

## Fluxo simplificado

```
Users (opcional)
      │
      ├──────────────────┐
      │                  │
Customers            Stores
      │                  │
      └────────┬─────────┘
               │
          Contracts
               │
          Billings
```

---

## Checklist rápido antes de rodar

- [ ] **Customers** e **Stores** concluídos conforme necessidade do escopo (ver `ORDEM_DEPENDENCIAS.md`).
- [ ] **Contracts** executado no escopo desejado (filtros / `--clear-data` alinhados às regras em `rules.txt`).
- [ ] **Billings** somente após contracts estável para o mesmo ambiente e escopo.

---

## Escopo e filtros (um só lugar)

- **Arquivo agregado:** `contracts/contracts_filter_main.json` (`aggregated_ids`, `filters_applied`).
- **Orquestrador:** `orchestrator_tasks.py` (`--id-orcamento`, `--destino`, etc.) — os mesmos parâmetros devem propagar para todas as tasks do lote.
- **Detalhe completo:** [FILTRO_ESCOPO_ORQUESTRACAO.md](FILTRO_ESCOPO_ORQUESTRACAO.md) (mapa módulo × JSON × CLI; billings deve seguir o mesmo escopo + interseção XLSX).

---

## Onde aprofundar

| Tema | Documento |
|------|-----------|
| **Filtro / JSON / CLI / billings** | `docs/migracao/FILTRO_ESCOPO_ORQUESTRACAO.md` |
| Fases lógicas de implementação (A → D: billings; **contracts antes de billings**; ver `meta.escopo_excluido`) | `docs/migracao/PONTOS_A_FECHAR.json` → `ordem_implementacao` e `escopo_requisitos` |
| Steps internos (customers 1–4, stores 1–7, contracts 1–8, etc.) | `ORDEM_DEPENDENCIAS.md` |
| TRUNCATE, DELETE, filtros, `--clear-data` | `rules.txt` |
| Schema destino (ex.: `gmcommercial`, `gmpdv`) | Código do orquestrador e `utils/database_connection` |

---

**Última atualização:** 2026-03-29
