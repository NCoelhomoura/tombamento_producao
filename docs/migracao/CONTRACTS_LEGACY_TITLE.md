# Ajustes em `gmcommercial.contracts` (step 1) — legacy_customer_id e title

Documento de referência para implementação futura. Alinhado ao modelo de dados acordado e a `docs/migracao/PONTOS_A_FECHAR.json` (fase C / `escopo_requisitos.ajustes_contracts`).

**Status:** pendente de implementação no código (`contracts/contracts_to_core.py`, ETAPA 1).

---

## Grão da tabela e “duplicidades”

- **Grão:** **uma linha = um contrato = um `IdOrcamento`** no legado.
- **Obrigatório:** não haver **duas linhas com o mesmo `contracts.legacy_id`** (é o identificador natural do contrato; duplicar seria o mesmo orçamento inserido duas vezes).
- **`contracts.title` e `contracts.legacy_customer_id`:** na mesma linha são um único valor cada; **entre contratos diferentes** `title` e `legacy_customer_id` **podem repetir** (vários `IdOrcamento` para o mesmo `IdCliente` ou o mesmo `NomeCliente`). O que **não** pode é **duplicar o par `legacy_id` + linha** (segunda linha idêntica ao mesmo contrato).
- **Resumo:** a unicidade que a migração deve **garantir** é a de **`legacy_id`**; `title` e `legacy_customer_id` descrevem aquele contrato e **não** exigem unicidade global (salvo regra de negócio futura explícita).

---

## 1. `contracts.legacy_customer_id`

### Regra (manter)

- Deve armazenar o **`IdCliente` original** do ERP, **o mesmo** que aparece na linha da view usada para o contrato: **`ViewOrcamentosLojas.IdCliente`**.
- Deve ser **consistente** com **`customers.legacy_ids`**: o id gravado aqui é um dos ids de cliente legado que identificam aquele registro em `customers` (após deduplicação por CNPJ, usar o **IdCliente de referência** acordado na regra de negócio, desde que conste no JSON `legacy_ids`).

### Situação atual no código (para correção)

- No batch do step 1, o valor passado para `legacy_customer_id` está vindo de **`legado_id`**, que é **`row[0]` = `IdOrcamento`**.
- **Correção:** popular `legacy_customer_id` com **`IdCliente`** (no loop: `customer_legacy_id` / `row[1]`, após alinhar índices da query se novas colunas forem adicionadas — ver secção 2).

### `contracts.legacy_id` (sem mudança de significado)

- Continua representando **`IdOrcamento`** (identificador legado do orçamento/contrato).

---

## 2. `contracts.title`

### Regra

- **`contracts.title`** deve ser o **`ViewOrcamentosLojas.NomeCliente` original** (string como vem da view no SQL Server), para o contexto daquele `IdOrcamento` / linhas agregadas.

### Situação atual no código (para implementação)

- A query principal do step 1 (**`GROUP BY v.IdOrcamento`**) **não inclui** `NomeCliente` no `SELECT`; o insert usa `None` para `title`.
- **Implementação sugerida:** incluir na query algo como **`MAX(v.NomeCliente) AS NomeCliente`** (ou regra equivalente se houver mais de um nome por orçamento — documentar desempate se necessário), ajustar o índice das colunas em `all_rows`, e passar esse valor para a coluna **`title`** no `INSERT`, com truncamento apenas se o destino tiver limite de tamanho (conferir dicionário / DDL).

---

## 3. Checklist pós-implementação

- [ ] **Sem `legacy_id` duplicado** na tabela (contagem por `legacy_id` = 1 por valor).
- [ ] Para uma amostra de contratos: `legacy_id` = `IdOrcamento`; `legacy_customer_id` = `IdCliente` da view; `customer_id` (UUID) bate com o customer cujo `legacy_ids` contém esse `IdCliente`.
- [ ] `title` igual ao `NomeCliente` esperado na origem para o mesmo `IdOrcamento` (validar no SSMS ou query de comparação).

---

## 4. Referências

- View: **`ViewOrcamentosLojas`** (`NomeCliente`, `IdCliente`, `IdOrcamento`, …).
- Código: `contracts/contracts_to_core.py` — função do step 1 de `contracts` (montagem de `batch_values` e `INSERT` em `contracts`).

---

**Última atualização:** 2026-03-30
