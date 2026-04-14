# Ordem de Dependências e Sequência de Execução

Este documento descreve a ordem correta de execução das migrações para garantir integridade referencial dos dados.

**Sequência macro (inclui billings após contracts):** [docs/migracao/SEQUENCIA_EXECUCAO.md](docs/migracao/SEQUENCIA_EXECUCAO.md)

## ⚠️ IMPORTANTE: Ordem Obrigatória

As migrações devem ser executadas na seguinte ordem para evitar erros de chaves estrangeiras:

```
1. Users (se necessário)
2. Customers (steps 2, 3, 4)
3. Stores (steps 2, 3, 4)
4. Contracts (todas as etapas)
5. Billings (depois de contracts — ver docs/migracao/SEQUENCIA_EXECUCAO.md)
```

## 📋 Dependências Detalhadas

### 1. Users (Opcional - apenas se necessário)

**Quando executar:**
- Antes de `contracts` se houver referências a usuários (vendedores, etc.)

**Steps:**
- Step 1: `users`

**Dependências:**
- Nenhuma

---

### 2. Customers

**Quando executar:**
- ANTES de `contracts` (obrigatório)
- Contracts referencia `customers` via `customer_id`

**Steps obrigatórios:**
- Step 1: `customer_segments` (obrigatório antes do step 2)
- Step 2: `customers` (obrigatório antes de contracts)
- Step 3: `addresses` (opcional, mas recomendado)
- Step 4: `contacts` (opcional, mas recomendado)

**Dependências:**
- Step 1 não depende de nada
- Step 2 depende de Step 1 (`customer_segments`)
- Step 3 depende de Step 2 (`customers`)
- Step 4 depende de Step 2 (`customers`)

**Filtros:**
- Se `contracts` foi executado com filtros, `customers` deve usar o mesmo filtro via `contracts/contracts_filter_main.json`
- Se `contracts` não foi executado, `customers` busca diretamente da `ViewOrcamentosLojas`

---

### 3. Stores

**Quando executar:**
- ANTES de `contracts` (obrigatório)
- Contracts referencia `stores` via `store_id` em `contract_scenarios`

**Steps obrigatórios:**
- Step 1: `store_segments` (obrigatório antes dos outros steps)
- Step 2: `retail_chains` (obrigatório antes do step 3)
- Step 3: `store_brands` (obrigatório antes do step 4)
- Step 4: `stores` (obrigatório antes de contracts)
- Step 5: `store_cnpjs` (opcional, mas recomendado)
- Step 6: `addresses` (opcional, mas recomendado)
- Step 7: `contacts` (opcional, mas recomendado)

**Dependências:**
- Step 1 não depende de nada
- Step 2 depende de Step 1 (`store_segments`)
- Step 3 depende de Step 1 (`store_segments`) e Step 2 (`retail_chains`)
- Step 4 depende de Step 1 (`store_segments`), Step 2 (`retail_chains`) e Step 3 (`store_brands`)
- Step 5 depende de Step 4 (`stores`)
- Step 6 depende de Step 4 (`stores`)
- Step 7 depende de Step 4 (`stores`)

**Filtros:**
- Se `contracts` foi executado com filtros, `stores` deve usar o mesmo filtro via `contracts/contracts_filter_main.json`
- Se `contracts` não foi executado, `stores` busca diretamente da `ViewOrcamentosLojas`
- ⚠️ **CRÍTICO:** Mesmo em full load, `stores` sempre filtra pela `ViewOrcamentosLojas` (apenas lojas relacionadas a contratos)

---

### 4. Contracts

**Quando executar:**
- DEPOIS de `customers` e `stores` (obrigatório)
- É o módulo principal que referencia os outros

**Steps:**
- Step 1: `contracts` (obrigatório primeiro)
- Step 2: `contract_scenarios` (depende de Step 1 e `stores`)
- Step 3: `contract_scenario_stores` (depende de Step 2)
- Step 4: `contract_sellers` (depende de Step 1 e `users`)
- Step 5: `contract_team_members` (depende de Step 1 e `users`)
- Step 6: `contract_contacts` (depende de Step 1)
- Step 7: `contract_partners` (depende de Step 1)
- Step 8: `contract_additional_charges` (depende de Step 1)

**Dependências:**
- Step 1 não depende de outros steps de contracts, mas requer `customers` e `stores` migrados
- Step 2 depende de Step 1 (`contracts`) e `stores` (via `store_id`)
- Step 3 depende de Step 2 (`contract_scenarios`)
- Steps 4-8 dependem de Step 1 (`contracts`)

**Filtros:**
- Pode ser executado com filtros específicos (`--id-orcamento`, `--data-aviso-previo`, `--data-inicio-operacao`)
- Gera `contracts/contracts_filter_main.json` com IDs agregados para uso por `stores` e `customers`

---

## 🔄 Sincronização entre Módulos

### Regra de Ouro

**Se `contracts` for atualizado, `stores` e `customers` DEVEM ser re-executados para sincronizar.**

### Por quê?

1. **Novos contratos podem incluir:**
   - Novos `IdEstabelecimento` → `stores` precisa ser atualizado
   - Novos `IdCliente` → `customers` precisa ser atualizado
   - Novos `IdBandeira` e `IdRede` → `stores` precisa ser atualizado

2. **Se não sincronizar:**
   - `contract_scenarios` pode referenciar `stores` que não existem
   - `contracts` pode referenciar `customers` que não existem
   - Inconsistência de dados no destino

### Como Sincronizar

**Opção 1: Execução Completa (Recomendado)**
```bash
# 1. Customers (com filtros do contracts anterior, se existir)
python orchestrator_tasks.py customers

# 2. Stores (com filtros do contracts anterior, se existir)
python orchestrator_tasks.py stores

# 3. Contracts (com filtros específicos)
python orchestrator_tasks.py contracts --id-orcamento 6192 --clear-data
```

**Opção 2: Execução Incremental**
```bash
# 1. Customers step 2 (apenas customers relacionados)
python orchestrator_tasks.py customers 2

# 2. Stores steps 2,3,4 (apenas stores relacionados)
python orchestrator_tasks.py stores 2
python orchestrator_tasks.py stores 3
python orchestrator_tasks.py stores 4

# 3. Contracts (todas as etapas)
python orchestrator_tasks.py contracts --id-orcamento 6192 --clear-data
```

---

## 🚨 Casos Especiais

### Uso de `--clear-data`

⚠️ **ATENÇÃO:** O flag `--clear-data` limpa TODAS as tabelas do escopo executado.

**Regras:**
- `--clear-data` só limpa tabelas que serão migradas na execução atual
- Se executar `contracts --clear-data`, limpa apenas tabelas de contracts
- **NÃO limpa** tabelas de `stores` ou `customers` mesmo que sejam dependências

**Quando usar:**
- Migração completa de um módulo (sem filtros)
- Reset completo antes de nova migração
- Testes em ambiente de desenvolvimento

**Quando NÃO usar:**
- Migração incremental com filtros (use DELETE ao invés de TRUNCATE)
- Quando outros módulos já foram executados anteriormente
- Em produção sem backup adequado

**Exemplo com `--clear-data`:**
```bash
# Ordem correta:
# 1. Customers (sem --clear-data, usa filtros)
python orchestrator_tasks.py customers 2

# 2. Stores (sem --clear-data, usa filtros)
python orchestrator_tasks.py stores 2
python orchestrator_tasks.py stores 3
python orchestrator_tasks.py stores 4

# 3. Contracts (com --clear-data para limpar tudo)
python orchestrator_tasks.py contracts --id-orcamento 6192 --clear-data
```

---

## 📊 Fluxograma de Dependências

```
┌─────────────┐
│   Users     │ (opcional)
└──────┬──────┘
       │
       ├─────────────────┐
       │                 │
┌──────▼──────┐   ┌──────▼──────┐
│  Customers  │   │   Stores    │
│             │   │             │
│ Step 1:     │   │ Step 1:     │
│  segments   │   │  segments   │
│             │   │             │
│ Step 2:     │   │ Step 2:     │
│  customers  │   │  chains     │
│             │   │             │
│ Step 3:     │   │ Step 3:     │
│  addresses  │   │  brands     │
│             │   │             │
│ Step 4:     │   │ Step 4:     │
│  contacts   │   │  stores     │
└──────┬──────┘   └──────┬──────┘
       │                 │
       └────────┬────────┘
                │
       ┌────────▼────────┐
       │    Contracts    │
       │                 │
       │ Step 1:         │
       │  contracts      │
       │                 │
       │ Step 2:         │
       │  scenarios      │
       │                 │
       │ Step 3:         │
       │  scenario_stores│
       │                 │
       │ Steps 4-8:      │
       │  sellers, etc.  │
       └─────────────────┘
```

---

## ✅ Checklist de Execução

Antes de executar `contracts`, verifique:

- [ ] `customers` step 2 foi executado (customers migrados)
- [ ] `stores` step 4 foi executado (stores migrados)
- [ ] Se usar filtros, `customers` e `stores` usam os mesmos filtros de `contracts`
- [ ] Se usar `--clear-data`, está ciente que limpa TODAS as tabelas do escopo

Após executar `contracts`, verifique:

- [ ] `contracts/contracts_filter_main.json` foi gerado
- [ ] Se novos `IdEstabelecimento` foram adicionados, `stores` precisa ser re-executado
- [ ] Se novos `IdCliente` foram adicionados, `customers` precisa ser re-executado

---

## 📝 Exemplos Práticos

### Exemplo 1: Migração Completa (Primeira Vez)

```bash
# 1. Users (se necessário)
python orchestrator_tasks.py users

# 2. Customers (todas as etapas)
python orchestrator_tasks.py customers

# 3. Stores (todas as etapas)
python orchestrator_tasks.py stores

# 4. Contracts (todas as etapas)
python orchestrator_tasks.py contracts
```

### Exemplo 2: Migração Incremental com Filtros

```bash
# 1. Customers (apenas relacionados ao contrato 6192)
python orchestrator_tasks.py customers 2

# 2. Stores (apenas relacionados ao contrato 6192)
python orchestrator_tasks.py stores 2
python orchestrator_tasks.py stores 3
python orchestrator_tasks.py stores 4

# 3. Contracts (apenas contrato 6192)
python orchestrator_tasks.py contracts --id-orcamento 6192
```

### Exemplo 3: Atualização com `--clear-data`

```bash
# 1. Customers (sem --clear-data, usa filtros do JSON)
python orchestrator_tasks.py customers 2

# 2. Stores (sem --clear-data, usa filtros do JSON)
python orchestrator_tasks.py stores 2
python orchestrator_tasks.py stores 3
python orchestrator_tasks.py stores 4

# 3. Contracts (com --clear-data para limpar tudo antes)
python orchestrator_tasks.py contracts --id-orcamento 6192 --clear-data
```

---

## 🔍 Troubleshooting

### Erro: "Store não encontrado"

**Causa:** `contracts` está tentando referenciar `stores` que não foram migrados.

**Solução:** Execute `stores` antes de `contracts`:
```bash
python orchestrator_tasks.py stores 2
python orchestrator_tasks.py stores 3
python orchestrator_tasks.py stores 4
python orchestrator_tasks.py contracts --id-orcamento 6192
```

### Erro: "Customer não encontrado"

**Causa:** `contracts` está tentando referenciar `customers` que não foram migrados.

**Solução:** Execute `customers` antes de `contracts`:
```bash
python orchestrator_tasks.py customers 2
python orchestrator_tasks.py contracts --id-orcamento 6192
```

### Erro: "Contract não encontrado" em steps 2-8

**Causa:** Steps 2-8 de `contracts` dependem do step 1.

**Solução:** Execute o step 1 primeiro:
```bash
python orchestrator_tasks.py contracts 1 --id-orcamento 6192
python orchestrator_tasks.py contracts 2
# ... etc
```

---

**Última atualização:** 2026-01-13
