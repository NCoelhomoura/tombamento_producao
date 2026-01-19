# RELATÓRIO DE COMPARAÇÃO: Estrutura PRD vs Dicionário

## Resumo Executivo

Este relatório compara a estrutura real das tabelas do schema `commercial` em PRD com o que está documentado no `contract_dictionary.txt`.

---

## 1. CONTRACT_SCENARIO_STORES

### ❌ DIFERENÇA CRÍTICA ENCONTRADA

**No PRD (real):**
- `closed_at` (DATE, nullable) ✅ EXISTE
- `removed_at` ❌ NÃO EXISTE

**No Dicionário (documentado):**
- `removed_at` (TIMESTAMP WITH TIME ZONE, nullable) ✅ DOCUMENTADO
- `closed_at` ❌ NÃO DOCUMENTADO

**Estrutura completa no PRD:**
1. id (UUID, NOT NULL, PK)
2. store_id (UUID, NOT NULL)
3. scenario_id (UUID, NOT NULL)
4. start_date (DATE, NOT NULL)
5. status (INTEGER, NOT NULL)
6. created_at (TIMESTAMP WITH TIME ZONE, NOT NULL)
7. updated_at (TIMESTAMP WITH TIME ZONE, NOT NULL)
8. closed_at (DATE, nullable) ⚠️ **DIFERENTE DO DICIONÁRIO**
9. legacy_id (INTEGER, nullable)

**Observação:** O código está tentando inserir `removed_at`, mas a tabela tem `closed_at`. Isso causa o erro observado no log.

---

## 2. CONTRACTS

### ⚠️ COLUNAS ADICIONAIS NO PRD

**No PRD (real):**
- `end_date` (DATE, nullable) ✅ EXISTE
- `observations` (TEXT, nullable) ✅ EXISTE
- `amount` (NUMERIC, NOT NULL, default=0.0) ✅ EXISTE
- `legacy_id` (INTEGER, nullable) ✅ EXISTE

**No Dicionário:**
- Apenas estrutura de HML documentada (gmcommercial.contracts)
- Não há estrutura de PRD documentada para `contracts`

**Estrutura completa no PRD:**
1. id (UUID, NOT NULL, PK)
2. customer_id (UUID, NOT NULL)
3. billing_day (INTEGER, NOT NULL)
4. due_day (INTEGER, NOT NULL)
5. billing_type (VARCHAR(50), NOT NULL)
6. operation_type (VARCHAR(50), NOT NULL)
7. thirteenth_salary_type (VARCHAR(50), NOT NULL)
8. trade_type (VARCHAR(50), NOT NULL)
9. start_date (DATE, NOT NULL)
10. status (VARCHAR(50), NOT NULL)
11. deleted_at (TIMESTAMP WITH TIME ZONE, nullable)
12. created_at (TIMESTAMP WITH TIME ZONE, NOT NULL)
13. updated_at (TIMESTAMP WITH TIME ZONE, NOT NULL)
14. end_date (DATE, nullable) ⚠️ **NÃO DOCUMENTADO**
15. observations (TEXT, nullable) ⚠️ **NÃO DOCUMENTADO**
16. amount (NUMERIC, NOT NULL) ⚠️ **NÃO DOCUMENTADO**
17. legacy_id (INTEGER, nullable) ✅ EXISTE

---

## 3. CONTRACT_SCENARIOS

### ✅ ESTRUTURA CORRETA NO DICIONÁRIO

**No PRD (real):** 14 colunas
**No Dicionário:** 14 colunas documentadas ✅

**Estrutura no PRD corresponde ao dicionário:**
- Todas as colunas documentadas existem
- `promoter_task_id` ✅ existe
- `end_date` ✅ existe
- `legacy_id` ✅ existe

---

## 4. CONTRACT_SELLERS

### ⚠️ COLUNA ADICIONAL NO PRD

**No PRD (real):**
- `status` (VARCHAR(50), NOT NULL, default='') ✅ EXISTE

**No Dicionário:**
- Apenas estrutura de HML documentada
- Não há estrutura de PRD documentada

**Estrutura completa no PRD:**
1. id (UUID, NOT NULL, PK)
2. user_id (UUID, NOT NULL)
3. contract_id (UUID, NOT NULL)
4. seller_type (VARCHAR(50), NOT NULL)
5. created_at (TIMESTAMP WITH TIME ZONE, NOT NULL)
6. updated_at (TIMESTAMP WITH TIME ZONE, NOT NULL)
7. status (VARCHAR(50), NOT NULL) ⚠️ **NÃO DOCUMENTADO**

---

## 5. CONTRACT_TEAM_MEMBERS

### ✅ ESTRUTURA CORRETA

**No PRD (real):** 6 colunas
**No Dicionário:** Estrutura de HML documentada (parece corresponder)

**Estrutura no PRD:**
1. id (UUID, NOT NULL, PK)
2. user_id (UUID, NOT NULL)
3. contract_id (UUID, NOT NULL)
4. position (VARCHAR(50), NOT NULL)
5. created_at (TIMESTAMP WITH TIME ZONE, NOT NULL)
6. updated_at (TIMESTAMP WITH TIME ZONE, NOT NULL)

**Observação:** Não há estrutura de PRD documentada, apenas HML.

---

## 6. CONTRACT_CONTACTS

### ✅ ESTRUTURA CORRETA

**No PRD (real):** 5 colunas
**No Dicionário:** Estrutura de HML documentada (parece corresponder)

**Estrutura no PRD:**
1. id (UUID, NOT NULL, PK)
2. contract_id (UUID, NOT NULL)
3. name (VARCHAR(255), NOT NULL)
4. created_at (TIMESTAMP WITH TIME ZONE, NOT NULL)
5. updated_at (TIMESTAMP WITH TIME ZONE, NOT NULL)

**Observação:** Não há estrutura de PRD documentada, apenas HML.

---

## 7. CONTRACT_PARTNERS

### ✅ ESTRUTURA CORRETA

**No PRD (real):** 8 colunas
**No Dicionário:** Estrutura de HML documentada (parece corresponder)

**Estrutura no PRD:**
1. id (UUID, NOT NULL, PK)
2. person_id (UUID, NOT NULL)
3. contract_id (UUID, NOT NULL)
4. position (VARCHAR(255), nullable)
5. phone (VARCHAR(15), nullable)
6. email (VARCHAR(255), nullable)
7. created_at (TIMESTAMP WITH TIME ZONE, NOT NULL)
8. updated_at (TIMESTAMP WITH TIME ZONE, NOT NULL)

**Observação:** Não há estrutura de PRD documentada, apenas HML.

---

## 8. CONTRACT_ADDITIONAL_CHARGES

### ✅ ESTRUTURA CORRETA

**No PRD (real):** 7 colunas
**No Dicionário:** Estrutura de HML documentada (parece corresponder)

**Estrutura no PRD:**
1. id (UUID, NOT NULL, PK)
2. contract_id (UUID, NOT NULL)
3. amount (NUMERIC, NOT NULL)
4. charge_type (VARCHAR(50), NOT NULL)
5. billing_model (VARCHAR(50), NOT NULL)
6. created_at (TIMESTAMP WITH TIME ZONE, NOT NULL)
7. updated_at (TIMESTAMP WITH TIME ZONE, NOT NULL)

**Observação:** Não há estrutura de PRD documentada, apenas HML.

---

## RESUMO DE PROBLEMAS ENCONTRADOS

### 🔴 CRÍTICO (causa erro na execução):

1. **contract_scenario_stores**: 
   - Código tenta inserir `removed_at` mas tabela tem `closed_at`
   - **AÇÃO NECESSÁRIA:** Ajustar código para usar `closed_at` ou atualizar tabela

### ⚠️ IMPORTANTE (pode causar problemas):

2. **contracts**: 
   - Faltam colunas documentadas: `end_date`, `observations`, `amount`
   - **AÇÃO NECESSÁRIA:** Documentar estrutura completa de PRD

3. **contract_sellers**: 
   - Falta coluna documentada: `status`
   - **AÇÃO NECESSÁRIA:** Documentar estrutura completa de PRD

### ℹ️ INFORMATIVO (estrutura não documentada para PRD):

4. **contract_team_members**: Apenas HML documentado
5. **contract_contacts**: Apenas HML documentado
6. **contract_partners**: Apenas HML documentado
7. **contract_additional_charges**: Apenas HML documentado

---

## RECOMENDAÇÕES

1. **URGENTE:** Corrigir `contract_scenario_stores` - substituir `removed_at` por `closed_at` no código OU atualizar tabela
2. Documentar estrutura completa de PRD para todas as tabelas no dicionário
3. Verificar se as colunas adicionais (`end_date`, `observations`, `amount` em contracts e `status` em sellers) devem ser populadas na migração
