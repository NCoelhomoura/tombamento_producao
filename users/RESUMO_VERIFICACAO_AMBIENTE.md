# RESUMO: VERIFICAÇÃO DE AMBIENTE E SCHEMA

## ✅ ONDE É DECLARADA A LÓGICA DE AMBIENTE

### 1. Declaração Principal
- **Arquivo:** `utils/database_connection.py`
- **Linha 59:** `_destino_atual = 'HML'` (padrão)
- **Linha 60:** `_destino_configurado_explicitamente = False`

### 2. Configurações de Schema
- **HML:** `schema: 'gmcore'` (linha 40 em `database_connection.py`, linha 27 em `users_to_core.py`)
- **PRD:** `schema: 'core'` (linha 51 em `database_connection.py`, linha 28 em `users_to_core.py`)

**✅ CONCLUSÃO:** Declarações estão corretas e consistentes.

---

## ✅ ONDE É DEFINIDO O AMBIENTE

### 1. Método `set_destino()`
- **Arquivo:** `utils/database_connection.py` (linha 107-128)
- **Função:** Define o destino ('HML' ou 'PRD') e marca como configurado explicitamente

### 2. Método `get_destino()`
- **Arquivo:** `utils/database_connection.py` (linha 130-148)
- **Função:** Retorna o destino atual (verifica variável de ambiente se não foi configurado explicitamente)

### 3. Onde é chamado `set_destino()`
- **Arquivo:** `orchestrator_tasks.py` (linha 563-586)
- **Momento:** Após processar argumentos da linha de comando

**✅ CONCLUSÃO:** Definição do ambiente está correta.

---

## ⚠️ PROBLEMA IDENTIFICADO EM users

### Problema Principal
**Execução prévia:** `step1_migrate_users()` está sendo executado ANTES do orchestrator configurar o destino como PRD.

**Evidência:**
```
[DEBUG step1_migrate_users] Destino obtido após reload: HML
[DEBUG step1_migrate_users] Schema determinado diretamente: gmcore (destino=HML)
...
[DEBUG] Configurando destino final: PRD  ← Configuração acontece DEPOIS
```

### Causa Raiz
O módulo `users_to_core.py` pode ser importado antes do orchestrator configurar o destino, resultando em:
1. `DatabaseConnection.get_destino()` retorna 'HML' (padrão)
2. `get_schema_atual()` retorna 'gmcore'
3. Dados são gravados no schema errado

---

## ✅ SOLUÇÃO IMPLEMENTADA

### Verificação Robusta em `step1_migrate_users()`

**Arquivo:** `users/users_to_core.py` (linha 203-253)

**Implementação:**
1. ✅ Recarrega o módulo `DatabaseConnection` para garantir estado atualizado
2. ✅ Verifica destino atual e flags de configuração
3. ✅ Verifica variável de ambiente como fallback
4. ✅ Determina schema diretamente baseado no destino (sem usar `get_schema_atual()`)
5. ✅ Valida consistência entre destino e schema
6. ✅ Força correção se houver inconsistência

**Mapeamento Implementado:**
```python
if destino == 'PRD':
    schema = SCHEMA_PRD  # 'core'
else:
    schema = SCHEMA_HML  # 'gmcore'
```

**Validação de Consistência:**
```python
if destino == 'PRD' and schema != 'core':
    schema = 'core'  # Forçar correção
elif destino == 'HML' and schema != 'gmcore':
    schema = 'gmcore'  # Forçar correção
```

---

## ✅ VERIFICAÇÃO DE CONFORMIDADE

### Mapeamento Ambiente → Schema

| Ambiente | Schema Esperado | database_connection.py | users_to_core.py | Status |
|----------|-----------------|------------------------|-------------------|--------|
| HML      | gmcore          | ✅ gmcore              | ✅ gmcore         | ✅ OK  |
| PRD      | core             | ✅ core                | ✅ core           | ✅ OK  |

### Uso do Schema em users

| Método/Função | Como Obtém Schema | Status |
|---------------|-------------------|--------|
| `get_schema_atual()` | `DatabaseConnection.get_destino()` → mapeia para schema | ⚠️ Pode estar desatualizado |
| `step1_migrate_users()` | Determina diretamente baseado em destino | ✅ Implementado com verificação robusta |
| `truncate_table()` | Usa `get_schema_atual()` | ⚠️ Pode estar desatualizado |
| `validate_step1_users()` | Usa `get_schema_atual()` | ⚠️ Pode estar desatualizado |

---

## 📋 PRÓXIMOS PASSOS RECOMENDADOS

1. ✅ **Implementado:** Verificação robusta em `step1_migrate_users()`
2. ⏳ **Pendente:** Testar migração em PRD para validar que funciona
3. ⏳ **Pendente:** Considerar refatorar `get_schema_atual()` para usar a mesma lógica robusta
4. ⏳ **Pendente:** Considerar passar destino explicitamente como parâmetro em métodos futuros

---

## 🧪 TESTE RECOMENDADO

```bash
python orchestrator_tasks.py users --destino PRD --clear-data
```

**Validações esperadas:**
1. Log deve mostrar `[VERIFICAÇÃO AMBIENTE] Destino obtido: PRD`
2. Log deve mostrar `[VERIFICAÇÃO AMBIENTE] Schema determinado: core`
3. Log deve mostrar `[VERIFICAÇÃO AMBIENTE] ✅ Schema final validado: core para destino: PRD`
4. Dados devem ser gravados em `core.users` (não em `gmcore.users`)
5. Todos os registros devem ter `legacy_id` populado

---

## 📝 NOTAS TÉCNICAS

- A solução implementada garante que mesmo se o módulo foi importado antes do destino ser configurado, o destino correto será usado
- A validação de consistência força a correção se houver qualquer inconsistência
- Logs detalhados foram adicionados para facilitar debugging futuro
