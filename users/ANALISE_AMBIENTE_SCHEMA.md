# ANÁLISE: LÓGICA DE AMBIENTE E SCHEMA

## 1. ONDE É DECLARADA A LÓGICA DE AMBIENTE

### 1.1. Declaração Principal
**Arquivo:** `utils/database_connection.py`

```python
# Linha 57-60
# Chaveador de destino: 'HML' ou 'PRD'
# Pode ser alterado via variável de ambiente MIGRATION_DESTINO ou método set_destino()
_destino_atual = 'HML'  # Padrão: HML (será sobrescrito por set_destino() ou variável de ambiente)
_destino_configurado_explicitamente = False  # Flag para indicar se foi configurado via set_destino()
```

### 1.2. Configurações de Schema por Ambiente
**Arquivo:** `utils/database_connection.py`

```python
# Linha 37-44: HML
POSTGRESQL_HML_DESTINO_CONFIG = {
    'host': 'apgsql-gmpromo-prd.eastus.cloudapp.azure.com',
    'database': 'supera_dev_seed',
    'schema': 'gmcore',  # ← Schema HML
    ...
}

# Linha 48-55: PRD
POSTGRESQL_PRD_DESTINO_CONFIG = {
    'host': 'gmcore-eks-dev-postgres.ckksg9kcwfzj.us-east-2.rds.amazonaws.com',
    'database': 'gmcoredb',
    'schema': 'core',  # ← Schema PRD
    ...
}
```

### 1.3. Declaração em users
**Arquivo:** `users/users_to_core.py`

```python
# Linha 22-28
SCHEMA_HML = 'gmcore'
SCHEMA_PRD = 'core'

# Linha 31-43
def get_schema_atual():
    """Retorna o schema atual baseado no destino configurado"""
    destino = DatabaseConnection.get_destino()
    if destino == 'PRD':
        return SCHEMA_PRD  # 'core'
    else:
        return SCHEMA_HML  # 'gmcore'
```

---

## 2. ONDE É DEFINIDO O AMBIENTE

### 2.1. Método set_destino()
**Arquivo:** `utils/database_connection.py` (Linha 107-128)

```python
@staticmethod
def set_destino(destino: str):
    """
    Define o destino da migracao: 'HML' ou 'PRD'
    """
    destino_upper = destino.upper()
    if destino_upper not in ['HML', 'PRD']:
        raise ValueError(f"Destino invalido: {destino}. Use 'HML' ou 'PRD'")
    
    # ⚠️ CRÍTICO: Sempre sobrescrever o destino quando set_destino() é chamado
    DatabaseConnection._destino_atual = destino_upper
    DatabaseConnection._destino_configurado_explicitamente = True
```

### 2.2. Método get_destino()
**Arquivo:** `utils/database_connection.py` (Linha 130-148)

```python
@staticmethod
def get_destino():
    """
    Retorna o destino atual configurado
    """
    # ⚠️ CRÍTICO: Se destino foi configurado explicitamente, retornar diretamente
    if DatabaseConnection._destino_configurado_explicitamente:
        return DatabaseConnection._destino_atual
    
    # Variável de ambiente só é verificada se destino NÃO foi configurado explicitamente
    env_destino = os.getenv('MIGRATION_DESTINO', '').upper()
    if env_destino in ['HML', 'PRD']:
        DatabaseConnection._destino_atual = env_destino
    
    return DatabaseConnection._destino_atual
```

### 2.3. Onde é chamado set_destino()
**Arquivo:** `orchestrator_tasks.py` (Linha 563-586)

```python
# Linha 563-586
print(f"\n[DEBUG] Configurando destino final: {destino}")
DatabaseConnection.set_destino(destino)

# Verificar se foi configurado corretamente
destino_verificado = DatabaseConnection.get_destino()
if destino_verificado != destino:
    print(f"\n⚠️ AVISO: Destino verificado ({destino_verificado}) diferente do esperado ({destino})")
    DatabaseConnection.set_destino(destino)
    destino_verificado = DatabaseConnection.get_destino()
```

---

## 3. ANÁLISE EM users

### 3.1. Onde o schema é usado em users

#### 3.1.1. Função get_schema_atual()
**Arquivo:** `users/users_to_core.py` (Linha 31-43)

```python
def get_schema_atual():
    """Retorna o schema atual baseado no destino configurado"""
    destino = DatabaseConnection.get_destino()  # ← Chama get_destino()
    if destino == 'PRD':
        return SCHEMA_PRD  # 'core'
    else:
        return SCHEMA_HML  # 'gmcore'
```

**Problema identificado:**
- Esta função depende de `DatabaseConnection.get_destino()` que pode retornar um valor em cache ou desatualizado se o módulo foi importado antes do destino ser configurado.

#### 3.1.2. Método step1_migrate_users()
**Arquivo:** `users/users_to_core.py` (Linha 203-241)

**Código atual:**
```python
def step1_migrate_users(self):
    # ... código de reload ...
    destino = DatabaseConnection.get_destino()
    
    # Determinar schema diretamente baseado no destino
    if destino == 'PRD':
        schema = 'core'
    else:
        schema = 'gmcore'
    
    # Verificação de inconsistência
    if destino == 'PRD' and schema == 'gmcore':
        schema = 'core'
```

**Problema identificado:**
- Mesmo após reload, o destino ainda pode estar como 'HML' se o módulo foi importado antes do orchestrator configurar o destino.

#### 3.1.3. Outros métodos que usam schema

**truncate_table()** (Linha 122-146):
```python
def truncate_table(self, table_name: str, schema: str = None):
    if schema is None:
        schema = get_schema_atual()  # ← Usa get_schema_atual()
```

**validate_step1_users()** (Linha 149-201):
```python
def validate_step1_users(self):
    destino = DatabaseConnection.get_destino()
    schema = get_schema_atual()  # ← Usa get_schema_atual()
```

**Inserção de dados** (Linha 361, 397, 468):
```python
INSERT INTO {schema}.users (...)  # ← Usa variável schema determinada em step1_migrate_users()
```

---

## 4. PROBLEMAS IDENTIFICADOS

### 4.1. Problema Principal: Execução Prévia
**Sintoma:** `step1_migrate_users()` está sendo executado ANTES do orchestrator configurar o destino como PRD.

**Evidência:**
```
2026-01-16 13:25:33,815 - INFO - [DEBUG step1_migrate_users] Destino obtido após reload: HML
2026-01-16 13:25:33,816 - INFO - [DEBUG step1_migrate_users] Schema determinado diretamente: gmcore (destino=HML)
...
[DEBUG] Configurando destino final: PRD  ← Configuração acontece DEPOIS
```

### 4.2. Problema Secundário: Cache/Estado Desatualizado
**Sintoma:** Mesmo após o orchestrator configurar o destino como PRD, quando `step1_migrate_users()` é executado novamente, ainda retorna HML.

**Causa possível:**
- O módulo `users_to_core.py` foi importado antes do destino ser configurado
- O reload do módulo não está funcionando corretamente
- Há alguma execução automática durante a importação

### 4.3. Inconsistência na Determinação do Schema
**Problema:** 
- `get_schema_atual()` depende de `DatabaseConnection.get_destino()` que pode estar desatualizado
- `step1_migrate_users()` tenta determinar o schema diretamente, mas ainda depende de `get_destino()`

---

## 5. VERIFICAÇÃO DE CONFORMIDADE

### 5.1. Mapeamento Ambiente → Schema

| Ambiente | Schema Esperado | Schema em database_connection.py | Schema em users_to_core.py | Status |
|----------|-----------------|----------------------------------|----------------------------|--------|
| HML      | gmcore          | ✅ gmcore (linha 40)             | ✅ gmcore (linha 27)       | ✅ OK  |
| PRD      | core             | ✅ core (linha 51)               | ✅ core (linha 28)         | ✅ OK  |

**Conclusão:** Os mapeamentos estão corretos e consistentes.

### 5.2. Fluxo de Definição de Ambiente

```
orchestrator_tasks.py (main)
    ↓
    Processa argumentos --destino PRD
    ↓
    DatabaseConnection.set_destino('PRD')
    ↓
    _destino_atual = 'PRD'
    _destino_configurado_explicitamente = True
    ↓
    run_users_task()
    ↓
    Importa UsersMigration
    ↓
    migration.step1_migrate_users()
    ↓
    DatabaseConnection.get_destino()  ← Deve retornar 'PRD'
    ↓
    get_schema_atual() ou determinação direta
    ↓
    Schema = 'core' (se PRD) ou 'gmcore' (se HML)
```

**Problema:** O fluxo está correto, mas há uma execução prévia que acontece antes do orchestrator configurar o destino.

---

## 6. SOLUÇÕES PROPOSTAS

### 6.1. Solução Imediata: Forçar Verificação do Destino
**Arquivo:** `users/users_to_core.py`

Modificar `step1_migrate_users()` para:
1. Recarregar o módulo `DatabaseConnection` antes de verificar o destino
2. Verificar variável de ambiente como fallback
3. Determinar schema diretamente baseado no destino obtido
4. Adicionar validação de inconsistência

### 6.2. Solução de Longo Prazo: Passar Destino Explicitamente
**Arquivo:** `users/users_to_core.py` e `orchestrator_tasks.py`

Modificar para passar o destino como parâmetro:
```python
def step1_migrate_users(self, destino_explicito: str = None):
    if destino_explicito:
        destino = destino_explicito
    else:
        destino = DatabaseConnection.get_destino()
    schema = 'core' if destino == 'PRD' else 'gmcore'
```

### 6.3. Solução Preventiva: Garantir Ordem de Execução
**Arquivo:** `orchestrator_tasks.py`

Garantir que o destino seja configurado ANTES de importar qualquer módulo de migração.

---

## 7. RECOMENDAÇÕES

1. ✅ **Imediato:** Implementar verificação forçada do destino em `step1_migrate_users()`
2. ✅ **Curto Prazo:** Adicionar logs detalhados para rastrear quando e onde o destino é definido
3. ✅ **Médio Prazo:** Refatorar para passar destino explicitamente como parâmetro
4. ✅ **Longo Prazo:** Implementar um sistema de configuração centralizado que garanta a ordem de execução

---

## 8. PRÓXIMOS PASSOS

1. Implementar a solução imediata (forçar verificação do destino)
2. Adicionar logs detalhados
3. Testar a migração em PRD
4. Validar que os dados foram gravados corretamente em `core.users`
