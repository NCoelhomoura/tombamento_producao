# ANÁLISE CRITERIOSA DE PERFORMANCE - STORES MIGRATION

## TEMPOS OBSERVADOS (do log_execution.txt)

### ETAPA 1 - Store Segments (14 registros)
- Não há logs específicos, mas deve ser rápida (< 1 minuto)

### ETAPA 2 - Retail Chains (6.018 registros)
- Não há logs específicos no arquivo fornecido

### ETAPA 3 - Store Brands (6.914 registros)
- **Início**: 12:33:18
- **Fim**: 12:46:58
- **Duração Total**: 13 minutos 40 segundos
- **Performance**: ~8,5 registros/segundo

**Breakdown estimado:**
- Truncate: ~1 segundo
- Carregar mapeamentos (2 queries): ~2-3 segundos
- Query SQL Server (bandeira_canal_map): ~2-3 segundos
- Query SQL Server (Bandeira - 6.914 linhas): ~5-10 segundos
- Processamento DataFrame: ~1-2 segundos
- Inserção (1 chunk de 6.914): **~13 minutos** ⚠️ GARGALO

### ETAPA 4 - Stores (40.621 registros)
- **Início**: ~13:08:19
- **Chunk 1** (20.000): 13:08:19 → 13:48:13 = **40 minutos** ⚠️ GARGALO CRÍTICO
- **Chunk 2** (20.000): 13:48:13 → 13:49:26 = 1 minuto 13 segundos
- **Chunk 3** (6.621): 13:49:26 → 13:49:26 = <1 segundo
- **Duração Total**: ~41 minutos
- **Performance média**: ~16,5 registros/segundo

### ETAPA 5 - Store CNPJs (40.607 registros)
- **Início**: 13:49:28
- **Chunk 1** (20.000): 13:49:28 → 14:29:05 = **39 minutos 37 segundos** ⚠️ GARGALO CRÍTICO
- **Performance**: ~8,4 registros/segundo

---

## ANÁLISE DETALHADA DO CÓDIGO

### PROBLEMA 1: Query de Lookup Após Cada Chunk (ETAPA 4)

**Código (linhas 1414-1423):**
```python
if include_legacy and legacy_ids_chunk:
    cursor_pg.execute(f"""
        SELECT id, legacy_id 
        FROM {schema}.stores 
        WHERE legacy_id = ANY(%s)
        ORDER BY legacy_id
    """, (legacy_ids_chunk,))
    for uuid_row, leg_id in cursor_pg.fetchall():
        self.store_id_map[leg_id] = uuid_row
```

**Problemas identificados:**
1. **Query executada após CADA chunk de 20.000 registros**
2. **`WHERE legacy_id = ANY(%s)` com array de 20.000 elementos** - pode ser lento sem índice
3. **`ORDER BY legacy_id`** - ordenação desnecessária se não há índice
4. **Loop linha a linha** para popular o map (embora seja rápido)

**Impacto estimado:**
- Se a query leva 30-35 segundos por chunk × 2 chunks = 60-70 segundos
- Mas o chunk 1 leva 40 minutos, então há algo mais...

### PROBLEMA 2: Commit Após Cada Chunk

**Código (linha 1440):**
```python
conn_pg.commit()
```

**Problemas identificados:**
1. **Commit após cada chunk de 20.000 registros**
2. **Força flush de WAL (Write-Ahead Log)**
3. **Pode causar lock contention** se houver outras operações

**Impacto estimado:**
- Commit de 20.000 registros pode levar 5-10 segundos
- Mas não explica 40 minutos...

### PROBLEMA 3: executemany() com gen_random_uuid()

**Código (linha 1412):**
```python
cursor_pg.executemany(insert_query, batch_values)
```

**Query:**
```sql
INSERT INTO stores (id, name, store_brand_id, is_active, created_at, updated_at, legacy_id)
VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s)
```

**Problemas identificados:**
1. **`gen_random_uuid()` chamado para CADA linha** - pode ser custoso
2. **Sem uso de COPY ou bulk insert** - executemany() ainda faz múltiplas inserções
3. **Foreign key constraint em `store_brand_id`** - verificação a cada insert

**Impacto estimado:**
- `gen_random_uuid()` é rápido (~microsegundos), mas × 20.000 = alguns segundos
- Verificação de FK pode ser custosa se não houver índice

### PROBLEMA 4: Conversão DataFrame → Lista de Dicionários

**Código (linha 1022):**
```python
processed_data = df[['legacy_id', 'nome', ...]].to_dict('records')
```

**Problemas identificados:**
1. **`to_dict('records')` cria lista de dicionários** - overhead de memória
2. **Depois converte para tuplas no loop** (linha 1390-1408)
3. **Dupla conversão desnecessária**

**Impacto estimado:**
- Para 6.914 registros: ~1-2 segundos
- Para 40.621 registros: ~5-10 segundos

### PROBLEMA 5: Processamento DataFrame com Múltiplas Operações

**Código (linhas 989-1019):**
```python
df['legacy_id'] = df['Id']
df['nome'] = self.clean_string_vectorized(df['NomeFantasia'])
df['retail_chain_id'] = df['IdRede'].map(retail_chain_map)
df['canal_id'] = df['Id'].map(bandeira_canal_map)
df['store_segment_id'] = df['canal_id'].map(store_segment_map)
# ... mais operações
```

**Problemas identificados:**
1. **Múltiplas passadas sobre o DataFrame** - cada operação cria cópia
2. **`.map()` pode ser lento para grandes DataFrames**
3. **`.astype(str)` cria nova série**

**Impacto estimado:**
- Para 6.914 registros: ~1-2 segundos (aceitável)
- Para 40.621 registros: ~5-10 segundos (ainda aceitável)

---

## GARGALOS IDENTIFICADOS (por ordem de impacto)

### 🔴 CRÍTICO 1: Query de Lookup Lenta (ETAPA 4)
- **Onde**: Linha 1416-1421
- **Quando**: Após cada chunk de 20.000 registros
- **Problema**: `WHERE legacy_id = ANY(%s)` sem índice ou com índice ineficiente
- **Impacto**: 30-40 minutos por chunk
- **Solução**: 
  - Criar índice em `stores.legacy_id`
  - Ou usar `RETURNING` na inserção

### 🔴 CRÍTICO 2: executemany() Lento (ETAPA 4 e 5)
- **Onde**: Linha 1412 (ETAPA 4), linha 1681 (ETAPA 5)
- **Quando**: Durante inserção de cada chunk
- **Problema**: 
  - `executemany()` ainda faz múltiplas inserções individuais
  - Verificação de FK a cada insert
  - `gen_random_uuid()` chamado múltiplas vezes
- **Impacto**: 30-40 minutos por chunk de 20.000
- **Solução**: 
  - Usar `COPY FROM` do PostgreSQL
  - Ou usar `execute_values()` do psycopg2.extras
  - Ou inserir em uma única transação sem commits intermediários

### 🟡 MÉDIO 1: Commit Após Cada Chunk
- **Onde**: Linha 1440
- **Quando**: Após cada chunk de 20.000 registros
- **Problema**: Flush de WAL e possível lock contention
- **Impacto**: 5-10 segundos por chunk
- **Solução**: Commitar em chunks maiores ou no final

### 🟡 MÉDIO 2: Conversão DataFrame → Dict → Tuples
- **Onde**: Linhas 1022 e 1390-1408
- **Quando**: Durante processamento
- **Problema**: Dupla conversão desnecessária
- **Impacto**: 5-10 segundos para 40.621 registros
- **Solução**: Converter diretamente para tuplas

### 🟢 BAIXO: Processamento DataFrame
- **Onde**: Linhas 989-1019
- **Quando**: Durante processamento
- **Problema**: Múltiplas passadas sobre DataFrame
- **Impacto**: 5-10 segundos para 40.621 registros
- **Solução**: Otimizar operações (aceitável como está)

---

## RECOMENDAÇÕES PRIORITÁRIAS

### 1. URGENTE: Usar COPY ou execute_values() para inserção
- Substituir `executemany()` por `psycopg2.extras.execute_values()`
- Ou usar `COPY FROM` com StringIO
- **Ganho esperado**: 10-20x mais rápido (de 40 min para 2-4 min)

### 2. URGENTE: Criar índices no banco de destino
```sql
CREATE INDEX IF NOT EXISTS idx_stores_legacy_id ON gmcore.stores(legacy_id);
CREATE INDEX IF NOT EXISTS idx_store_cnpjs_store_id ON gmcore.store_cnpjs(store_id);
CREATE INDEX IF NOT EXISTS idx_stores_store_brand_id ON gmcore.stores(store_brand_id);
```

### 3. URGENTE: Usar RETURNING na inserção (ETAPA 4)
- Em vez de query separada após insert, usar `RETURNING id, legacy_id`
- **Ganho esperado**: Eliminar query de lookup (economia de 30-40 min)

### 4. MÉDIO: Reduzir frequência de commits
- Commitar a cada 50.000-100.000 registros em vez de 20.000
- **Ganho esperado**: 10-20 segundos por etapa

### 5. MÉDIO: Otimizar conversão de dados
- Converter DataFrame diretamente para lista de tuplas
- **Ganho esperado**: 5-10 segundos por etapa

---

## PERFORMANCE ESPERADA APÓS OTIMIZAÇÕES

### ETAPA 3 (6.914 registros)
- **Atual**: 13 minutos 40 segundos
- **Esperado**: 30-60 segundos
- **Melhoria**: 13-27x mais rápido

### ETAPA 4 (40.621 registros)
- **Atual**: 41 minutos
- **Esperado**: 2-4 minutos
- **Melhoria**: 10-20x mais rápido

### ETAPA 5 (40.607 registros)
- **Atual**: 39+ minutos (ainda processando)
- **Esperado**: 2-4 minutos
- **Melhoria**: 10-20x mais rápido

---

## CONCLUSÃO

O principal gargalo está na **inserção de dados** usando `executemany()`, que é ineficiente para grandes volumes. A solução é usar `execute_values()` ou `COPY FROM`, que são métodos otimizados para bulk insert no PostgreSQL.

O segundo gargalo é a **query de lookup após cada chunk** na ETAPA 4, que pode ser eliminada usando `RETURNING` na inserção.

Com essas otimizações, a migração de milhões de linhas será viável.



