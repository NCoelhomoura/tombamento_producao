# MAPEAMENTO DE ALTERAÇÕES - FILTRO `--status-pedido`

## RESUMO
Adicionar filtro `--status-pedido` que aceita valores INTEGER separados por vírgula (ex: `--status-pedido 6,7,8` ou `--status-pedido 6`).
- Se NÃO existir o comando: trazer tudo (sem filtro)
- Se existir: filtrar `StatusPedido IN (valores)` na coluna `StatusPedido` da `ViewOrcamentosLojas`
- Seguir a mesma estrutura de `DataInicioOperacao` e `DataAvisoPrevio`

---

## ARQUIVOS QUE PRECISAM DE ALTERAÇÕES

### 1. `orchestrator_tasks.py`

#### 1.1. Processamento de Argumentos (linha ~567)
**Localização**: Função `main()`, loop de processamento de argumentos

**Alteração necessária**:
- Adicionar processamento do argumento `--status-pedido` após `--data-inicio-operacao`
- Aceitar valores separados por vírgula e converter para lista de inteiros
- Armazenar em variável `status_pedido_filter` (lista de inteiros ou None)

**Código a adicionar** (após linha ~593):
```python
elif arg == '--status-pedido' and i + 1 < len(sys.argv):
    # Aceitar lista separada por vírgula: --status-pedido 6,7,8
    status_pedido_str = sys.argv[i + 1]
    status_pedido_filter = [int(x.strip()) for x in status_pedido_str.split(',')]
    i += 2
```

#### 1.2. Inicialização da Variável (linha ~560)
**Localização**: Declaração de variáveis de filtros

**Alteração necessária**:
- Adicionar `status_pedido_filter = None` junto com outras variáveis de filtro

#### 1.3. Passagem de Parâmetros para Funções
**Localização**: Chamadas de `run_customers_task()`, `run_stores_task()`, `run_contracts_task()`

**Alterações necessárias**:
- Adicionar parâmetro `status_pedido_filter=status_pedido_filter` em todas as chamadas:
  - `run_customers_task()` (linhas ~681, ~717, ~743, ~748, ~756, ~764)
  - `run_stores_task()` (linhas ~687, ~723, ~780, ~788, ~796, ~804)
  - `run_contracts_task()` (linhas ~710, ~730, ~841)

#### 1.4. Assinaturas das Funções
**Localização**: Definições de `run_customers_task()`, `run_stores_task()`, `run_contracts_task()`

**Alterações necessárias**:
- Adicionar parâmetro `status_pedido_filter=None` nas assinaturas:
  - `run_customers_task()` (linha ~44)
  - `run_stores_task()` (linha ~167)
  - `run_contracts_task()` (linha ~289)

#### 1.5. Passagem para Instâncias de Migração
**Localização**: Criação de instâncias `CustomersMigration`, `StoresMigration`, `ContractsMigration`

**Alterações necessárias**:
- Adicionar `status_pedido_filter=status_pedido_filter` nos construtores:
  - `CustomersMigration()` (linha ~79)
  - `StoresMigration()` (linha ~199)
  - `ContractsMigration()` (linha ~329)

---

### 2. `contracts/contracts_to_core.py`

#### 2.1. Construtor `__init__` (linha ~173)
**Alteração necessária**:
- Adicionar parâmetro `status_pedido_filter=None` na assinatura
- Adicionar `self.status_pedido_filter = status_pedido_filter if status_pedido_filter else []` (linha ~205)

#### 2.2. Método `has_filters()` (linha ~227)
**Alteração necessária**:
- Adicionar verificação: `len(self.status_pedido_filter) > 0` no return

#### 2.3. Método `save_filter_json()` (linha ~320)
**Alteração necessária**:
- Adicionar no dicionário `filters_applied`:
  ```python
  'status_pedido': self.status_pedido_filter if self.status_pedido_filter else None,
  ```

#### 2.4. Método `load_filter_json()` (linha ~362)
**Alteração necessária**:
- Não precisa de alteração (já carrega o JSON completo)

#### 2.5. Query Principal - `step1_migrate_contracts()` (linha ~792)
**Localização**: Construção da query SQL principal

**Alteração necessária**:
- Adicionar filtro após `DataInicioOperacao` (linha ~840):
  ```python
  # Filtro StatusPedido
  if len(self.status_pedido_filter) > 0:
      placeholders = ','.join(['?' for _ in self.status_pedido_filter])
      where_conditions.append(f"v.StatusPedido IN ({placeholders})")
      query_params.extend(self.status_pedido_filter)
  ```

#### 2.6. Query de Contagem - `step1_migrate_contracts()` (linha ~485)
**Localização**: Query de contagem antes da migração

**Alteração necessária**:
- Adicionar o mesmo filtro StatusPedido após `DataInicioOperacao` (linha ~511)

#### 2.7. Queries de Coleta de IDs - `step1_migrate_contracts()` (linha ~887)
**Localização**: Queries que coletam IDs únicos da ViewOrcamentosLojas

**Alteração necessária**:
- Adicionar filtro StatusPedido em todas as queries que usam `where_conditions_ids`:
  - Query de IdCliente (linha ~920)
  - Query de IdEstabelecimento (linha ~950)
  - Query de IdBandeira (linha ~980)
  - Query de IdRede (linha ~1010)

#### 2.8. JSON Temporário - `step1_migrate_contracts()` (linha ~705)
**Alteração necessária**:
- Adicionar `'status_pedido': self.status_pedido_filter if self.status_pedido_filter else None,` no `temp_filter_data['filters_applied']`

#### 2.9. Logs de Filtros - `step1_migrate_contracts()` (linha ~857)
**Alteração necessária**:
- Adicionar `StatusPedido={self.status_pedido_filter}` nos logs

#### 2.10. Outras Etapas que Usam ViewOrcamentosLojas
**Localização**: Verificar todas as etapas que fazem queries na ViewOrcamentosLojas

**Etapas a verificar**:
- `step2_migrate_contract_scenarios()` - Verificar se usa filtros de data
- `step9_migrate_promoter_tasks()` - Verificar se usa filtros de data
- Qualquer outra etapa que use `load_filter_json()` e aplique filtros

**Alteração necessária**:
- Adicionar filtro StatusPedido em todas as queries que aplicam filtros de data

---

### 3. `stores/stores_to_core.py`

#### 3.1. Construtor `__init__` (linha ~84)
**Alteração necessária**:
- Adicionar parâmetro `status_pedido_filter=None` na assinatura
- Adicionar `self.status_pedido_filter = status_pedido_filter if status_pedido_filter else []` (linha ~102)

#### 3.2. Método `save_filter_json_from_view()` (linha ~145)
**Alteração necessária**:
- Carregar `status_pedido_filter` do JSON se não vier do CMD (linha ~160):
  ```python
  status_pedido_filter = self.status_pedido_filter
  
  # Se CMD não especificou filtros, usar do JSON se existir
  if len(status_pedido_filter) == 0 and existing_data and 'filters_applied' in existing_data:
      filters = existing_data['filters_applied']
      json_status_pedido = filters.get('status_pedido')
      if json_status_pedido:
          status_pedido_filter = json_status_pedido
  ```
- Adicionar filtro nas queries SQL (linha ~180):
  ```python
  if len(status_pedido_filter) > 0:
      placeholders = ','.join(['?' for _ in status_pedido_filter])
      where_conditions.append(f"v.StatusPedido IN ({placeholders})")
      query_params.extend(status_pedido_filter)
  ```
- Adicionar no log (linha ~174):
  ```python
  logger.info(f"[STORES] Filtros aplicados em save_filter_json_from_view: ..., status_pedido={status_pedido_filter}")
  ```

#### 3.3. Método `step2_migrate_retail_chains()` (linha ~1069)
**Alteração necessária**:
- Carregar `status_pedido_filter` do JSON se não vier do CMD (similar a data_aviso_previo)
- Adicionar filtro na query SQL (linha ~1095):
  ```python
  if len(status_pedido_filter) > 0:
      placeholders = ','.join(['?' for _ in status_pedido_filter])
      where_conditions.append(f"v.StatusPedido IN ({placeholders})")
      query_params.extend(status_pedido_filter)
  ```

#### 3.4. Método `step3_migrate_store_brands()` (linha ~1512)
**Alteração necessária**:
- Mesma lógica do step2: carregar do JSON e adicionar filtro na query SQL

#### 3.5. Método `step4_migrate_stores()` (linha ~2093)
**Alteração necessária**:
- Mesma lógica dos steps anteriores: carregar do JSON e adicionar filtro na query SQL

---

### 4. `customers/customers_to_core.py`

#### 4.1. Construtor `__init__` (linha ~85)
**Alteração necessária**:
- Adicionar parâmetro `status_pedido_filter=None` na assinatura
- Adicionar `self.status_pedido_filter = status_pedido_filter if status_pedido_filter else []` (linha ~101)

#### 4.2. Método `save_filter_json_from_view()` (linha ~279)
**Alteração necessária**:
- Mesma lógica de stores: carregar do JSON se não vier do CMD
- Adicionar filtro nas queries SQL (linha ~323):
  ```python
  if len(status_pedido_filter) > 0:
      placeholders = ','.join(['?' for _ in status_pedido_filter])
      where_conditions.append(f"v.StatusPedido IN ({placeholders})")
      query_params.extend(status_pedido_filter)
  ```

#### 4.3. Método `step2_migrate_customers()` (linha ~1006)
**Alteração necessária**:
- Carregar `status_pedido_filter` do JSON se não vier do CMD (similar a data_aviso_previo)
- Adicionar filtro na query SQL onde aplica filtros de data

#### 4.4. Método `step5_migrate_customer_brands()` (linha ~2200)
**Alteração necessária**:
- Mesma lógica: carregar do JSON e adicionar filtro na query SQL

#### 4.5. Método `step6_migrate_customer_customer_brand()` (linha ~2453)
**Alteração necessária**:
- Mesma lógica: carregar do JSON e adicionar filtro na query SQL

---

### 5. `contracts/contracts_filter_main.json`

#### 5.1. Estrutura JSON
**Alteração necessária**:
- Adicionar campo `status_pedido` no objeto `filters_applied`:
  ```json
  "filters_applied": {
    "id_orcamento": [],
    "data_aviso_previo_min": "2025-01-01",
    "data_inicio_operacao_max": "2026-01-21",
    "status_pedido": null,
    "limit_rows": 0,
    "clear_data": true
  }
  ```

**Nota**: O campo será preenchido automaticamente quando o filtro for usado. Quando não usado, será `null`.

---

## RESUMO DE ALTERAÇÕES POR ARQUIVO

| Arquivo | Linhas Aproximadas | Tipo de Alteração |
|---------|-------------------|-------------------|
| `orchestrator_tasks.py` | ~560, ~567, ~593, ~44, ~79, ~167, ~199, ~289, ~329, ~681, ~687, ~710, ~717, ~723, ~730, ~743, ~748, ~756, ~764, ~780, ~788, ~796, ~804, ~841 | Processamento de argumentos, passagem de parâmetros |
| `contracts/contracts_to_core.py` | ~173, ~205, ~227, ~320, ~485, ~705, ~792, ~840, ~857, ~887, ~920, ~950, ~980, ~1010, + outras etapas | Construtor, filtros, queries SQL, JSON, logs |
| `stores/stores_to_core.py` | ~84, ~102, ~145, ~160, ~174, ~180, ~1069, ~1095, ~1512, ~1531, ~2093, ~2112 | Construtor, filtros, queries SQL |
| `customers/customers_to_core.py` | ~85, ~101, ~279, ~323, ~1006, ~2200, ~2453 | Construtor, filtros, queries SQL |
| `contracts/contracts_filter_main.json` | ~5 | Estrutura JSON |

---

## LÓGICA DE IMPLEMENTAÇÃO

### Prioridade de Filtros (CMD > JSON > None)
1. **Linha de comando (CMD)**: Se `--status-pedido` foi especificado, usar esses valores
2. **JSON**: Se CMD não especificou mas JSON tem `status_pedido`, usar do JSON
3. **None**: Se nenhum dos dois, não aplicar filtro (trazer tudo)

### Formato do Filtro SQL
```sql
WHERE v.StatusPedido IN (?, ?, ?)
```
Com parâmetros: `[6, 7, 8]` (lista de inteiros)

### Estrutura no JSON
```json
{
  "filters_applied": {
    "status_pedido": [6, 7, 8]  // ou null se não aplicado
  }
}
```

---

## VALIDAÇÕES NECESSÁRIAS

1. ✅ Verificar que `StatusPedido` existe na `ViewOrcamentosLojas` (confirmado)
2. ✅ Verificar que é INTEGER (confirmado)
3. ✅ Verificar que aceita múltiplos valores separados por vírgula
4. ✅ Verificar que funciona com um único valor
5. ✅ Verificar que funciona sem o filtro (trazer tudo)
6. ✅ Verificar que segue o mesmo padrão de `DataInicioOperacao` e `DataAvisoPrevio`

---

## OBSERVAÇÕES IMPORTANTES

1. **Não usar TRUNCATE quando há filtros**: O filtro `status_pedido` deve ser considerado no método `has_filters()` para evitar TRUNCATE quando filtros estão aplicados.

2. **Consistência entre módulos**: Todos os módulos (contracts, stores, customers) devem aplicar o mesmo filtro quando consultam `ViewOrcamentosLojas`.

3. **Logs**: Adicionar `status_pedido` em todos os logs que mostram filtros aplicados.

4. **Documentação**: Atualizar `README.md` com exemplo de uso do novo filtro.

---

## PRÓXIMOS PASSOS

1. ✅ Mapeamento completo (este documento)
2. ⏳ Implementar alterações no código
3. ⏳ Testar com valores únicos e múltiplos
4. ⏳ Testar sem o filtro (comportamento padrão)
5. ⏳ Validar que funciona em conjunto com outros filtros
6. ⏳ Atualizar documentação (README.md)
