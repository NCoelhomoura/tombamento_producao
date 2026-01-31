# PROBLEMA IDENTIFICADO - Associação de IdOrcamentoLoja aos Scenarios

## Análise do Problema

### Situação Atual

**Scenario migrado (`042b9d01-e069-4eb0-8440-dc92ea3ba08a`):**
- Frequency: `once_per_week` (Frequencia: 6)
- Hours: `1.0`
- Hour Value: `38.18`
- Start Date: `2025-02-01`
- End Date: `None`

**Na origem, para os mesmos `IdOrcamentoLoja` (477052, 477102, 678722):**
- **20 registros diferentes** encontrados
- Valores variam significativamente:
  - Horas: `1.5`, `3.5`, `5`, `6`, `7` (não apenas `1.0`)
  - ValorHora: `35.82`, `35.88`, `35.93`, `38.18`
  - DataInicioOperacao: desde `2021-01-01` até `2025-02-01`
  - DataAvisoPrevio: variam ou são NULL

### Problema Identificado

O código está associando **TODOS os registros históricos** de um `IdOrcamentoLoja` ao mesmo scenario, mesmo quando esses registros têm valores diferentes de `Frequencia`, `Horas`, `ValorHora`, `DataInicioOperacao` e `DataAvisoPrevio`.

**Causa Raiz:**

1. **STEP 2** (`step2_migrate_contract_scenarios`):
   - Faz `SELECT DISTINCT` na query SQL
   - Aplica `drop_duplicates` no DataFrame
   - Cria apenas cenários únicos baseados na combinação: `IdOrcamento`, `Frequencia`, `Horas`, `ValorHora`, `DataInicioOperacao`, `DataAvisoPrevio`
   - Cria o `scenario_id_map` mapeando cada combinação única para um UUID

2. **STEP 3** (`step3_migrate_contract_scenario_stores`):
   - Busca **TODOS os registros** da origem (sem DISTINCT)
   - Para cada registro, cria um `scenario_key` baseado na mesma combinação
   - Tenta mapear o `scenario_key` para o `scenario_id_map`
   - **PROBLEMA**: Se o `scenario_key` não for encontrado no `scenario_id_map`, o código deveria filtrar esse registro, mas parece que está associando incorretamente

### Evidência do Problema

Da análise executada:
- Apenas **3 registros** na origem realmente correspondem ao scenario migrado:
  - IdOrcamentoLoja: 477052, Horas: 3.5, ValorHora: 38.18, DataInicioOperacao: 2025-02-01
  - IdOrcamentoLoja: 477102, Horas: 3.5, ValorHora: 38.18, DataInicioOperacao: 2025-02-01
  - IdOrcamentoLoja: 678722, Horas: 3.5, ValorHora: 38.18, DataInicioOperacao: 2025-02-01

- Mas **20 registros** foram associados ao mesmo scenario, incluindo registros históricos com valores diferentes

### Possíveis Causas

1. **Normalização inconsistente**: As funções `create_scenario_key_from_df` (step2) e `create_scenario_key` (step3) podem estar normalizando os valores de forma diferente
2. **Problema no mapeamento**: O `scenario_id_map` pode estar retornando o mesmo UUID para chaves diferentes
3. **Filtro não funcionando**: O código que filtra registros sem `scenario_id` válido pode não estar funcionando corretamente

## Solução Proposta

### Opção 1: Garantir que apenas registros que correspondem exatamente aos scenarios sejam associados

No `step3`, após criar o `scenario_key` e mapear para `scenario_id`, adicionar validação adicional:

```python
# Após linha 2937: df['scenario_id'] = df['scenario_key'].map(self.scenario_id_map)

# Validar que o scenario_key realmente corresponde ao scenario_id encontrado
# Buscar os valores do scenario do banco e comparar
```

### Opção 2: Aplicar DISTINCT também no step3

No `step3`, aplicar a mesma lógica de DISTINCT do step2 antes de associar aos scenarios:

```python
# Após criar o DataFrame, aplicar drop_duplicates baseado na mesma combinação única
distinct_cols = ['IdOrcamento', 'Frequencia', 'Horas', 'ValorHora', 'DataInicioOperacao', 'DataAvisoPrevio']
# Normalizar e fazer DISTINCT antes de criar scenario_key
```

### Opção 3: Validar correspondência exata

Após mapear `scenario_id`, validar que os valores realmente correspondem:

```python
# Para cada registro com scenario_id válido, buscar o scenario do banco
# Comparar os valores normalizados e filtrar se não corresponderem exatamente
```

## Próximos Passos

1. Verificar se há diferença na normalização entre step2 e step3
2. Adicionar logs detalhados para rastrear qual `scenario_key` está sendo criado e qual `scenario_id` está sendo retornado
3. Implementar validação adicional para garantir correspondência exata
