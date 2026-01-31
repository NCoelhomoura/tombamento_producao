# Script de Análise de Discrepâncias - Contract Scenarios

Este script analisa discrepâncias entre os dados migrados em `contract_scenarios` e os dados da origem (`ViewOrcamentosLojas`).

## Como usar:

```bash
python analyze_scenario_discrepancies.py <scenario_id> [id_orcamento]
```

### Parâmetros:
- `scenario_id` (obrigatório): UUID do scenario em `contract_scenarios` que você quer analisar
- `id_orcamento` (opcional): IdOrcamento para filtrar na origem (ajuda a garantir que está buscando os dados corretos)

### Exemplo:

```bash
python analyze_scenario_discrepancies.py 042b9d01-e069-4eb0-8440-dc92ea3ba08a 6053
```

## O que o script faz:

1. **Busca o scenario no destino** (`contract_scenarios`)
   - Obtém: `frequency`, `hours`, `hour_value`, `start_date`, `end_date`

2. **Busca os `legacy_id` relacionados** (`contract_scenario_stores`)
   - Obtém todos os `IdOrcamentoLoja` (legacy_id) associados ao scenario

3. **Busca os dados na origem** (`ViewOrcamentosLojas`)
   - Busca os registros correspondentes aos `IdOrcamentoLoja` encontrados
   - Obtém: `Frequencia`, `Horas`, `ValorHora`, `DataInicioOperacao`, `DataAvisoPrevio`

4. **Compara os valores**
   - Normaliza os valores usando a mesma lógica do código de migração
   - Identifica discrepâncias entre origem e destino

5. **Gera relatório**
   - Exibe no console
   - Salva em arquivo `scenario_analysis_<scenario_id>_<timestamp>.txt`

## Saída do script:

O script mostra:
- ✅ Valores encontrados no destino (`contract_scenarios`)
- ✅ Total de registros na origem
- ✅ Total de registros com discrepâncias
- ⚠️ Detalhes de cada discrepância encontrada
- 📊 Tabela completa com todos os dados da origem

## Exemplo de saída:

```
====================================================================================================
ANÁLISE DE DISCREPÂNCIAS - SCENARIO_ID: 042b9d01-e069-4eb0-8440-dc92ea3ba08a
====================================================================================================

[1] Buscando dados em contract_scenarios (destino)...
✓ Scenario encontrado:
  - Contract ID: <uuid>
  - Frequency: Mensal
  - Hours: 40
  - Hour Value: 50.0
  - Start Date: 2026-01-15
  - End Date: 2026-12-31

[2] Buscando legacy_id (IdOrcamentoLoja) em contract_scenario_stores...
✓ Encontrados 3 registros em contract_scenario_stores
✓ Legacy IDs (IdOrcamentoLoja): [477052, 477102, 678722]

[3] Buscando dados na origem (ViewOrcamentosLojas)...
✓ Encontrados 3 registros na origem

[4] Comparando dados e identificando discrepâncias...

====================================================================================================
RELATÓRIO DE ANÁLISE
====================================================================================================

⚠️ DISCREPÂNCIAS ENCONTRADAS:
----------------------------------------------------------------------------------------------------

IdOrcamentoLoja: 477052 | IdOrcamento: 6053 | IdEstabelecimento: 123
  ❌ Frequencia:
     Origem: Semanal (normalizado: 'Semanal')
     Destino: Mensal (normalizado: 'Mensal')
  ❌ Horas:
     Origem: 20 (normalizado: '20')
     Destino: 40 (normalizado: '40')
```

## Observações:

- O script usa a mesma lógica de normalização do código de migração para garantir comparação consistente
- O script conecta automaticamente ao ambiente PRD
- O relatório é salvo no mesmo diretório do script
