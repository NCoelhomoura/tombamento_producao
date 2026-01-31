"""
Script para analisar discrepâncias entre contract_scenarios migrados e dados da origem.

Este script:
1. Busca um scenario_id específico em contract_scenarios (destino)
2. Busca os legacy_id (IdOrcamentoLoja) relacionados em contract_scenario_stores
3. Busca esses IdOrcamentoLoja na origem (ViewOrcamentosLojas)
4. Compara os valores de Frequencia, Horas, ValorHora, DataInicioOperacao, DataAvisoPrevio
5. Gera um relatório detalhado das discrepâncias
"""

import sys
import os
import pandas as pd
from datetime import datetime

# Adicionar diretório raiz do projeto ao path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Adicionar diretório utils ao path
utils_path = os.path.join(project_root, 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from utils.database_connection import DatabaseConnection

# Configurar destino como PRD
DatabaseConnection.set_destino('PRD')

def get_schema_atual():
    """Retorna o schema atual baseado no destino configurado"""
    destino = DatabaseConnection.get_destino()
    if destino == 'PRD':
        return 'commercial'
    else:
        return 'gmcommercial'

def analyze_scenario(scenario_id: str, id_orcamento: int = None):
    """
    Analisa um scenario_id específico e compara com os dados da origem.
    
    Args:
        scenario_id: UUID do scenario em contract_scenarios
        id_orcamento: IdOrcamento para filtrar na origem (opcional)
    """
    print("="*100)
    print(f"ANÁLISE DE DISCREPÂNCIAS - SCENARIO_ID: {scenario_id}")
    print("="*100)
    print(f"Data/Hora: {datetime.now()}")
    print()
    
    schema = get_schema_atual()
    
    # ============================================================================
    # 1. BUSCAR DADOS DO CONTRACT_SCENARIOS (DESTINO)
    # ============================================================================
    print("[1] Buscando dados em contract_scenarios (destino)...")
    conn_pg = DatabaseConnection.get_postgresql_destino_connection()
    cursor_pg = conn_pg.cursor()
    
    query_scenario = f"""
        SELECT 
            id,
            contract_id,
            frequency,
            hours,
            hour_value,
            start_date,
            end_date,
            legacy_id,
            created_at
        FROM {schema}.contract_scenarios
        WHERE id = %s
    """
    
    cursor_pg.execute(query_scenario, (scenario_id,))
    scenario_row = cursor_pg.fetchone()
    
    if not scenario_row:
        print(f"[ERRO] Scenario_id '{scenario_id}' nao encontrado em contract_scenarios!")
        cursor_pg.close()
        conn_pg.close()
        return
    
    scenario_data = {
        'id': scenario_row[0],
        'contract_id': scenario_row[1],
        'frequency': scenario_row[2],
        'hours': scenario_row[3],
        'hour_value': scenario_row[4],
        'start_date': scenario_row[5],
        'end_date': scenario_row[6],
        'legacy_id': scenario_row[7],
        'created_at': scenario_row[8]
    }
    
    print(f"[OK] Scenario encontrado:")
    print(f"  - Contract ID: {scenario_data['contract_id']}")
    print(f"  - Frequency: {scenario_data['frequency']}")
    print(f"  - Hours: {scenario_data['hours']}")
    print(f"  - Hour Value: {scenario_data['hour_value']}")
    print(f"  - Start Date: {scenario_data['start_date']}")
    print(f"  - End Date: {scenario_data['end_date']}")
    print()
    
    # Buscar IdOrcamento (legacy_id) do contract se não foi informado
    if id_orcamento is None:
        print("[1.1] Buscando IdOrcamento (legacy_id) do contract...")
        query_contract = f"""
            SELECT legacy_id
            FROM {schema}.contracts
            WHERE id = %s
        """
        cursor_pg.execute(query_contract, (scenario_data['contract_id'],))
        contract_row = cursor_pg.fetchone()
        if contract_row and contract_row[0] is not None:
            id_orcamento = contract_row[0]
            print(f"[OK] IdOrcamento encontrado: {id_orcamento}")
        else:
            print("[AVISO] Nao foi possivel encontrar IdOrcamento do contract. Continuando sem filtro...")
        print()
    
    # ============================================================================
    # 2. BUSCAR LEGACY_IDs (IdOrcamentoLoja) EM CONTRACT_SCENARIO_STORES
    # ============================================================================
    print("[2] Buscando legacy_id (IdOrcamentoLoja) em contract_scenario_stores...")
    
    query_stores = f"""
        SELECT 
            id,
            scenario_id,
            store_id,
            legacy_id,
            status,
            closed_at
        FROM {schema}.contract_scenario_stores
        WHERE scenario_id = %s
        ORDER BY legacy_id
    """
    
    cursor_pg.execute(query_stores, (scenario_id,))
    stores_rows = cursor_pg.fetchall()
    
    if not stores_rows:
        print(f"❌ ERRO: Nenhum registro encontrado em contract_scenario_stores para scenario_id '{scenario_id}'!")
        cursor_pg.close()
        conn_pg.close()
        return
    
    legacy_ids = [row[3] for row in stores_rows if row[3] is not None]
    
    print(f"[OK] Encontrados {len(stores_rows)} registros em contract_scenario_stores")
    print(f"[OK] Legacy IDs (IdOrcamentoLoja): {legacy_ids}")
    print()
    
    cursor_pg.close()
    conn_pg.close()
    
    # ============================================================================
    # 3. BUSCAR DADOS NA ORIGEM (ViewOrcamentosLojas)
    # ============================================================================
    print("[3] Buscando dados na origem (ViewOrcamentosLojas)...")
    
    conn_sql = DatabaseConnection.get_sql_server_prd_connection()
    cursor_sql = conn_sql.cursor()
    
    # Construir query com filtros
    where_clauses = []
    params = []
    
    if legacy_ids:
        placeholders = ','.join(['?' for _ in legacy_ids])
        where_clauses.append(f"v.IdOrcamentoLoja IN ({placeholders})")
        params.extend(legacy_ids)
    
    if id_orcamento:
        where_clauses.append("v.IdOrcamento IN (?)")
        params.append(id_orcamento)
    
    # Aplicar filtros de data se necessário (mesmos filtros da migração)
    # where_clauses.append("(CONVERT(DATE, v.DataAvisoPrevio) >= ? OR v.DataAvisoPrevio IS NULL)")
    # params.append('2026-01-01')
    # where_clauses.append("CONVERT(DATE, v.DataInicioOperacao) <= ?")
    # params.append('2026-01-31')
    
    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    query_origem = f"""
        SELECT 
            v.IdOrcamentoLoja,
            v.IdOrcamento,
            v.IdEstabelecimento,
            v.Frequencia,
            v.Horas,
            v.ValorHora,
            v.DataInicioOperacao,
            v.DataAvisoPrevio,
            v.NomeTarefa,
            v.NomeCliente,
            v.StatusPedido,
            v.IdTarefa,
            v.IdCliente
        FROM ViewOrcamentosLojas v
        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
        WHERE {where_clause}
        ORDER BY v.IdOrcamentoLoja
    """
    
    cursor_sql.execute(query_origem, params)
    origem_rows = cursor_sql.fetchall()
    
    if not origem_rows:
        print(f"[ERRO] Nenhum registro encontrado na origem para os IdOrcamentoLoja: {legacy_ids}")
        cursor_sql.close()
        conn_sql.close()
        return
    
    print(f"[OK] Encontrados {len(origem_rows)} registros na origem")
    
    # Debug: verificar estrutura dos dados retornados
    if origem_rows:
        print(f"[DEBUG] Primeira linha tem {len(origem_rows[0])} colunas")
        print(f"[DEBUG] Primeira linha: {origem_rows[0]}")
    print()
    
    # ============================================================================
    # 4. COMPARAR DADOS E IDENTIFICAR DISCREPÂNCIAS
    # ============================================================================
    print("[4] Comparando dados e identificando discrepâncias...")
    print()
    
    # Converter para lista de listas (pandas pode ter problemas com tuplas do pyodbc)
    origem_data = [list(row) for row in origem_rows]
    
    # Criar DataFrame para facilitar análise
    # Verificar se os dados têm o formato esperado
    if origem_data and len(origem_data[0]) == 13:
        origem_df = pd.DataFrame(origem_data, columns=[
            'IdOrcamentoLoja', 'IdOrcamento', 'IdEstabelecimento', 'Frequencia', 'Horas',
            'ValorHora', 'DataInicioOperacao', 'DataAvisoPrevio', 'NomeTarefa', 'NomeCliente',
            'StatusPedido', 'IdTarefa', 'IdCliente'
        ])
    else:
        print(f"[ERRO] Formato de dados inesperado. Esperado 13 colunas, recebido: {len(origem_data[0]) if origem_data else 0}")
        print(f"[DEBUG] Dados recebidos: {origem_data[:3] if len(origem_data) >= 3 else origem_data}")
        cursor_sql.close()
        conn_sql.close()
        return
    
    # Normalizar valores para comparação (mesma lógica do código de migração)
    origem_df['Frequencia_norm'] = origem_df['Frequencia'].astype(str).fillna('')
    origem_df['Horas_norm'] = origem_df['Horas'].astype(str).fillna('')
    origem_df['ValorHora_norm'] = pd.to_numeric(origem_df['ValorHora'], errors='coerce').fillna(0.0).astype(float).astype(str)
    origem_df['DataInicioOperacao_norm'] = pd.to_datetime(origem_df['DataInicioOperacao'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('')
    origem_df['DataAvisoPrevio_norm'] = pd.to_datetime(origem_df['DataAvisoPrevio'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('')
    
    # Valores do scenario migrado (normalizados)
    scenario_freq = str(scenario_data['frequency']) if scenario_data['frequency'] is not None else ''
    scenario_hours = str(scenario_data['hours']) if scenario_data['hours'] is not None else ''
    scenario_hour_value = str(float(scenario_data['hour_value'])) if scenario_data['hour_value'] is not None else '0.0'
    scenario_start_date = scenario_data['start_date'].strftime('%Y-%m-%d') if scenario_data['start_date'] else ''
    scenario_end_date = scenario_data['end_date'].strftime('%Y-%m-%d') if scenario_data['end_date'] else ''
    
    # Verificar discrepâncias
    discrepancies = []
    
    for idx, row in origem_df.iterrows():
        disc = {
            'IdOrcamentoLoja': row['IdOrcamentoLoja'],
            'IdOrcamento': row['IdOrcamento'],
            'IdEstabelecimento': row['IdEstabelecimento'],
            'discrepancies': []
        }
        
        # Comparar Frequencia
        if row['Frequencia_norm'] != scenario_freq:
            disc['discrepancies'].append({
                'campo': 'Frequencia',
                'origem': row['Frequencia'],
                'destino': scenario_data['frequency'],
                'origem_norm': row['Frequencia_norm'],
                'destino_norm': scenario_freq
            })
        
        # Comparar Horas
        if row['Horas_norm'] != scenario_hours:
            disc['discrepancies'].append({
                'campo': 'Horas',
                'origem': row['Horas'],
                'destino': scenario_data['hours'],
                'origem_norm': row['Horas_norm'],
                'destino_norm': scenario_hours
            })
        
        # Comparar ValorHora
        if row['ValorHora_norm'] != scenario_hour_value:
            disc['discrepancies'].append({
                'campo': 'ValorHora',
                'origem': row['ValorHora'],
                'destino': scenario_data['hour_value'],
                'origem_norm': row['ValorHora_norm'],
                'destino_norm': scenario_hour_value
            })
        
        # Comparar DataInicioOperacao
        if row['DataInicioOperacao_norm'] != scenario_start_date:
            disc['discrepancies'].append({
                'campo': 'DataInicioOperacao',
                'origem': row['DataInicioOperacao'],
                'destino': scenario_data['start_date'],
                'origem_norm': row['DataInicioOperacao_norm'],
                'destino_norm': scenario_start_date
            })
        
        # Comparar DataAvisoPrevio
        if row['DataAvisoPrevio_norm'] != scenario_end_date:
            disc['discrepancies'].append({
                'campo': 'DataAvisoPrevio',
                'origem': row['DataAvisoPrevio'],
                'destino': scenario_data['end_date'],
                'origem_norm': row['DataAvisoPrevio_norm'],
                'destino_norm': scenario_end_date
            })
        
        if disc['discrepancies']:
            discrepancies.append(disc)
    
    # ============================================================================
    # 5. GERAR RELATÓRIO
    # ============================================================================
    print("="*100)
    print("RELATÓRIO DE ANÁLISE")
    print("="*100)
    print()
    
    print(f"SCENARIO_ID: {scenario_id}")
    print(f"CONTRACT_ID: {scenario_data['contract_id']}")
    print()
    
    print("VALORES NO DESTINO (contract_scenarios):")
    print(f"  - Frequency: {scenario_data['frequency']}")
    print(f"  - Hours: {scenario_data['hours']}")
    print(f"  - Hour Value: {scenario_data['hour_value']}")
    print(f"  - Start Date: {scenario_data['start_date']}")
    print(f"  - End Date: {scenario_data['end_date']}")
    print()
    
    print(f"TOTAL DE REGISTROS NA ORIGEM: {len(origem_df)}")
    print(f"TOTAL DE REGISTROS COM DISCREPÂNCIAS: {len(discrepancies)}")
    print()
    
    if discrepancies:
        print("[AVISO] DISCREPANCIAS ENCONTRADAS:")
        print("-"*100)
        
        for disc in discrepancies:
            print(f"\nIdOrcamentoLoja: {disc['IdOrcamentoLoja']} | IdOrcamento: {disc['IdOrcamento']} | IdEstabelecimento: {disc['IdEstabelecimento']}")
            for d in disc['discrepancies']:
                print(f"  [ERRO] {d['campo']}:")
                print(f"     Origem: {d['origem']} (normalizado: '{d['origem_norm']}')")
                print(f"     Destino: {d['destino']} (normalizado: '{d['destino_norm']}')")
    else:
        print("[OK] NENHUMA DISCREPANCIA ENCONTRADA! Todos os valores estao consistentes.")
    
    print()
    print("="*100)
    print("DADOS COMPLETOS DA ORIGEM:")
    print("="*100)
    print(origem_df[['IdOrcamentoLoja', 'IdOrcamento', 'IdEstabelecimento', 'Frequencia', 'Horas', 
                     'ValorHora', 'DataInicioOperacao', 'DataAvisoPrevio']].to_string(index=False))
    print()
    
    cursor_sql.close()
    conn_sql.close()
    
    # Salvar relatório em arquivo
    report_file = f"scenario_analysis_{scenario_id[:8]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    report_path = os.path.join(os.path.dirname(__file__), report_file)
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("="*100 + "\n")
        f.write(f"RELATÓRIO DE ANÁLISE - SCENARIO_ID: {scenario_id}\n")
        f.write("="*100 + "\n")
        f.write(f"Data/Hora: {datetime.now()}\n\n")
        f.write(f"SCENARIO_ID: {scenario_id}\n")
        f.write(f"CONTRACT_ID: {scenario_data['contract_id']}\n\n")
        f.write("VALORES NO DESTINO (contract_scenarios):\n")
        f.write(f"  - Frequency: {scenario_data['frequency']}\n")
        f.write(f"  - Hours: {scenario_data['hours']}\n")
        f.write(f"  - Hour Value: {scenario_data['hour_value']}\n")
        f.write(f"  - Start Date: {scenario_data['start_date']}\n")
        f.write(f"  - End Date: {scenario_data['end_date']}\n\n")
        f.write(f"TOTAL DE REGISTROS NA ORIGEM: {len(origem_df)}\n")
        f.write(f"TOTAL DE REGISTROS COM DISCREPÂNCIAS: {len(discrepancies)}\n\n")
        
        if discrepancies:
            f.write("DISCREPÂNCIAS ENCONTRADAS:\n")
            f.write("-"*100 + "\n")
            for disc in discrepancies:
                f.write(f"\nIdOrcamentoLoja: {disc['IdOrcamentoLoja']} | IdOrcamento: {disc['IdOrcamento']} | IdEstabelecimento: {disc['IdEstabelecimento']}\n")
                for d in disc['discrepancies']:
                    f.write(f"  [ERRO] {d['campo']}:\n")
                    f.write(f"     Origem: {d['origem']} (normalizado: '{d['origem_norm']}')\n")
                    f.write(f"     Destino: {d['destino']} (normalizado: '{d['destino_norm']}')\n")
        else:
            f.write("[OK] NENHUMA DISCREPANCIA ENCONTRADA!\n\n")
        
        f.write("\n" + "="*100 + "\n")
        f.write("DADOS COMPLETOS DA ORIGEM:\n")
        f.write("="*100 + "\n")
        f.write(origem_df[['IdOrcamentoLoja', 'IdOrcamento', 'IdEstabelecimento', 'Frequencia', 'Horas', 
                           'ValorHora', 'DataInicioOperacao', 'DataAvisoPrevio']].to_string(index=False))
    
    print(f"[OK] Relatorio salvo em: {report_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python analyze_scenario_discrepancies.py <scenario_id> [id_orcamento]")
        print("\nExemplo:")
        print("  python analyze_scenario_discrepancies.py 042b9d01-e069-4eb0-8440-dc92ea3ba08a 6053")
        sys.exit(1)
    
    scenario_id = sys.argv[1]
    id_orcamento = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    try:
        analyze_scenario(scenario_id, id_orcamento)
    except Exception as e:
        print(f"[ERRO] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
