"""
Script para encontrar um scenario específico no destino e verificar quantas lojas estão associadas.
"""

import sys
import os

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

def find_scenario(id_orcamento, frequencia, horas, valor_hora, data_inicio, data_aviso_previo=None):
    """
    Busca um scenario específico no destino.
    
    Args:
        id_orcamento: IdOrcamento (int)
        frequencia: Frequencia (int) - será convertido para enum
        horas: Horas (float ou str)
        valor_hora: ValorHora (float ou str)
        data_inicio: DataInicioOperacao (string 'YYYY-MM-DD')
        data_aviso_previo: DataAvisoPrevio (string 'YYYY-MM-DD' ou None)
    """
    print("="*100)
    print(f"BUSCANDO SCENARIO NO DESTINO")
    print("="*100)
    print(f"IdOrcamento: {id_orcamento}")
    print(f"Frequencia: {frequencia}")
    print(f"Horas: {horas}")
    print(f"ValorHora: {valor_hora}")
    print(f"DataInicioOperacao: {data_inicio}")
    print(f"DataAvisoPrevio: {data_aviso_previo}")
    print()
    
    schema = get_schema_atual()
    
    # Mapa de frequencia: inteiro -> enum
    frequency_map = {
        1: 'once_per_week',
        2: 'twice_per_week',
        3: 'three_times_per_week',
        4: 'four_times_per_week',
        5: 'five_times_per_week',
        6: 'six_times_per_week',
        7: 'seven_times_per_week',
        15: 'every_15_days',
        30: 'once_per_month'
    }
    
    frequency_enum = frequency_map.get(int(frequencia), None)
    if not frequency_enum:
        print(f"[ERRO] Frequencia {frequencia} nao encontrada no mapa")
        return
    
    # Normalizar valor_hora (pode vir com vírgula)
    try:
        valor_hora_float = float(str(valor_hora).replace(',', '.'))
    except ValueError:
        print(f"[ERRO] Nao foi possivel converter ValorHora: {valor_hora}")
        return
    
    # Normalizar horas
    try:
        horas_float = float(str(horas).replace(',', '.'))
    except ValueError:
        print(f"[ERRO] Nao foi possivel converter Horas: {horas}")
        return
    
    # Normalizar data_inicio
    data_inicio_str = data_inicio.split()[0] if ' ' in str(data_inicio) else str(data_inicio)
    
    # Buscar contract_id primeiro
    conn_pg = DatabaseConnection.get_postgresql_destino_connection()
    cursor_pg = conn_pg.cursor()
    
    # Buscar contract_id pelo legacy_id (IdOrcamento)
    cursor_pg.execute(f"""
        SELECT id
        FROM {schema}.contracts
        WHERE legacy_id = %s
    """, (id_orcamento,))
    
    contract_row = cursor_pg.fetchone()
    if not contract_row:
        print(f"[ERRO] Contract nao encontrado para IdOrcamento={id_orcamento}")
        cursor_pg.close()
        conn_pg.close()
        return
    
    contract_id = contract_row[0]
    print(f"[OK] Contract encontrado: {contract_id}")
    
    # Buscar scenario
    query_scenario = f"""
        SELECT 
            id,
            contract_id,
            frequency,
            hours,
            hour_value,
            start_date,
            end_date,
            created_at
        FROM {schema}.contract_scenarios
        WHERE contract_id = %s
            AND frequency = %s
            AND hours = %s
            AND hour_value = %s
            AND start_date::date = %s
    """
    
    params = [contract_id, frequency_enum, horas_float, valor_hora_float, data_inicio_str]
    
    # Se data_aviso_previo for None, buscar apenas scenarios com end_date NULL
    if data_aviso_previo is None:
        query_scenario += " AND end_date IS NULL"
    else:
        data_aviso_str = data_aviso_previo.split()[0] if ' ' in str(data_aviso_previo) else str(data_aviso_previo)
        query_scenario += " AND end_date::date = %s"
        params.append(data_aviso_str)
    
    print(f"\n[QUERY] Executando query para buscar scenario...")
    print(f"Query: {query_scenario}")
    print(f"Params: {params}")
    
    cursor_pg.execute(query_scenario, params)
    scenario_rows = cursor_pg.fetchall()
    
    if not scenario_rows:
        print(f"\n[ERRO] Scenario nao encontrado com esses valores!")
        print(f"\nTentando buscar todos os scenarios deste contract para comparacao...")
        
        # Buscar todos os scenarios do contract para comparação
        cursor_pg.execute(f"""
            SELECT 
                id,
                frequency,
                hours,
                hour_value,
                start_date,
                end_date
            FROM {schema}.contract_scenarios
            WHERE contract_id = %s
            ORDER BY created_at DESC
        """, (contract_id,))
        
        all_scenarios = cursor_pg.fetchall()
        print(f"\n[INFO] Encontrados {len(all_scenarios)} scenarios para este contract:")
        for idx, sc in enumerate(all_scenarios, 1):
            print(f"  {idx}. Scenario ID: {sc[0]}")
            print(f"     Frequency: {sc[1]}")
            print(f"     Hours: {sc[2]}")
            print(f"     Hour Value: {sc[3]}")
            print(f"     Start Date: {sc[4]}")
            print(f"     End Date: {sc[5]}")
            print()
        
        cursor_pg.close()
        conn_pg.close()
        return
    
    print(f"\n[OK] Scenario encontrado!")
    print("-"*100)
    
    for scenario_row in scenario_rows:
        scenario_id = scenario_row[0]
        scenario_contract_id = scenario_row[1]
        scenario_frequency = scenario_row[2]
        scenario_hours = scenario_row[3]
        scenario_hour_value = scenario_row[4]
        scenario_start_date = scenario_row[5]
        scenario_end_date = scenario_row[6]
        scenario_created_at = scenario_row[7]
        
        print(f"Scenario ID: {scenario_id}")
        print(f"Contract ID: {scenario_contract_id}")
        print(f"Frequency: {scenario_frequency}")
        print(f"Hours: {scenario_hours}")
        print(f"Hour Value: {scenario_hour_value}")
        print(f"Start Date: {scenario_start_date}")
        print(f"End Date: {scenario_end_date}")
        print(f"Created At: {scenario_created_at}")
        print()
        
        # Buscar lojas associadas a este scenario
        cursor_pg.execute(f"""
            SELECT 
                css.id,
                css.scenario_id,
                css.store_id,
                css.legacy_id,
                css.status,
                css.closed_at,
                s.name as store_name
            FROM {schema}.contract_scenario_stores css
            LEFT JOIN core.stores s ON s.id = css.store_id
            WHERE css.scenario_id = %s
            ORDER BY css.legacy_id
        """, (scenario_id,))
        
        stores_rows = cursor_pg.fetchall()
        
        print(f"[LOJAS] Total de lojas associadas: {len(stores_rows)}")
        print("-"*100)
        
        if stores_rows:
            print("Lojas associadas:")
            for idx, store_row in enumerate(stores_rows, 1):
                store_id = store_row[0]
                store_scenario_id = store_row[1]
                store_store_id = store_row[2]
                store_legacy_id = store_row[3]
                store_status = store_row[4]
                store_closed_at = store_row[5]
                store_name = store_row[6]
                
                print(f"  {idx}. Legacy ID (IdOrcamentoLoja): {store_legacy_id}")
                print(f"     Store ID: {store_store_id}")
                print(f"     Store Name: {store_name}")
                print(f"     Status: {store_status}")
                print(f"     Closed At: {store_closed_at}")
                print()
        else:
            print("[AVISO] Nenhuma loja associada a este scenario!")
        
        print("="*100)
    
    cursor_pg.close()
    conn_pg.close()

if __name__ == "__main__":
    # Valores do scenario a buscar
    id_orcamento = 6053
    frequencia = 1
    horas = 1
    valor_hora = 38.18  # Normalizar vírgula se necessário
    data_inicio = '2025-02-01'
    data_aviso_previo = None
    
    try:
        find_scenario(id_orcamento, frequencia, horas, valor_hora, data_inicio, data_aviso_previo)
    except Exception as e:
        print(f"[ERRO] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
