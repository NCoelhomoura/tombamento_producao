"""
Script para debugar por que o registro não foi associado ao scenario.
"""

import sys
import os
import pandas as pd

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

def debug_scenario_key():
    """
    Debugar por que o registro não foi associado ao scenario.
    """
    print("="*100)
    print(f"DEBUG: Por que o registro nao foi associado ao scenario?")
    print("="*100)
    
    # Dados do registro na origem
    id_orcamento = 6053
    id_orcamento_loja = 858734
    frequencia = 1
    horas = 1
    valor_hora = 38.18
    data_inicio = '2025-02-01'
    data_aviso_previo = None
    
    print(f"\n[DADOS DO REGISTRO]")
    print(f"IdOrcamento: {id_orcamento}")
    print(f"IdOrcamentoLoja: {id_orcamento_loja}")
    print(f"Frequencia: {frequencia}")
    print(f"Horas: {horas}")
    print(f"ValorHora: {valor_hora}")
    print(f"DataInicioOperacao: {data_inicio}")
    print(f"DataAvisoPrevio: {data_aviso_previo}")
    
    # Criar scenario_key usando a mesma lógica do step3
    def create_scenario_key(row):
        """Cria chave única baseada na combinação de campos (mesma normalização do step2)"""
        freq_str = str(row['Frequencia']) if pd.notna(row['Frequencia']) else ''
        horas_str = str(row['Horas']) if pd.notna(row['Horas']) else ''
        try:
            valor_hora_float = float(row['ValorHora']) if pd.notna(row['ValorHora']) else 0.0
            valor_hora_str = str(valor_hora_float)
        except (ValueError, TypeError):
            valor_hora_str = '0.0'
        start_date_str = row['DataInicioOperacao'].strftime('%Y-%m-%d') if pd.notna(row['DataInicioOperacao']) else ''
        end_date_str = row['DataAvisoPrevio'].strftime('%Y-%m-%d') if pd.notna(row['DataAvisoPrevio']) else None
        
        return (
            int(row['IdOrcamento']),
            freq_str,
            horas_str,
            valor_hora_str,
            start_date_str,
            end_date_str
        )
    
    # Criar DataFrame com o registro
    df = pd.DataFrame([{
        'IdOrcamento': id_orcamento,
        'Frequencia': frequencia,
        'Horas': horas,
        'ValorHora': valor_hora,
        'DataInicioOperacao': pd.to_datetime(data_inicio),
        'DataAvisoPrevio': None if data_aviso_previo is None else pd.to_datetime(data_aviso_previo)
    }])
    
    scenario_key = create_scenario_key(df.iloc[0])
    print(f"\n[SCENARIO_KEY CRIADO]")
    print(f"Scenario Key: {scenario_key}")
    print(f"  - IdOrcamento: {scenario_key[0]}")
    print(f"  - Frequencia: '{scenario_key[1]}'")
    print(f"  - Horas: '{scenario_key[2]}'")
    print(f"  - ValorHora: '{scenario_key[3]}'")
    print(f"  - DataInicioOperacao: '{scenario_key[4]}'")
    print(f"  - DataAvisoPrevio: {scenario_key[5]}")
    
    # Buscar scenario_id_map do banco
    schema = get_schema_atual()
    conn_pg = DatabaseConnection.get_postgresql_destino_connection()
    cursor_pg = conn_pg.cursor()
    
    # Buscar contract_id
    cursor_pg.execute(f"""
        SELECT id
        FROM {schema}.contracts
        WHERE legacy_id = %s
    """, (id_orcamento,))
    
    contract_row = cursor_pg.fetchone()
    if not contract_row:
        print(f"\n[ERRO] Contract nao encontrado!")
        cursor_pg.close()
        conn_pg.close()
        return
    
    contract_id = contract_row[0]
    print(f"\n[CONTRACT_ID] {contract_id}")
    
    # Buscar scenario do banco
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
            AND frequency = 'once_per_week'
            AND hours = 1.0
            AND hour_value = 38.18
            AND start_date::date = '2025-02-01'
            AND end_date IS NULL
    """, (contract_id,))
    
    scenario_row = cursor_pg.fetchone()
    if not scenario_row:
        print(f"\n[ERRO] Scenario nao encontrado!")
        cursor_pg.close()
        conn_pg.close()
        return
    
    scenario_id = scenario_row[0]
    scenario_frequency = scenario_row[1]
    scenario_hours = scenario_row[2]
    scenario_hour_value = scenario_row[3]
    scenario_start_date = scenario_row[4]
    scenario_end_date = scenario_row[5]
    
    print(f"\n[SCENARIO NO BANCO]")
    print(f"Scenario ID: {scenario_id}")
    print(f"Frequency: {scenario_frequency}")
    print(f"Hours: {scenario_hours}")
    print(f"Hour Value: {scenario_hour_value}")
    print(f"Start Date: {scenario_start_date}")
    print(f"End Date: {scenario_end_date}")
    
    # Criar scenario_key do banco (mesma lógica do step2)
    frequency_enum_to_int = {
        'once_per_week': '1',
        'twice_per_week': '2',
        'three_times_per_week': '3',
        'four_times_per_week': '4',
        'five_times_per_week': '5',
        'six_times_per_week': '6',
        'seven_times_per_week': '7',
        'every_15_days': '15',
        'once_per_month': '30'
    }
    
    freq_str_db = frequency_enum_to_int.get(str(scenario_frequency), str(scenario_frequency))
    horas_str_db = str(scenario_hours) if scenario_hours else ''
    try:
        valor_hora_float_db = float(scenario_hour_value) if scenario_hour_value is not None else 0.0
        valor_hora_str_db = str(valor_hora_float_db)
    except (ValueError, TypeError):
        valor_hora_str_db = '0.0'
    start_date_str_db = scenario_start_date.strftime('%Y-%m-%d') if scenario_start_date else ''
    end_date_str_db = scenario_end_date.strftime('%Y-%m-%d') if scenario_end_date else None
    
    scenario_key_db = (
        int(id_orcamento),
        freq_str_db,
        horas_str_db,
        valor_hora_str_db,
        start_date_str_db,
        end_date_str_db
    )
    
    print(f"\n[SCENARIO_KEY DO BANCO]")
    print(f"Scenario Key: {scenario_key_db}")
    print(f"  - IdOrcamento: {scenario_key_db[0]}")
    print(f"  - Frequencia: '{scenario_key_db[1]}'")
    print(f"  - Horas: '{scenario_key_db[2]}'")
    print(f"  - ValorHora: '{scenario_key_db[3]}'")
    print(f"  - DataInicioOperacao: '{scenario_key_db[4]}'")
    print(f"  - DataAvisoPrevio: {scenario_key_db[5]}")
    
    # Comparar
    print(f"\n[COMPARACAO]")
    print(f"Scenario Key do registro: {scenario_key}")
    print(f"Scenario Key do banco:     {scenario_key_db}")
    print(f"Sao iguais? {scenario_key == scenario_key_db}")
    
    if scenario_key != scenario_key_db:
        print(f"\n[DIFERENCAS]")
        for i, (val1, val2) in enumerate(zip(scenario_key, scenario_key_db)):
            if val1 != val2:
                print(f"  Posicao {i}: '{val1}' != '{val2}'")
    
    # Verificar se está no contract_scenario_stores
    cursor_pg.execute(f"""
        SELECT 
            css.id,
            css.scenario_id,
            css.legacy_id,
            css.store_id,
            css.status
        FROM {schema}.contract_scenario_stores css
        WHERE css.scenario_id = %s
            AND css.legacy_id = %s
    """, (scenario_id, id_orcamento_loja))
    
    store_row = cursor_pg.fetchone()
    if store_row:
        print(f"\n[ENCONTRADO] Registro encontrado em contract_scenario_stores!")
        print(f"  ID: {store_row[0]}")
        print(f"  Scenario ID: {store_row[1]}")
        print(f"  Legacy ID: {store_row[2]}")
        print(f"  Store ID: {store_row[3]}")
        print(f"  Status: {store_row[4]}")
    else:
        print(f"\n[NAO ENCONTRADO] Registro NAO encontrado em contract_scenario_stores!")
        print(f"  Isso significa que o registro nao foi migrado no step3.")
    
    cursor_pg.close()
    conn_pg.close()
    print("\n" + "="*100)

if __name__ == "__main__":
    try:
        debug_scenario_key()
    except Exception as e:
        print(f"[ERRO] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
