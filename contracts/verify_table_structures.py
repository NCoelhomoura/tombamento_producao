"""
Script para verificar a estrutura das tabelas nos ambientes
e comparar com o contract_dictionary.txt
"""

import sys
import os

# Adicionar diretório raiz ao path
root_path = os.path.join(os.path.dirname(__file__), '..')
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from utils.database_connection import DatabaseConnection

def get_view_columns_sql_server(view_name='ViewOrcamentosLojas'):
    """Obtém as colunas da view no SQL Server"""
    print(f"\n{'='*80}")
    print(f"VERIFICANDO: {view_name} no SQL Server PRD")
    print(f"{'='*80}")
    
    conn = None
    try:
        conn = DatabaseConnection.get_sql_server_prd_connection()
        cursor = conn.cursor()
        
        # Query para obter colunas da view
        query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE,
            COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{view_name}'
        ORDER BY ORDINAL_POSITION
        """
        
        cursor.execute(query)
        columns = cursor.fetchall()
        
        print(f"\nColunas encontradas: {len(columns)}\n")
        print(f"{'Nome':<40} {'Tipo':<20} {'Tamanho':<10} {'Nullable':<10}")
        print("-" * 80)
        
        relevant_columns = []
        for col in columns:
            col_name, data_type, max_length, is_nullable, default = col
            max_len_str = str(max_length) if max_length else '-'
            print(f"{col_name:<40} {data_type:<20} {max_len_str:<10} {is_nullable:<10}")
            
            # Filtrar colunas relevantes para contract_scenarios
            if col_name in ['IdOrcamento', 'Frequencia', 'Horas', 'ValorHora', 
                           'DataInicioOperacao', 'DataAvisoPrevio', 'IdCliente',
                           'IdOrcamentoLoja', 'IdEstabelecimento', 'IdTarefa', 
                           'NomeTarefa', 'StatusPedido', 'Ativo', 
                           'DataInclusaoOrcamentoLojas', 'DataAlteracaoOrcamentoLojas',
                           'DataExclusao']:
                relevant_columns.append({
                    'name': col_name,
                    'type': data_type,
                    'max_length': max_length,
                    'nullable': is_nullable
                })
        
        cursor.close()
        return relevant_columns
        
    except Exception as e:
        print(f"ERRO ao consultar SQL Server: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_table_columns_postgresql(schema, table_name):
    """Obtém as colunas da tabela no PostgreSQL"""
    print(f"\n{'='*80}")
    print(f"VERIFICANDO: {schema}.{table_name} no PostgreSQL PRD")
    print(f"{'='*80}")
    
    conn = None
    try:
        conn = DatabaseConnection.get_postgresql_prd_connection()
        cursor = conn.cursor()
        
        # Query para obter colunas da tabela
        query = f"""
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        
        cursor.execute(query)
        columns = cursor.fetchall()
        
        print(f"\nColunas encontradas: {len(columns)}\n")
        print(f"{'Nome':<40} {'Tipo':<30} {'Tamanho':<10} {'Nullable':<10}")
        print("-" * 90)
        
        table_columns = []
        for col in columns:
            col_name, data_type, max_length, is_nullable, default = col
            max_len_str = str(max_length) if max_length else '-'
            print(f"{col_name:<40} {data_type:<30} {max_len_str:<10} {is_nullable:<10}")
            table_columns.append({
                'name': col_name,
                'type': data_type,
                'max_length': max_length,
                'nullable': is_nullable == 'YES'
            })
        
        cursor.close()
        return table_columns
        
    except Exception as e:
        print(f"ERRO ao consultar PostgreSQL: {e}")
        return []
    finally:
        if conn:
            conn.close()

def compare_structures():
    """Compara as estruturas encontradas com o dictionary"""
    print("\n" + "="*80)
    print("VERIFICAÇÃO DE ESTRUTURAS DE TABELAS")
    print("="*80)
    
    # 1. Verificar ViewOrcamentosLojas
    view_columns = get_view_columns_sql_server('ViewOrcamentosLojas')
    
    # 2. Verificar contract_scenarios
    contract_scenarios_columns = get_table_columns_postgresql('commercial', 'contract_scenarios')
    
    # 3. Verificar contract_scenario_stores
    contract_scenario_stores_columns = get_table_columns_postgresql('commercial', 'contract_scenario_stores')
    
    # 4. Comparações
    print("\n" + "="*80)
    print("COMPARAÇÕES E ANÁLISES")
    print("="*80)
    
    # Verificar se store_id existe em contract_scenarios
    print("\n[1] Verificando se 'store_id' existe em contract_scenarios:")
    has_store_id = any(col['name'] == 'store_id' for col in contract_scenarios_columns)
    if has_store_id:
        print("   [ATENCAO] 'store_id' AINDA EXISTE na tabela contract_scenarios!")
        print("   O dictionary indica que foi removida, mas a tabela ainda possui.")
    else:
        print("   [OK] 'store_id' NAO existe em contract_scenarios (conforme esperado)")
    
    # Verificar campos esperados em contract_scenarios
    print("\n[2] Campos esperados em contract_scenarios:")
    expected_fields = ['id', 'contract_id', 'frequency', 'hours', 'hour_value', 
                      'start_date', 'status', 'created_at', 'updated_at', 
                      'promoter_task_id', 'legacy_id']
    optional_fields = ['deleted_at', 'end_date']
    
    for field in expected_fields:
        exists = any(col['name'] == field for col in contract_scenarios_columns)
        status = "[OK]" if exists else "[FALTANDO]"
        print(f"   {status} {field}")
    
    for field in optional_fields:
        exists = any(col['name'] == field for col in contract_scenarios_columns)
        status = "[OK]" if exists else "  (opcional)"
        print(f"   {status} {field}")
    
    # Verificar campos em contract_scenario_stores
    print("\n[3] Campos esperados em contract_scenario_stores:")
    expected_fields_stores = ['id', 'store_id', 'scenario_id', 'start_date', 
                             'status', 'created_at', 'updated_at']
    optional_fields_stores = ['closed_at', 'legacy_id']
    
    for field in expected_fields_stores:
        exists = any(col['name'] == field for col in contract_scenario_stores_columns)
        if exists:
            col_info = next((c for c in contract_scenario_stores_columns if c['name'] == field), None)
            col_type = col_info['type'] if col_info else 'N/A'
            status = f"[OK] {field} (tipo: {col_type})"
        else:
            status = f"[FALTANDO] {field}"
        print(f"   {status}")
    
    for field in optional_fields_stores:
        exists = any(col['name'] == field for col in contract_scenario_stores_columns)
        status = "[OK]" if exists else "  (opcional)"
        print(f"   {status} {field}")
    
    # Verificar campos relevantes na ViewOrcamentosLojas
    print("\n[4] Campos relevantes na ViewOrcamentosLojas para contract_scenarios:")
    required_view_fields = ['IdOrcamento', 'Frequencia', 'Horas', 'ValorHora', 
                           'DataInicioOperacao', 'DataAvisoPrevio']
    
    for field in required_view_fields:
        exists = any(col['name'] == field for col in view_columns)
        if exists:
            col_info = next((c for c in view_columns if c['name'] == field), None)
            col_type = col_info['type'] if col_info else 'N/A'
            status = f"[OK] {field} (tipo: {col_type})"
        else:
            status = f"[FALTANDO] {field}"
        print(f"   {status}")
    
    # Verificar diferença no tipo de status em contract_scenario_stores
    print("\n[5] Verificando tipo de dados de 'status' em contract_scenario_stores:")
    status_col = next((c for c in contract_scenario_stores_columns if c['name'] == 'status'), None)
    if status_col:
        print(f"   Tipo encontrado no banco: {status_col['type']}")
        print(f"   Tipo esperado no dictionary: INTEGER")
        if status_col['type'] != 'integer':
            print(f"   [ATENCAO] Tipo diferente! Banco tem '{status_col['type']}' mas dictionary espera 'INTEGER'")
    else:
        print("   [ERRO] Campo 'status' nao encontrado!")
    
    print("\n" + "="*80)
    print("VERIFICAÇÃO CONCLUÍDA")
    print("="*80)

if __name__ == "__main__":
    compare_structures()
