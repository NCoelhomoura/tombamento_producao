"""
Script temporário para analisar a estrutura das tabelas do schema commercial em PRD
e comparar com o dicionário.
"""

import sys
import os

# Adicionar diretório raiz ao path
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from utils.database_connection import DatabaseConnection

# Tabelas do schema commercial que devem existir
TABLES_TO_CHECK = [
    'contracts',
    'contract_scenarios',
    'contract_scenario_stores',
    'contract_sellers',
    'contract_team_members',
    'contract_contacts',
    'contract_partners',
    'contract_additional_charges'
]

def get_table_structure(schema, table_name):
    """Obtém a estrutura de uma tabela"""
    conn = None
    try:
        conn = DatabaseConnection.get_postgresql_destino_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = %s
        AND table_name = %s
        ORDER BY ordinal_position
        """
        
        cursor.execute(query, (schema, table_name))
        columns = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return columns
    except Exception as e:
        print(f"ERRO ao consultar {schema}.{table_name}: {e}")
        if conn:
            conn.close()
        return None

def format_column_info(column):
    """Formata informação da coluna"""
    col_name, data_type, max_length, is_nullable, default = column
    
    # Ajustar tipo de dados para formato legível
    if max_length:
        type_str = f"{data_type}({max_length})"
    else:
        type_str = data_type
    
    nullable = "SIM" if is_nullable == "YES" else "NAO"
    
    return {
        'name': col_name,
        'type': type_str,
        'nullable': nullable,
        'default': default
    }

def main():
    print("="*80)
    print("ANÁLISE DA ESTRUTURA DAS TABELAS DO SCHEMA COMMERCIAL EM PRD")
    print("="*80)
    print()
    
    # Configurar destino como PRD
    DatabaseConnection.set_destino('PRD')
    schema = 'commercial'
    
    print(f"Schema: {schema}")
    print(f"Ambiente: PRD")
    print()
    
    results = {}
    
    for table_name in TABLES_TO_CHECK:
        print(f"\n{'='*80}")
        print(f"TABELA: {table_name}")
        print(f"{'='*80}")
        
        columns = get_table_structure(schema, table_name)
        
        if columns is None:
            print(f"❌ Tabela {schema}.{table_name} não encontrada ou erro ao consultar")
            results[table_name] = None
            continue
        
        if len(columns) == 0:
            print(f"⚠️  Tabela {schema}.{table_name} não possui colunas ou não existe")
            results[table_name] = []
            continue
        
        print(f"\nColunas encontradas ({len(columns)}):")
        print("-"*80)
        print(f"{'Nome':<30} {'Tipo':<30} {'Nullable':<10} {'Default'}")
        print("-"*80)
        
        table_columns = []
        for col in columns:
            col_info = format_column_info(col)
            table_columns.append(col_info)
            default_str = str(col_info['default'])[:30] if col_info['default'] else ''
            print(f"{col_info['name']:<30} {col_info['type']:<30} {col_info['nullable']:<10} {default_str}")
        
        results[table_name] = table_columns
    
    # Resumo
    print("\n" + "="*80)
    print("RESUMO")
    print("="*80)
    print(f"\nTotal de tabelas analisadas: {len(TABLES_TO_CHECK)}")
    
    found_tables = [t for t, cols in results.items() if cols is not None and len(cols) > 0]
    not_found_tables = [t for t, cols in results.items() if cols is None or len(cols) == 0]
    
    print(f"Tabelas encontradas: {len(found_tables)}")
    for t in found_tables:
        print(f"  [OK] {t} ({len(results[t])} colunas)")
    
    if not_found_tables:
        print(f"\nTabelas nao encontradas ou sem colunas: {len(not_found_tables)}")
        for t in not_found_tables:
            print(f"  [ERRO] {t}")
    
    print("\n" + "="*80)
    print("ANÁLISE CONCLUÍDA")
    print("="*80)

if __name__ == "__main__":
    main()
