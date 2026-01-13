"""
Script para encontrar tabelas e views relacionadas a Orcamentos
"""

# Configurar encoding para evitar problemas no Windows
import sys
import io
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database_connection import DatabaseConnection

def find_tables_and_views():
    """Encontra todas as tabelas e views relacionadas a Orcamentos"""
    print("\n" + "="*80)
    print("BUSCANDO TABELAS E VIEWS RELACIONADAS A ORCAMENTOS")
    print("="*80)
    
    conn = None
    try:
        conn = DatabaseConnection.get_sql_server_prd_connection()
        cursor = conn.cursor()
        
        # Buscar tabelas
        cursor.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            AND (TABLE_NAME LIKE '%Orcamento%' OR TABLE_NAME LIKE '%orcamento%')
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        
        tables = cursor.fetchall()
        
        print(f"\nTABELAS ENCONTRADAS ({len(tables)}):")
        for schema, table in tables:
            print(f"  {schema}.{table}")
        
        # Buscar views
        cursor.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE (TABLE_NAME LIKE '%Orcamento%' OR TABLE_NAME LIKE '%orcamento%')
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        
        views = cursor.fetchall()
        
        print(f"\nVIEWS ENCONTRADAS ({len(views)}):")
        for schema, view in views:
            print(f"  {schema}.{view}")
        
        # Buscar também por "Contrato"
        cursor.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            AND (TABLE_NAME LIKE '%Contrato%' OR TABLE_NAME LIKE '%contrato%')
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        
        tables_contrato = cursor.fetchall()
        
        if tables_contrato:
            print(f"\nTABELAS COM 'CONTRATO' ENCONTRADAS ({len(tables_contrato)}):")
            for schema, table in tables_contrato:
                print(f"  {schema}.{table}")
        
        cursor.close()
        conn.close()
        
        return tables, views
        
    except Exception as e:
        print(f"ERRO: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.close()
        return None, None


def get_table_structure_full(schema, table_name):
    """Obtém a estrutura completa de uma tabela"""
    print(f"\n{'='*80}")
    print(f"ESTRUTURA: {schema}.{table_name}")
    print(f"{'='*80}")
    
    conn = None
    try:
        conn = DatabaseConnection.get_sql_server_prd_connection()
        cursor = conn.cursor()
        
        # Obter colunas
        cursor.execute(f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE,
                COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        
        # Obter PKs
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{table_name}'
            AND CONSTRAINT_NAME LIKE 'PK_%'
            ORDER BY ORDINAL_POSITION
        """)
        
        pk_columns = [row[0] for row in cursor.fetchall()]
        
        print(f"\n{'Nome':<40} {'Tipo':<25} {'Tamanho':<10} {'Nullable':<10} {'PK':<5}")
        print(f"{'-'*90}")
        
        for col in columns:
            col_name = col[0]
            data_type = col[1]
            max_length = col[2] if col[2] else ''
            precision = col[3] if col[3] else ''
            scale = col[4] if col[4] else ''
            nullable = col[5]
            is_pk = 'SIM' if col_name in pk_columns else 'NAO'
            
            if data_type in ['nvarchar', 'varchar', 'char', 'nchar']:
                type_str = f"{data_type}({max_length})" if max_length else data_type
            elif data_type in ['numeric', 'decimal']:
                type_str = f"{data_type}({precision},{scale})" if precision and scale else data_type
            else:
                type_str = data_type
            
            print(f"{col_name:<40} {type_str:<25} {str(max_length):<10} {nullable:<10} {is_pk:<5}")
        
        cursor.close()
        conn.close()
        
        return columns, pk_columns
        
    except Exception as e:
        print(f"ERRO: {e}")
        if conn:
            conn.close()
        return None, None


def get_view_structure_full(schema, view_name):
    """Obtém a estrutura completa de uma view"""
    print(f"\n{'='*80}")
    print(f"ESTRUTURA: {schema}.{view_name}")
    print(f"{'='*80}")
    
    conn = None
    try:
        conn = DatabaseConnection.get_sql_server_prd_connection()
        cursor = conn.cursor()
        
        # Obter colunas
        cursor.execute(f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{view_name}'
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        
        print(f"\n{'Nome':<40} {'Tipo':<25} {'Tamanho':<10} {'Nullable':<10}")
        print(f"{'-'*85}")
        
        for col in columns:
            col_name = col[0]
            data_type = col[1]
            max_length = col[2] if col[2] else ''
            precision = col[3] if col[3] else ''
            scale = col[4] if col[4] else ''
            nullable = col[5]
            
            if data_type in ['nvarchar', 'varchar', 'char', 'nchar']:
                type_str = f"{data_type}({max_length})" if max_length else data_type
            elif data_type in ['numeric', 'decimal']:
                type_str = f"{data_type}({precision},{scale})" if precision and scale else data_type
            else:
                type_str = data_type
            
            print(f"{col_name:<40} {type_str:<25} {str(max_length):<10} {nullable:<10}")
        
        cursor.close()
        conn.close()
        
        return columns
        
    except Exception as e:
        print(f"ERRO: {e}")
        if conn:
            conn.close()
        return None


def main():
    """Executa a busca"""
    tables, views = find_tables_and_views()
    
    # Se encontrou a view ViewOrcamentoLojas, investigar
    if views:
        for schema, view in views:
            if 'OrcamentoLojas' in view or 'orcamentolojas' in view.lower():
                print(f"\n\n{'='*80}")
                print(f"INVESTIGANDO VIEW PRINCIPAL: {schema}.{view}")
                print(f"{'='*80}")
                get_view_structure_full(schema, view)
    
    # Se encontrou a tabela Orcamentos, investigar
    if tables:
        for schema, table in tables:
            if table.lower() == 'orcamentos':
                print(f"\n\n{'='*80}")
                print(f"INVESTIGANDO TABELA PRINCIPAL: {schema}.{table}")
                print(f"{'='*80}")
                get_table_structure_full(schema, table)


if __name__ == "__main__":
    main()




