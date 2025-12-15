"""
Script para investigar a estrutura das tabelas e views relacionadas a contratos
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

def get_view_structure(view_name):
    """Obtém a estrutura de uma view no SQL Server"""
    print(f"\n{'='*80}")
    print(f"INVESTIGANDO VIEW: {view_name}")
    print(f"{'='*80}")
    
    conn = None
    try:
        conn = DatabaseConnection.get_sql_server_prd_connection()
        cursor = conn.cursor()
        
        # Obter definição da view
        cursor.execute(f"""
            SELECT definition
            FROM sys.sql_modules
            WHERE object_id = OBJECT_ID('dbo.{view_name}')
        """)
        
        definition = cursor.fetchone()
        if definition:
            print(f"\nDEFINIÇÃO DA VIEW:")
            print(f"{'-'*80}")
            print(definition[0][:2000])  # Primeiros 2000 caracteres
            if len(definition[0]) > 2000:
                print(f"\n... (truncado, total: {len(definition[0])} caracteres)")
        
        # Obter colunas da view
        cursor.execute(f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{view_name}'
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        print(f"\n\nCOLUNAS DA VIEW ({len(columns)}):")
        print(f"{'-'*80}")
        print(f"{'Nome':<40} {'Tipo':<20} {'Tamanho':<10} {'Nullable':<10}")
        print(f"{'-'*80}")
        
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
            
            print(f"{col_name:<40} {type_str:<20} {str(max_length):<10} {nullable:<10}")
        
        cursor.close()
        conn.close()
        
        return columns
        
    except Exception as e:
        print(f"ERRO ao investigar view {view_name}: {e}")
        if conn:
            conn.close()
        return None


def get_table_structure(table_name):
    """Obtém a estrutura de uma tabela no SQL Server"""
    print(f"\n{'='*80}")
    print(f"INVESTIGANDO TABELA: {table_name}")
    print(f"{'='*80}")
    
    conn = None
    try:
        conn = DatabaseConnection.get_sql_server_prd_connection()
        cursor = conn.cursor()
        
        # Obter colunas da tabela
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
            WHERE TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        
        # Obter chaves primárias
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = '{table_name}'
            AND CONSTRAINT_NAME LIKE 'PK_%'
            ORDER BY ORDINAL_POSITION
        """)
        
        pk_columns = [row[0] for row in cursor.fetchall()]
        
        print(f"\nCOLUNAS DA TABELA ({len(columns)}):")
        print(f"{'-'*80}")
        print(f"{'Nome':<40} {'Tipo':<20} {'Tamanho':<10} {'Nullable':<10} {'PK':<5}")
        print(f"{'-'*80}")
        
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
            
            print(f"{col_name:<40} {type_str:<20} {str(max_length):<10} {nullable:<10} {is_pk:<5}")
        
        cursor.close()
        conn.close()
        
        return columns, pk_columns
        
    except Exception as e:
        print(f"ERRO ao investigar tabela {table_name}: {e}")
        if conn:
            conn.close()
        return None, None


def get_postgresql_tables_structure():
    """Obtém a estrutura das tabelas de contratos no PostgreSQL"""
    print(f"\n{'='*80}")
    print(f"INVESTIGANDO TABELAS DE DESTINO: gmcommercial")
    print(f"{'='*80}")
    
    conn = None
    try:
        conn = DatabaseConnection.get_postgresql_hml_connection()
        cursor = conn.cursor()
        
        # Listar tabelas do schema gmcommercial
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'gmcommercial'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        
        print(f"\nTabelas encontradas: {len(tables)}")
        for table in tables:
            print(f"  - {table}")
        
        # Obter estrutura de cada tabela
        tables_structure = {}
        
        for table_name in tables:
            cursor.execute(f"""
                SELECT 
                    column_name,
                    data_type,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = 'gmcommercial'
                AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """)
            
            columns = cursor.fetchall()
            
            # Obter chaves primárias
            cursor.execute(f"""
                SELECT column_name
                FROM information_schema.key_column_usage
                WHERE table_schema = 'gmcommercial'
                AND table_name = '{table_name}'
                AND constraint_name IN (
                    SELECT constraint_name
                    FROM information_schema.table_constraints
                    WHERE table_schema = 'gmcommercial'
                    AND table_name = '{table_name}'
                    AND constraint_type = 'PRIMARY KEY'
                )
                ORDER BY ordinal_position
            """)
            
            pk_columns = [row[0] for row in cursor.fetchall()]
            
            tables_structure[table_name] = {
                'columns': columns,
                'pk': pk_columns
            }
            
            print(f"\n\nTABELA: {table_name}")
            print(f"{'-'*80}")
            print(f"{'Nome':<40} {'Tipo':<25} {'Tamanho':<10} {'Nullable':<10} {'PK':<5}")
            print(f"{'-'*80}")
            
            for col in columns:
                col_name = col[0]
                data_type = col[1]
                max_length = col[2] if col[2] else ''
                precision = col[3] if col[3] else ''
                scale = col[4] if col[4] else ''
                nullable = col[5]
                is_pk = 'SIM' if col_name in pk_columns else 'NAO'
                
                if data_type in ['character varying', 'varchar']:
                    type_str = f"{data_type}({max_length})" if max_length else data_type
                elif data_type in ['numeric', 'decimal']:
                    type_str = f"{data_type}({precision},{scale})" if precision and scale else data_type
                else:
                    type_str = data_type
                
                print(f"{col_name:<40} {type_str:<25} {str(max_length):<10} {nullable:<10} {is_pk:<5}")
        
        cursor.close()
        conn.close()
        
        return tables_structure
        
    except Exception as e:
        print(f"ERRO ao investigar tabelas PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.close()
        return None


def main():
    """Executa a investigação completa"""
    print("\n" + "="*80)
    print("INVESTIGAÇÃO DE ESTRUTURA - CONTRATOS")
    print("="*80)
    
    # 1. Investigar ViewOrcamentoLojas
    view_columns = get_view_structure('ViewOrcamentoLojas')
    
    # 2. Investigar tabela Orcamentos
    table_columns, pk_columns = get_table_structure('Orcamentos')
    
    # 3. Investigar tabelas de destino
    dest_tables = get_postgresql_tables_structure()
    
    print("\n" + "="*80)
    print("INVESTIGAÇÃO CONCLUÍDA")
    print("="*80)


if __name__ == "__main__":
    main()


