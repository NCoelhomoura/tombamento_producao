"""
Script para investigar a estrutura das tabelas usuarios (SQL Server) e users (PostgreSQL)
"""
import sys
import os

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.database_connection import DatabaseConnection
import pyodbc

def get_sql_server_table_structure(table_name):
    """Obtém a estrutura de uma tabela do SQL Server PRD"""
    conn = None
    try:
        conn = DatabaseConnection.get_sql_server_prd_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT 
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
        """
        
        cursor.execute(query, (table_name,))
        columns = cursor.fetchall()
        
        structure = []
        for col in columns:
            col_name = col[0]
            data_type = col[1]
            max_length = col[2]
            precision = col[3]
            scale = col[4]
            is_nullable = col[5]
            default = col[6]
            
            # Formatar tipo de dados
            if data_type in ['varchar', 'nvarchar', 'char', 'nchar']:
                if max_length == -1:
                    type_str = f"{data_type}(MAX)"
                else:
                    type_str = f"{data_type}({max_length})"
            elif data_type in ['decimal', 'numeric']:
                type_str = f"{data_type}({precision},{scale})"
            else:
                type_str = data_type
            
            structure.append({
                'name': col_name,
                'type': type_str,
                'nullable': is_nullable,
                'default': default
            })
        
        cursor.close()
        return structure
        
    except Exception as e:
        print(f"Erro ao obter estrutura da tabela {table_name}: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        if conn:
            conn.close()

def get_postgresql_table_structure(table_name, schema='gmcore'):
    """Obtém a estrutura de uma tabela do PostgreSQL HML"""
    conn = None
    try:
        conn = DatabaseConnection.get_postgresql_destino_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT 
            c.column_name,
            c.data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default
        FROM information_schema.columns c
        WHERE c.table_schema = %s
        AND c.table_name = %s
        ORDER BY c.ordinal_position
        """
        
        cursor.execute(query, (schema, table_name))
        columns = cursor.fetchall()
        
        structure = []
        for col in columns:
            col_name = col[0]
            data_type = col[1]
            max_length = col[2]
            precision = col[3]
            scale = col[4]
            is_nullable = col[5]
            default = col[6]
            
            # Formatar tipo de dados
            if data_type in ['character varying', 'varchar', 'char']:
                if max_length:
                    type_str = f"{data_type}({max_length})"
                else:
                    type_str = data_type
            elif data_type in ['numeric', 'decimal']:
                if precision and scale:
                    type_str = f"{data_type}({precision},{scale})"
                else:
                    type_str = data_type
            else:
                type_str = data_type
            
            structure.append({
                'name': col_name,
                'type': type_str,
                'nullable': is_nullable,
                'default': default
            })
        
        cursor.close()
        return structure
        
    except Exception as e:
        print(f"Erro ao obter estrutura da tabela {table_name}: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        if conn:
            conn.close()

def get_sample_data(table_name, limit=5):
    """Obtém alguns registros de exemplo da tabela SQL Server"""
    conn = None
    try:
        conn = DatabaseConnection.get_sql_server_prd_connection()
        cursor = conn.cursor()
        
        query = f"SELECT TOP {limit} * FROM {table_name}"
        cursor.execute(query)
        
        # Obter nomes das colunas
        columns = [column[0] for column in cursor.description]
        
        # Obter dados
        rows = cursor.fetchall()
        
        cursor.close()
        return columns, rows
        
    except Exception as e:
        print(f"Erro ao obter dados de exemplo: {e}")
        import traceback
        traceback.print_exc()
        return None, None
    finally:
        if conn:
            conn.close()

def main():
    print("="*80)
    print("INVESTIGANDO ESTRUTURA DAS TABELAS USERS")
    print("="*80)
    
    # 1. Estrutura da tabela usuarios (SQL Server)
    print("\n1. ESTRUTURA DA TABELA usuarios (SQL Server PRD):")
    print("-"*80)
    sql_structure = get_sql_server_table_structure('Usuario')
    
    if sql_structure:
        print(f"\nTotal de colunas: {len(sql_structure)}\n")
        for col in sql_structure:
            nullable_str = "NULL" if col['nullable'] == 'YES' else "NOT NULL"
            default_str = f" DEFAULT {col['default']}" if col['default'] else ""
            print(f"  - {col['name']}: {col['type']} {nullable_str}{default_str}")
    else:
        print("Nao foi possivel obter a estrutura")
    
    # 2. Estrutura da tabela users (PostgreSQL)
    print("\n\n2. ESTRUTURA DA TABELA users (PostgreSQL HML - gmcore):")
    print("-"*80)
    pg_structure = get_postgresql_table_structure('users', 'gmcore')
    
    if pg_structure:
        print(f"\nTotal de colunas: {len(pg_structure)}\n")
        for col in pg_structure:
            nullable_str = "NULL" if col['nullable'] == 'YES' else "NOT NULL"
            default_str = f" DEFAULT {col['default']}" if col['default'] else ""
            print(f"  - {col['name']}: {col['type']} {nullable_str}{default_str}")
    else:
        print("Nao foi possivel obter a estrutura")
    
    # 3. Dados de exemplo (SQL Server)
    print("\n\n3. DADOS DE EXEMPLO DA TABELA usuarios (SQL Server PRD):")
    print("-"*80)
    columns, rows = get_sample_data('Usuario', limit=3)
    
    if columns and rows:
        print(f"\nColunas: {', '.join(columns)}")
        print(f"\nPrimeiros {len(rows)} registros:")
        for idx, row in enumerate(rows, 1):
            print(f"\n  Registro {idx}:")
            for col_name, value in zip(columns, row):
                print(f"    {col_name}: {value}")
    else:
        print("Nao foi possivel obter dados de exemplo")
    
    # 4. Contagem de registros
    print("\n\n4. CONTAGEM DE REGISTROS:")
    print("-"*80)
    try:
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute("SELECT COUNT(*) FROM Usuario")
        count_sql = cursor_sql.fetchone()[0]
        cursor_sql.close()
        conn_sql.close()
        print(f"SQL Server (Usuario): {count_sql} registros")
    except Exception as e:
        print(f"Erro ao contar registros SQL Server: {e}")
    
    try:
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        cursor_pg.execute("SELECT COUNT(*) FROM gmcore.users")
        count_pg = cursor_pg.fetchone()[0]
        cursor_pg.close()
        conn_pg.close()
        print(f"PostgreSQL (gmcore.users): {count_pg} registros")
    except Exception as e:
        print(f"Erro ao contar registros PostgreSQL: {e}")
    
    print("\n" + "="*80)

if __name__ == "__main__":
    main()



