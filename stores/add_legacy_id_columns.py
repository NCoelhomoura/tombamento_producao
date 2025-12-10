"""
Script para adicionar coluna legacy_id nas tabelas stores, store_brands e retail_chains
"""

import sys
import io
import os
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Adicionar diretório utils ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database_connection import DatabaseConnection


def column_exists(cursor, schema, table, column):
    """Verifica se uma coluna existe na tabela"""
    query = """
    SELECT COUNT(*) 
    FROM information_schema.columns 
    WHERE table_schema = %s 
    AND table_name = %s 
    AND column_name = %s
    """
    cursor.execute(query, (schema, table, column))
    return cursor.fetchone()[0] > 0


def add_legacy_id_column(table_name):
    """Adiciona coluna legacy_id em uma tabela"""
    conn = None
    try:
        conn = DatabaseConnection.get_postgresql_hml_connection()
        cursor = conn.cursor()
        
        schema = 'gmcore'
        
        # Verificar se a coluna já existe
        if column_exists(cursor, schema, table_name, 'legacy_id'):
            print(f"  Coluna legacy_id ja existe na tabela {schema}.{table_name}")
            cursor.close()
            conn.close()
            return True
        
        # Adicionar coluna
        alter_query = f"""
        ALTER TABLE {schema}.{table_name}
        ADD COLUMN legacy_id INTEGER
        """
        
        cursor.execute(alter_query)
        conn.commit()
        
        print(f"  OK - Coluna legacy_id adicionada em {schema}.{table_name}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"  ERRO ao adicionar coluna legacy_id em {schema}.{table_name}: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False


def main():
    """Função principal"""
    print("="*80)
    print("ADICIONANDO COLUNA legacy_id NAS TABELAS")
    print("="*80)
    print()
    
    tables = [
        'stores',
        'store_brands',
        'retail_chains'
    ]
    
    success_count = 0
    for table in tables:
        print(f"Processando tabela: {table}")
        if add_legacy_id_column(table):
            success_count += 1
        print()
    
    print("="*80)
    print(f"RESUMO: {success_count}/{len(tables)} tabelas processadas com sucesso")
    print("="*80)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nERRO: {e}")
        import traceback
        traceback.print_exc()


