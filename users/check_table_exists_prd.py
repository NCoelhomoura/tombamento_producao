"""
Script para verificar se a tabela users existe em PRD
"""

import sys
import os

# Adicionar diretório raiz ao path
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from utils.database_connection import DatabaseConnection

def check_table_exists():
    """Verifica se a tabela users existe em PRD"""
    print("="*80)
    print("VERIFICANDO EXISTÊNCIA DA TABELA users EM PRD")
    print("="*80)
    
    # Configurar destino como PRD
    DatabaseConnection.set_destino('PRD')
    destino = DatabaseConnection.get_destino()
    schema = 'core'
    
    print(f"Ambiente: {destino}")
    print(f"Schema: {schema}")
    print("="*80)
    
    try:
        conn = DatabaseConnection.get_postgresql_destino_connection()
        cursor = conn.cursor()
        
        # 1. Verificar se o schema existe
        print("\n1. Verificando se o schema existe...")
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = %s
        """, (schema,))
        schema_exists = cursor.fetchone()
        
        if schema_exists:
            print(f"[OK] Schema '{schema}' existe")
        else:
            print(f"[ERRO] Schema '{schema}' NÃO existe")
            cursor.close()
            conn.close()
            return
        
        # 2. Verificar se a tabela existe no schema
        print(f"\n2. Verificando se a tabela '{schema}.users' existe...")
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_name = 'users'
        """, (schema,))
        table_exists = cursor.fetchone()
        
        if table_exists:
            print(f"[OK] Tabela '{schema}.users' existe")
        else:
            print(f"[ERRO] Tabela '{schema}.users' NÃO existe")
            # Listar tabelas disponíveis no schema
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s
                ORDER BY table_name
            """, (schema,))
            tables = cursor.fetchall()
            if tables:
                print(f"\nTabelas disponíveis no schema '{schema}':")
                for table in tables:
                    print(f"  - {table[0]}")
            cursor.close()
            conn.close()
            return
        
        # 3. Verificar estrutura da tabela
        print(f"\n3. Verificando estrutura da tabela '{schema}.users'...")
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s 
            AND table_name = 'users'
            ORDER BY ordinal_position
        """, (schema,))
        columns = cursor.fetchall()
        
        if columns:
            print(f"\nColunas encontradas ({len(columns)}):")
            print("-"*80)
            print(f"{'Nome':<30} {'Tipo':<30} {'Nullable':<15}")
            print("-"*80)
            for col_name, col_type, is_nullable in columns:
                print(f"{col_name:<30} {col_type:<30} {is_nullable:<15}")
        else:
            print(f"[AVISO] Nenhuma coluna encontrada")
        
        # 4. Verificar permissões de acesso
        print(f"\n4. Verificando permissões de acesso...")
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.users")
            count = cursor.fetchone()[0]
            print(f"[OK] Permissão de leitura: OK (Total de registros: {count})")
        except Exception as e:
            print(f"[ERRO] Sem permissão de leitura: {e}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*80)
        print("VERIFICAÇÃO CONCLUÍDA")
        print("="*80)
        
    except Exception as e:
        print(f"\n[ERRO] Erro durante verificação: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    check_table_exists()
