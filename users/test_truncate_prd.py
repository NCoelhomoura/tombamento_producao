"""
Script para testar TRUNCATE na tabela users em PRD
"""

import sys
import os

# Adicionar diretório raiz ao path
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from utils.database_connection import DatabaseConnection

def test_truncate():
    """Testa TRUNCATE na tabela users em PRD"""
    print("="*80)
    print("TESTANDO TRUNCATE NA TABELA users EM PRD")
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
        
        # Testar diferentes formas de referenciar a tabela
        print("\n1. Testando SELECT COUNT(*) FROM core.users...")
        try:
            cursor.execute("SELECT COUNT(*) FROM core.users")
            count = cursor.fetchone()[0]
            print(f"[OK] SELECT funcionou. Total de registros: {count}")
        except Exception as e:
            print(f"[ERRO] SELECT falhou: {e}")
        
        print("\n2. Testando SELECT COUNT(*) com schema entre aspas...")
        try:
            cursor.execute('SELECT COUNT(*) FROM "core"."users"')
            count = cursor.fetchone()[0]
            print(f"[OK] SELECT com aspas funcionou. Total de registros: {count}")
        except Exception as e:
            print(f"[ERRO] SELECT com aspas falhou: {e}")
        
        print("\n3. Testando SET search_path...")
        try:
            cursor.execute(f"SET search_path TO {schema}, public")
            cursor.execute("SELECT COUNT(*) FROM users")
            count = cursor.fetchone()[0]
            print(f"[OK] SELECT com search_path funcionou. Total de registros: {count}")
        except Exception as e:
            print(f"[ERRO] SELECT com search_path falhou: {e}")
        
        print("\n4. Verificando search_path atual...")
        cursor.execute("SHOW search_path")
        search_path = cursor.fetchone()[0]
        print(f"[INFO] search_path atual: {search_path}")
        
        print("\n5. Testando TRUNCATE TABLE core.users (SEM executar)...")
        print("[INFO] Não executando TRUNCATE para não perder dados")
        print("[INFO] Query seria: TRUNCATE TABLE core.users CASCADE")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*80)
        print("TESTE CONCLUÍDO")
        print("="*80)
        
    except Exception as e:
        print(f"\n[ERRO] Erro durante teste: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    test_truncate()
