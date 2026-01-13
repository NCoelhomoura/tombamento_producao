"""
Script para adicionar coluna legacy_id na tabela users (se não existir)
"""
import sys
import os

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.database_connection import DatabaseConnection

def add_legacy_id_to_users():
    """Adiciona coluna legacy_id na tabela users se não existir"""
    destino = DatabaseConnection.get_destino()
    schema = 'gmcore' if destino == 'HML' else 'core'
    
    print("="*80)
    print(f"ADICIONANDO COLUNA legacy_id NA TABELA users")
    print("="*80)
    print(f"Ambiente: {destino}")
    print(f"Schema: {schema}")
    print("="*80)
    
    if destino != 'HML':
        print(f"\n[AVISO] Este script so deve ser executado em HML. Ambiente atual: {destino}")
        print("Nao sera criada a coluna legacy_id em PRD.")
        return
    
    try:
        conn = DatabaseConnection.get_postgresql_destino_connection()
        cursor = conn.cursor()
        
        # Verificar se a coluna já existe
        print("\n1. Verificando se a coluna legacy_id ja existe...")
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = '{schema}' 
            AND table_name = 'users' 
            AND column_name = 'legacy_id'
        """)
        
        if cursor.fetchone():
            print(f"[OK] Coluna legacy_id ja existe na tabela {schema}.users")
            cursor.close()
            conn.close()
            return
        
        print(f"[INFO] Coluna legacy_id nao existe. Criando...")
        
        # Criar a coluna legacy_id
        print(f"\n2. Criando coluna legacy_id...")
        cursor.execute(f"""
            ALTER TABLE {schema}.users 
            ADD COLUMN legacy_id INTEGER
        """)
        conn.commit()
        
        print(f"[OK] Coluna legacy_id criada com sucesso na tabela {schema}.users")
        
        # Verificar se foi criada
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = '{schema}' 
            AND table_name = 'users' 
            AND column_name = 'legacy_id'
        """)
        col_info = cursor.fetchone()
        
        if col_info:
            print(f"\n[OK] Confirmacao:")
            print(f"   - Nome: {col_info[0]}")
            print(f"   - Tipo: {col_info[1]}")
            print(f"   - Nullable: {col_info[2]}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*80)
        print("[SUCESSO] Coluna legacy_id criada com sucesso!")
        print("="*80)
        print("\nProximos passos:")
        print("   1. Migrar dados de users do SQL Server para PostgreSQL")
        print("   2. Preencher legacy_id com os IDs originais do SQL Server")
        print("   3. Executar novamente a migracao de contracts (etapas 4 e 5)")
        
    except Exception as e:
        print(f"\n[ERRO]: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            conn.rollback()
            conn.close()

if __name__ == "__main__":
    add_legacy_id_to_users()



