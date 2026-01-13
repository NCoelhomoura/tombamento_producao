"""
Script para verificar a estrutura e dados da tabela users
"""
import sys
import os

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.database_connection import DatabaseConnection

def check_users_table():
    """Verifica a estrutura e dados da tabela users"""
    destino = DatabaseConnection.get_destino()
    schema = 'gmcore' if destino == 'HML' else 'core'
    
    print("="*80)
    print(f"VERIFICANDO TABELA users")
    print("="*80)
    print(f"Ambiente: {destino}")
    print(f"Schema: {schema}")
    print("="*80)
    
    try:
        conn = DatabaseConnection.get_postgresql_destino_connection()
        cursor = conn.cursor()
        
        # 1. Verificar se a tabela existe
        print("\n1. Verificando se a tabela existe...")
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = '{schema}' 
                AND table_name = 'users'
            )
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print(f"[ERRO] Tabela {schema}.users NAO EXISTE!")
            cursor.close()
            conn.close()
            return
        
        print(f"[OK] Tabela {schema}.users existe")
        
        # 2. Verificar estrutura da tabela (colunas)
        print("\n2. Verificando estrutura da tabela...")
        cursor.execute(f"""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = 'users'
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        
        print(f"\nColunas encontradas ({len(columns)}):")
        has_legacy_id = False
        for col_name, data_type, is_nullable, default in columns:
            nullable_str = "NULL" if is_nullable == 'YES' else "NOT NULL"
            default_str = f" DEFAULT {default}" if default else ""
            print(f"  - {col_name}: {data_type} {nullable_str}{default_str}")
            if col_name == 'legacy_id':
                has_legacy_id = True
        
        if not has_legacy_id:
            print("\n[AVISO] Coluna 'legacy_id' NAO EXISTE na tabela!")
        else:
            print("\n[OK] Coluna 'legacy_id' existe na tabela")
        
        # 3. Verificar quantidade de registros
        print("\n3. Verificando quantidade de registros...")
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.users")
        total_count = cursor.fetchone()[0]
        print(f"Total de registros: {total_count}")
        
        if total_count == 0:
            print("[AVISO] Tabela esta VAZIA!")
        else:
            print("[OK] Tabela possui dados")
        
        # 4. Se legacy_id existe, verificar quantos têm legacy_id preenchido
        if has_legacy_id:
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.users WHERE legacy_id IS NOT NULL")
            with_legacy_id = cursor.fetchone()[0]
            print(f"\nRegistros com legacy_id preenchido: {with_legacy_id}")
            
            if with_legacy_id == 0:
                print("[AVISO] Nenhum registro possui legacy_id preenchido!")
            else:
                print("[OK] Existem registros com legacy_id")
        
        # 5. Verificar alguns exemplos de dados (se existirem)
        if total_count > 0:
            print("\n4. Exemplos de registros (primeiros 5):")
            limit = min(5, total_count)
            if has_legacy_id:
                cursor.execute(f"""
                    SELECT id, legacy_id, created_at 
                    FROM {schema}.users 
                    ORDER BY created_at DESC
                    LIMIT {limit}
                """)
            else:
                cursor.execute(f"""
                    SELECT id, created_at 
                    FROM {schema}.users 
                    ORDER BY created_at DESC
                    LIMIT {limit}
                """)
            
            rows = cursor.fetchall()
            for idx, row in enumerate(rows, 1):
                if has_legacy_id:
                    print(f"  {idx}. id={row[0]}, legacy_id={row[1]}, created_at={row[2]}")
                else:
                    print(f"  {idx}. id={row[0]}, created_at={row[1]}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*80)
        print("RESUMO:")
        print("="*80)
        print(f"Tabela existe: {'SIM' if table_exists else 'NAO'}")
        print(f"Coluna legacy_id existe: {'SIM' if has_legacy_id else 'NAO'}")
        print(f"Total de registros: {total_count}")
        if has_legacy_id:
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.users WHERE legacy_id IS NOT NULL")
            with_legacy_id = cursor.fetchone()[0]
            print(f"Registros com legacy_id: {with_legacy_id}")
        
        print("\n" + "="*80)
        if not has_legacy_id:
            print("[ACAO NECESSARIA]:")
            print(f"   - Criar coluna legacy_id na tabela {schema}.users")
            print(f"   - Migrar dados de users do SQL Server para PostgreSQL")
            print(f"   - Preencher legacy_id com os IDs originais do SQL Server")
        elif total_count == 0:
            print("[ACAO NECESSARIA]:")
            print(f"   - Migrar dados de users do SQL Server para PostgreSQL")
            print(f"   - Preencher legacy_id com os IDs originais do SQL Server")
        elif has_legacy_id and total_count > 0:
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.users WHERE legacy_id IS NOT NULL")
            with_legacy_id = cursor.fetchone()[0]
            if with_legacy_id == 0:
                print("[ACAO NECESSARIA]:")
                print(f"   - Preencher legacy_id dos registros existentes")
            else:
                print("[OK] Tabela esta pronta para uso!")
        
    except Exception as e:
        print(f"\n[ERRO]: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_users_table()

