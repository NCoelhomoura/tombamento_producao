"""
Script para verificar a estrutura da tabela users em PRD e testar inserção
"""

import sys
import os

# Adicionar diretório raiz ao path
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from utils.database_connection import DatabaseConnection

def check_users_table_prd():
    """Verifica a estrutura e dados da tabela users em PRD"""
    print("="*80)
    print("VERIFICANDO TABELA users EM PRD")
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
                character_maximum_length,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = 'users'
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        
        print(f"\nColunas encontradas ({len(columns)}):")
        print("-"*80)
        print(f"{'Nome':<30} {'Tipo':<30} {'Nullable':<10} {'Default'}")
        print("-"*80)
        
        has_legacy_id = False
        for col_name, data_type, max_length, is_nullable, default in columns:
            nullable_str = "SIM" if is_nullable == 'YES' else "NAO"
            type_str = f"{data_type}({max_length})" if max_length else data_type
            default_str = str(default)[:30] if default else ''
            print(f"{col_name:<30} {type_str:<30} {nullable_str:<10} {default_str}")
            if col_name == 'legacy_id':
                has_legacy_id = True
        
        # 3. Verificar se legacy_id existe
        print(f"\n3. Coluna legacy_id existe: {'SIM' if has_legacy_id else 'NAO'}")
        
        # 4. Contar registros existentes
        print("\n4. Contando registros existentes...")
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.users")
        count = cursor.fetchone()[0]
        print(f"[INFO] Total de registros na tabela: {count}")
        
        # 5. Verificar se há registros com legacy_id
        if has_legacy_id:
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.users WHERE legacy_id IS NOT NULL")
            count_with_legacy = cursor.fetchone()[0]
            print(f"[INFO] Registros com legacy_id: {count_with_legacy}")
        
        # 6. Verificar constraints e índices
        print("\n5. Verificando constraints...")
        cursor.execute(f"""
            SELECT 
                constraint_name,
                constraint_type
            FROM information_schema.table_constraints
            WHERE table_schema = '{schema}'
            AND table_name = 'users'
        """)
        constraints = cursor.fetchall()
        print(f"Constraints encontradas ({len(constraints)}):")
        for const_name, const_type in constraints:
            print(f"  - {const_name}: {const_type}")
        
        # 7. Testar inserção simples (se não houver dados)
        if count == 0:
            print("\n6. Testando inserção simples...")
            try:
                test_query = f"""
                INSERT INTO {schema}.users (
                    id, name, user_name, normalized_user_name, email, normalized_email,
                    phone_number, password_hash, status, created_at, updated_at, deleted_at,
                    email_confirmed, email_confirmed_at, phone_number_confirmed, temporary_password,
                    two_factor_enabled, lockout_enabled, lockout_end, access_failed_count,
                    security_stamp, concurrency_stamp
                ) VALUES (
                    gen_random_uuid(), 'TESTE', 'teste', 'TESTE', 'teste@teste.com', 'teste@teste.com',
                    NULL, 'hash123', 'active', NOW(), NOW(), NULL,
                    true, NOW(), false, false,
                    false, true, NULL, 0,
                    NULL, NULL
                )
                """
                cursor.execute(test_query)
                conn.commit()
                print("[OK] Inserção de teste bem-sucedida")
                
                # Deletar registro de teste
                cursor.execute(f"DELETE FROM {schema}.users WHERE name = 'TESTE'")
                conn.commit()
                print("[OK] Registro de teste removido")
            except Exception as e:
                print(f"[ERRO] Falha na inserção de teste: {e}")
                conn.rollback()
        
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
    check_users_table_prd()
