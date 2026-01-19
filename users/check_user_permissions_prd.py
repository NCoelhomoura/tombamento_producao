"""
Script para verificar permissões do usuário do banco de dados em PRD
"""

import sys
import os
import io

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Adicionar diretório raiz ao path
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from utils.database_connection import DatabaseConnection

def check_permissions():
    """Verifica permissões do usuário do banco de dados"""
    print("="*80)
    print("VERIFICANDO PERMISSÕES DO USUÁRIO DO BANCO DE DADOS EM PRD")
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
        
        # 1. Verificar usuário atual
        print("\n1. Verificando usuário atual...")
        cursor.execute("SELECT current_user, session_user")
        current_user, session_user = cursor.fetchone()
        print(f"[INFO] Usuário atual: {current_user}")
        print(f"[INFO] Usuário da sessão: {session_user}")
        
        # 2. Verificar permissões no schema
        print(f"\n2. Verificando permissões no schema '{schema}'...")
        cursor.execute("""
            SELECT 
                nspname as schema_name,
                nspacl as privileges
            FROM pg_namespace
            WHERE nspname = %s
        """, (schema,))
        schema_info = cursor.fetchone()
        
        if schema_info:
            print(f"[INFO] Schema encontrado: {schema_info[0]}")
            print(f"[INFO] Privilégios do schema: {schema_info[1]}")
        else:
            print(f"[ERRO] Schema '{schema}' não encontrado")
        
        # 3. Verificar permissões na tabela users
        print(f"\n3. Verificando permissões na tabela '{schema}.users'...")
        cursor.execute("""
            SELECT 
                table_schema,
                table_name,
                privilege_type,
                grantee
            FROM information_schema.table_privileges
            WHERE table_schema = %s 
            AND table_name = 'users'
            AND grantee = current_user
            ORDER BY privilege_type
        """, (schema,))
        table_privileges = cursor.fetchall()
        
        if table_privileges:
            print(f"\n[INFO] Permissões encontradas ({len(table_privileges)}):")
            print("-"*80)
            print(f"{'Tipo':<20} {'Grantee':<30}")
            print("-"*80)
            for schema_name, table_name, priv_type, grantee in table_privileges:
                print(f"{priv_type:<20} {grantee:<30}")
        else:
            print(f"[AVISO] Nenhuma permissão explícita encontrada para o usuário atual")
        
        # 4. Verificar permissões usando has_table_privilege
        print(f"\n4. Verificando permissões específicas usando has_table_privilege...")
        permissions_to_check = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE']
        
        for perm in permissions_to_check:
            try:
                # Usar formato correto: 'schema.table' como string
                cursor.execute("""
                    SELECT has_table_privilege(%s, %s)
                """, (f'"{schema}"."users"', perm))
                has_perm = cursor.fetchone()[0]
                status = "✅ SIM" if has_perm else "❌ NÃO"
                print(f"[{status}] Permissão {perm:<10}: {has_perm}")
            except Exception as e:
                # Tentar formato alternativo
                try:
                    cursor.execute("""
                        SELECT has_table_privilege(%s, %s)
                    """, (f'{schema}.users', perm))
                    has_perm = cursor.fetchone()[0]
                    status = "✅ SIM" if has_perm else "❌ NÃO"
                    print(f"[{status}] Permissão {perm:<10}: {has_perm}")
                except Exception as e2:
                    print(f"[ERRO] Erro ao verificar {perm}: {e2}")
        
        # 5. Verificar se o usuário tem role que permite essas operações
        print(f"\n5. Verificando roles do usuário...")
        cursor.execute("""
            SELECT 
                r.rolname,
                r.rolsuper,
                r.rolcreaterole,
                r.rolcreatedb
            FROM pg_roles r
            WHERE r.rolname = current_user
        """)
        role_info = cursor.fetchone()
        
        if role_info:
            role_name, is_superuser, can_create_role, can_create_db = role_info
            print(f"[INFO] Role: {role_name}")
            print(f"[INFO] Superusuário: {'SIM' if is_superuser else 'NÃO'}")
            print(f"[INFO] Pode criar roles: {'SIM' if can_create_role else 'NÃO'}")
            print(f"[INFO] Pode criar databases: {'SIM' if can_create_db else 'NÃO'}")
        
        # 6. Testar operações práticas (sem executar, apenas verificar)
        print(f"\n6. Testando operações práticas...")
        
        # 6.1. Testar SELECT
        print("  6.1. Testando SELECT...")
        try:
            cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."users"')
            count = cursor.fetchone()[0]
            print(f"      [OK] SELECT funcionou. Total de registros: {count}")
        except Exception as e:
            print(f"      [ERRO] SELECT falhou: {e}")
        
        # 6.2. Testar INSERT (sem executar, apenas verificar sintaxe)
        print("  6.2. Verificando se pode fazer INSERT...")
        try:
            # Verificar se a tabela existe e se temos permissão
            cursor.execute("""
                SELECT has_table_privilege(%s, 'INSERT')
            """, (f'"{schema}"."users"',))
            can_insert = cursor.fetchone()[0]
            if can_insert:
                print(f"      [OK] Permissao de INSERT confirmada")
            else:
                print(f"      [ERRO] Sem permissao de INSERT")
        except Exception as e:
            print(f"      ❌ Erro ao verificar INSERT: {e}")
        
        # 6.3. Testar DELETE (sem executar, apenas verificar)
        print("  6.3. Verificando se pode fazer DELETE...")
        try:
            cursor.execute("""
                SELECT has_table_privilege(%s, 'DELETE')
            """, (f'"{schema}"."users"',))
            can_delete = cursor.fetchone()[0]
            if can_delete:
                print(f"      [OK] Permissao de DELETE confirmada")
            else:
                print(f"      [ERRO] Sem permissao de DELETE")
        except Exception as e:
            print(f"      ❌ Erro ao verificar DELETE: {e}")
        
        # 6.4. Testar TRUNCATE (sem executar, apenas verificar)
        print("  6.4. Verificando se pode fazer TRUNCATE...")
        try:
            cursor.execute("""
                SELECT has_table_privilege(%s, 'TRUNCATE')
            """, (f'"{schema}"."users"',))
            can_truncate = cursor.fetchone()[0]
            if can_truncate:
                print(f"      [OK] Permissao de TRUNCATE confirmada")
            else:
                print(f"      [ERRO] Sem permissao de TRUNCATE")
        except Exception as e:
            print(f"      ❌ Erro ao verificar TRUNCATE: {e}")
        
        # 7. Verificar se há políticas RLS (Row Level Security) ativas
        print(f"\n7. Verificando políticas RLS (Row Level Security)...")
        cursor.execute(f"""
            SELECT 
                tablename,
                rowsecurity
            FROM pg_tables
            WHERE schemaname = %s 
            AND tablename = 'users'
        """, (schema,))
        rls_info = cursor.fetchone()
        
        if rls_info:
            table_name, has_rls = rls_info
            print(f"[INFO] RLS ativo: {'SIM' if has_rls else 'NÃO'}")
            if has_rls:
                print(f"[AVISO] Row Level Security está ativo - pode afetar operações")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*80)
        print("VERIFICAÇÃO DE PERMISSÕES CONCLUÍDA")
        print("="*80)
        
    except Exception as e:
        print(f"\n[ERRO] Erro durante verificação: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    check_permissions()
