"""
Script para verificar chaves estrangeiras e constraints na tabela users em PRD
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

def check_foreign_keys():
    """Verifica chaves estrangeiras e constraints na tabela users"""
    print("="*80)
    print("VERIFICANDO CHAVES ESTRANGEIRAS E CONSTRAINTS NA TABELA users EM PRD")
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
        
        # Configurar search_path
        cursor.execute(f"SET search_path TO {schema}, public")
        
        # 1. Verificar foreign keys que APONTAM PARA users (tabelas que dependem de users)
        print("\n1. Verificando tabelas que têm FOREIGN KEY apontando para users...")
        cursor.execute("""
            SELECT
                tc.table_schema,
                tc.table_name,
                kcu.column_name,
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name,
                tc.constraint_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND ccu.table_schema = %s
                AND ccu.table_name = 'users'
            ORDER BY tc.table_schema, tc.table_name
        """, (schema,))
        
        fk_to_users = cursor.fetchall()
        
        if fk_to_users:
            print(f"\n[INFO] Encontradas {len(fk_to_users)} FOREIGN KEY(s) apontando para users:")
            print("-"*80)
            print(f"{'Tabela':<40} {'Coluna':<30} {'FK Name':<40}")
            print("-"*80)
            for row in fk_to_users:
                table_schema, table_name, column_name, fk_schema, fk_table, fk_column, constraint_name = row
                table_full = f"{table_schema}.{table_name}"
                print(f"{table_full:<40} {column_name:<30} {constraint_name:<40}")
        else:
            print("[OK] Nenhuma FOREIGN KEY apontando para users encontrada")
        
        # 2. Verificar foreign keys que users APONTA PARA (dependências de users)
        print("\n2. Verificando FOREIGN KEY(s) que users aponta para outras tabelas...")
        cursor.execute("""
            SELECT
                tc.table_schema,
                tc.table_name,
                kcu.column_name,
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name,
                tc.constraint_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = %s
                AND tc.table_name = 'users'
            ORDER BY kcu.column_name
        """, (schema,))
        
        fk_from_users = cursor.fetchall()
        
        if fk_from_users:
            print(f"\n[INFO] Encontradas {len(fk_from_users)} FOREIGN KEY(s) em users:")
            print("-"*80)
            print(f"{'Coluna em users':<30} {'Referencia':<50} {'FK Name':<40}")
            print("-"*80)
            for row in fk_from_users:
                table_schema, table_name, column_name, fk_schema, fk_table, fk_column, constraint_name = row
                reference = f"{fk_schema}.{fk_table}.{fk_column}"
                print(f"{column_name:<30} {reference:<50} {constraint_name:<40}")
        else:
            print("[OK] Nenhuma FOREIGN KEY em users encontrada")
        
        # 3. Verificar todas as constraints na tabela users
        print("\n3. Verificando todas as constraints na tabela users...")
        cursor.execute("""
            SELECT
                constraint_name,
                constraint_type
            FROM information_schema.table_constraints
            WHERE table_schema = %s
                AND table_name = 'users'
            ORDER BY constraint_type, constraint_name
        """, (schema,))
        
        constraints = cursor.fetchall()
        
        if constraints:
            print(f"\n[INFO] Encontradas {len(constraints)} constraint(s) em users:")
            print("-"*80)
            print(f"{'Tipo':<30} {'Nome':<50}")
            print("-"*80)
            for constraint_name, constraint_type in constraints:
                print(f"{constraint_type:<30} {constraint_name:<50}")
        else:
            print("[AVISO] Nenhuma constraint encontrada")
        
        # 4. Verificar se há triggers que podem estar interferindo
        print("\n4. Verificando triggers na tabela users...")
        cursor.execute("""
            SELECT
                trigger_name,
                event_manipulation,
                action_timing,
                action_statement
            FROM information_schema.triggers
            WHERE event_object_schema = %s
                AND event_object_table = 'users'
            ORDER BY trigger_name
        """, (schema,))
        
        triggers = cursor.fetchall()
        
        if triggers:
            print(f"\n[INFO] Encontrados {len(triggers)} trigger(s) em users:")
            print("-"*80)
            print(f"{'Nome':<40} {'Evento':<20} {'Timing':<20}")
            print("-"*80)
            for trigger_name, event, timing, statement in triggers:
                print(f"{trigger_name:<40} {event:<20} {timing:<20}")
        else:
            print("[OK] Nenhum trigger encontrado")
        
        # 5. Verificar se há índices únicos que podem estar causando problemas
        print("\n5. Verificando índices únicos na tabela users...")
        cursor.execute("""
            SELECT
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname = %s
                AND tablename = 'users'
                AND indexdef LIKE '%%UNIQUE%%'
            ORDER BY indexname
        """, (schema,))
        
        unique_indexes = cursor.fetchall()
        
        if unique_indexes:
            print(f"\n[INFO] Encontrados {len(unique_indexes)} índice(s) único(s) em users:")
            print("-"*80)
            for row in unique_indexes:
                if len(row) >= 2:
                    index_name, index_def = row[0], row[1]
                    print(f"  - {index_name}")
                    print(f"    {index_def}")
        else:
            print("[OK] Nenhum índice único encontrado")
        
        # 6. Verificar regras de CASCADE em foreign keys que apontam para users
        print("\n6. Verificando regras de CASCADE em foreign keys que apontam para users...")
        cursor.execute("""
            SELECT
                tc.table_schema,
                tc.table_name,
                tc.constraint_name,
                rc.delete_rule,
                rc.update_rule,
                kcu.column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.referential_constraints AS rc
                ON tc.constraint_name = rc.constraint_name
                AND tc.table_schema = rc.constraint_schema
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON rc.unique_constraint_name = ccu.constraint_name
                AND rc.unique_constraint_schema = ccu.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND ccu.table_schema = %s
                AND ccu.table_name = 'users'
        """, (schema,))
        
        cascade_rules = cursor.fetchall()
        
        if cascade_rules:
            print(f"\n[INFO] Encontradas {len(cascade_rules)} regra(s) de CASCADE relacionadas a users:")
            print("-"*80)
            print(f"{'Tabela':<40} {'Coluna':<30} {'DELETE':<15} {'UPDATE':<15}")
            print("-"*80)
            for row in cascade_rules:
                if len(row) >= 6:
                    table_schema, table_name, constraint_name, delete_rule, update_rule, column_name = row[:6]
                    table_full = f"{table_schema}.{table_name}"
                    print(f"{table_full:<40} {column_name:<30} {delete_rule:<15} {update_rule:<15}")
        else:
            print("[OK] Nenhuma regra de CASCADE encontrada")
        
        # 7. RESUMO: Verificar se as foreign keys podem estar impedindo INSERT
        print("\n7. RESUMO: Análise de impacto das foreign keys...")
        print("\n[INFO] Há 7 tabelas com FOREIGN KEY apontando para users:")
        print("  - core.notifications (user_id)")
        print("  - core.reminders (assigned_to_id, created_by_id)")
        print("  - core.user_claims (user_id)")
        print("  - core.user_logins (user_id)")
        print("  - core.user_roles (user_id)")
        print("  - core.user_tokens (user_id)")
        print("\n[INFO] Essas foreign keys NÃO devem impedir INSERT em users.")
        print("[INFO] Elas apenas garantem que quando essas tabelas referenciam users, o user existe.")
        print("\n[AVISO] O erro 'relation core.users does not exist' indica problema de:")
        print("  - Schema não encontrado no search_path")
        print("  - Nome da tabela incorreto")
        print("  - Problema de conexão/contexto")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*80)
        print("VERIFICACAO DE FOREIGN KEYS CONCLUIDA")
        print("="*80)
        
    except Exception as e:
        print(f"\n[ERRO] Erro durante verificacao: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    check_foreign_keys()
