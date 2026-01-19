"""
Script para verificar dados na tabela users em PRD
"""

import sys
import os

# Adicionar diretório raiz ao path
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from utils.database_connection import DatabaseConnection

def check_users_data_prd():
    """Verifica os dados da tabela users em PRD"""
    print("="*80)
    print("VERIFICANDO DADOS DA TABELA users EM PRD")
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
        
        # 1. Contar total de registros
        print("\n1. Contando registros...")
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.users")
        total_count = cursor.fetchone()[0]
        print(f"[INFO] Total de registros: {total_count}")
        
        # 2. Contar registros com legacy_id
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.users WHERE legacy_id IS NOT NULL")
        count_with_legacy = cursor.fetchone()[0]
        print(f"[INFO] Registros com legacy_id: {count_with_legacy}")
        
        # 3. Contar registros sem legacy_id
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.users WHERE legacy_id IS NULL")
        count_without_legacy = cursor.fetchone()[0]
        print(f"[INFO] Registros sem legacy_id: {count_without_legacy}")
        
        # 4. Buscar alguns registros de exemplo
        print("\n2. Buscando registros de exemplo (primeiros 10)...")
        cursor.execute(f"""
            SELECT 
                id,
                name,
                user_name,
                email,
                legacy_id,
                status,
                created_at
            FROM {schema}.users
            ORDER BY created_at DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()
        
        if rows:
            print("\nRegistros encontrados:")
            print("-"*80)
            print(f"{'ID (UUID)':<40} {'Name':<30} {'User Name':<20} {'Legacy ID':<12} {'Status':<15}")
            print("-"*80)
            for row in rows:
                uuid_str = str(row[0])[:36] if row[0] else 'NULL'
                name_str = str(row[1])[:28] if row[1] else 'NULL'
                user_name_str = str(row[2])[:18] if row[2] else 'NULL'
                legacy_id_str = str(row[4]) if row[4] is not None else 'NULL'
                status_str = str(row[5])[:13] if row[5] else 'NULL'
                print(f"{uuid_str:<40} {name_str:<30} {user_name_str:<20} {legacy_id_str:<12} {status_str:<15}")
        else:
            print("[AVISO] Nenhum registro encontrado na tabela")
        
        # 5. Verificar estrutura completa de alguns registros
        if rows:
            print("\n3. Detalhes completos do primeiro registro:")
            print("-"*80)
            first_row = rows[0]
            cursor.execute(f"""
                SELECT 
                    id, legacy_id, name, user_name, normalized_user_name, 
                    email, normalized_email, phone_number, password_hash, 
                    status, created_at, updated_at, deleted_at,
                    email_confirmed, email_confirmed_at, phone_number_confirmed,
                    temporary_password, two_factor_enabled, lockout_enabled,
                    lockout_end, access_failed_count, security_stamp, concurrency_stamp
                FROM {schema}.users
                WHERE id = %s
            """, (first_row[0],))
            detail_row = cursor.fetchone()
            
            if detail_row:
                print(f"ID: {detail_row[0]}")
                print(f"Legacy ID: {detail_row[1]}")
                print(f"Name: {detail_row[2]}")
                print(f"User Name: {detail_row[3]}")
                print(f"Normalized User Name: {detail_row[4]}")
                print(f"Email: {detail_row[5]}")
                print(f"Normalized Email: {detail_row[6]}")
                print(f"Phone Number: {detail_row[7]}")
                print(f"Password Hash: {'***' if detail_row[8] else 'NULL'}")
                print(f"Status: {detail_row[9]}")
                print(f"Created At: {detail_row[10]}")
                print(f"Updated At: {detail_row[11]}")
                print(f"Deleted At: {detail_row[12]}")
                print(f"Email Confirmed: {detail_row[13]}")
                print(f"Email Confirmed At: {detail_row[14]}")
                print(f"Phone Number Confirmed: {detail_row[15]}")
                print(f"Temporary Password: {detail_row[16]}")
                print(f"Two Factor Enabled: {detail_row[17]}")
                print(f"Lockout Enabled: {detail_row[18]}")
                print(f"Lockout End: {detail_row[19]}")
                print(f"Access Failed Count: {detail_row[20]}")
                print(f"Security Stamp: {detail_row[21]}")
                print(f"Concurrency Stamp: {detail_row[22]}")
        
        # 6. Verificar se há registros duplicados por legacy_id
        print("\n4. Verificando duplicatas por legacy_id...")
        cursor.execute(f"""
            SELECT legacy_id, COUNT(*) as count
            FROM {schema}.users
            WHERE legacy_id IS NOT NULL
            GROUP BY legacy_id
            HAVING COUNT(*) > 1
            ORDER BY count DESC
            LIMIT 10
        """)
        duplicates = cursor.fetchall()
        
        if duplicates:
            print(f"[AVISO] Encontradas {len(duplicates)} duplicatas por legacy_id:")
            for leg_id, count in duplicates:
                print(f"  - Legacy ID {leg_id}: {count} registros")
        else:
            print("[OK] Nenhuma duplicata encontrada por legacy_id")
        
        # 7. Verificar range de legacy_id
        print("\n5. Verificando range de legacy_id...")
        cursor.execute(f"""
            SELECT 
                MIN(legacy_id) as min_id,
                MAX(legacy_id) as max_id,
                COUNT(DISTINCT legacy_id) as unique_count
            FROM {schema}.users
            WHERE legacy_id IS NOT NULL
        """)
        range_info = cursor.fetchone()
        
        if range_info and range_info[0] is not None:
            print(f"[INFO] Legacy ID mínimo: {range_info[0]}")
            print(f"[INFO] Legacy ID máximo: {range_info[1]}")
            print(f"[INFO] Legacy IDs únicos: {range_info[2]}")
        else:
            print("[AVISO] Nenhum legacy_id encontrado")
        
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
    check_users_data_prd()
