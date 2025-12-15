"""
Script para adicionar coluna legacy_id nas tabelas contracts e contract_scenario_stores do schema gmcommercial
A coluna será criada apenas quando o ambiente for HML e quando ela não existir na tabela
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
    result = cursor.fetchone()
    return result[0] > 0 if result else False


def add_legacy_id_column_to_table(cursor, schema, table_name, column_name):
    """Adiciona coluna legacy_id em uma tabela específica se não existir"""
    try:
        print(f"\nVerificando coluna {column_name} na tabela {schema}.{table_name}...")
        
        # Verificar se a coluna já existe
        if column_exists(cursor, schema, table_name, column_name):
            print(f"  OK - Coluna {column_name} já existe na tabela {schema}.{table_name}")
            return True
        
        # Adicionar coluna
        print(f"  Criando coluna {column_name}...")
        alter_query = f"""
        ALTER TABLE {schema}.{table_name}
        ADD COLUMN {column_name} INTEGER
        """
        
        cursor.execute(alter_query)
        
        # Verificar se foi criada
        if column_exists(cursor, schema, table_name, column_name):
            print(f"  OK - Coluna {column_name} adicionada com sucesso em {schema}.{table_name}")
            return True
        else:
            print(f"  AVISO - Coluna {column_name} pode não ter sido criada corretamente")
            return False
        
    except Exception as e:
        print(f"  ERRO ao adicionar coluna legacy_id em {schema}.{table_name}: {e}")
        import traceback
        traceback.print_exc()
        return False


def add_legacy_id_columns():
    """Adiciona coluna legacy_id nas tabelas contracts e contract_scenario_stores se não existir"""
    conn = None
    try:
        # Verificar se o ambiente é HML
        destino = DatabaseConnection.get_destino()
        if destino != 'HML':
            print(f"AVISO: Ambiente atual é {destino}. Coluna legacy_id será criada apenas em HML.")
            print("Para criar a coluna, configure o destino para HML usando DatabaseConnection.set_destino('HML')")
            return False
        
        conn = DatabaseConnection.get_postgresql_hml_connection()
        cursor = conn.cursor()
        
        schema = 'gmcommercial'
        column_name = 'legacy_id'
        
        success_count = 0
        
        # Adicionar em contracts
        if add_legacy_id_column_to_table(cursor, schema, 'contracts', column_name):
            success_count += 1
        
        # Adicionar em contract_scenario_stores
        if add_legacy_id_column_to_table(cursor, schema, 'contract_scenario_stores', column_name):
            success_count += 1
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return success_count == 2
        
    except Exception as e:
        print(f"  ERRO ao adicionar colunas legacy_id: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()
        return False


def main():
    """Função principal"""
    print("="*80)
    print("ADICIONANDO COLUNA legacy_id NAS TABELAS CONTRACTS E CONTRACT_SCENARIO_STORES")
    print("="*80)
    
    # Configurar destino como HML
    DatabaseConnection.set_destino('HML')
    destino_atual = DatabaseConnection.get_destino()
    print(f"\nDestino configurado: {destino_atual}")
    
    if add_legacy_id_columns():
        print("\n" + "="*80)
        print("PROCESSO CONCLUÍDO COM SUCESSO")
        print("="*80)
    else:
        print("\n" + "="*80)
        print("PROCESSO CONCLUÍDO COM AVISOS")
        print("="*80)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nERRO CRITICO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

