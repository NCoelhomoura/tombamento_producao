"""
Orchestrator para gerenciar as tasks de migracao de dados
"""

import sys
import io
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import os
import sys
from datetime import datetime

# Configuracao de limite de linhas
# 0 = migrar todos os dados
# > 0 = limitar quantidade de linhas
LIMIT_ROWS = 0

# Configuracao de destino
# 'HML' = PostgreSQL HML (padrao)
# 'PRD' = PostgreSQL PRD AWS
DESTINO_PADRAO = 'PRD'  # Mudado para PRD como padrão

# Limpar arquivo de log no inicio do orchestrator
def clear_log_file():
    """Limpa o arquivo de log na raiz do projeto"""
    try:
        log_file_path = os.path.join(os.path.dirname(__file__), 'log_execution.txt')
        log_file_path = os.path.abspath(log_file_path)
        if os.path.exists(log_file_path):
            with open(log_file_path, 'w', encoding='utf-8') as f:
                f.write('')  # Truncar arquivo
    except Exception as e:
        pass  # Se houver erro, continuar normalmente

# Adicionar diretorios ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'utils'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'customers'))


def run_customers_task(step=None, limit_rows=0):
    """
    Executa a task de migracao de customers
    
    Args:
        step: None para todas as etapas, ou '1', '2', '3', '4' para etapa especifica
        limit_rows: 0 para todos os dados, > 0 para limitar quantidade
    """
    print("\n" + "="*80)
    print("TASK 1: CUSTOMERS")
    print("="*80)
    print(f"Data/Hora: {datetime.now()}")
    print(f"Limite de linhas: {'TODOS' if limit_rows == 0 else limit_rows}")
    if step:
        print(f"Etapa: {step}")
    print("="*80)
    
    from customers.customers_to_core import CustomersMigration
    
    migration = CustomersMigration(limit_rows=limit_rows)
    
    if step:
        # Executar apenas etapa especifica
        if step == '1':
            migration.step1_migrate_customer_segments()
        elif step == '2':
            # Carregar segmentos primeiro
            from utils.database_connection import DatabaseConnection
            from customers.customers_to_core import get_schema_atual
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.customer_segments")
            for row in cursor_pg.fetchall():
                migration.segment_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            migration.step2_migrate_customers()
        elif step == '3':
            # Carregar customers primeiro
            from utils.database_connection import DatabaseConnection
            from customers.customers_to_core import get_schema_atual
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.customers")
            for row in cursor_pg.fetchall():
                migration.customer_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            migration.step3_migrate_addresses()
        elif step == '4':
            # Carregar customers primeiro
            from utils.database_connection import DatabaseConnection
            from customers.customers_to_core import get_schema_atual
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.customers")
            for row in cursor_pg.fetchall():
                migration.customer_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            migration.step4_migrate_contacts()
        else:
            print(f"ERRO: Etapa {step} invalida. Use 1, 2, 3 ou 4")
            return
    else:
        # Executar todas as etapas
        migration.run()


def run_stores_task(step=None, limit_rows=0):
    """
    Executa a task de migracao de stores
    
    Args:
        step: None para todas as etapas, ou '1', '2', '3', etc. para etapa especifica
        limit_rows: 0 para todos os dados, > 0 para limitar quantidade
    """
    from utils.database_connection import DatabaseConnection
    destino_atual = DatabaseConnection.get_destino()
    
    print("\n" + "="*80)
    print("TASK 2: STORES")
    print("="*80)
    print(f"Data/Hora: {datetime.now()}")
    print(f"Destino: {destino_atual}")
    print(f"Limite de linhas: {'TODOS' if limit_rows == 0 else limit_rows}")
    if step:
        print(f"Etapa: {step}")
    print("="*80)
    
    # Adicionar diretório stores ao path
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'stores'))
    from stores.stores_to_core import StoresMigration
    
    migration = StoresMigration(limit_rows=limit_rows)
    
    if step:
        # Executar apenas uma etapa específica
        if step == '1':
            migration.step1_migrate_store_segments()
        elif step == '2':
            migration.step2_migrate_retail_chains()
        elif step == '3':
            # Carregar mapeamentos necessários
            from stores.stores_to_core import get_schema_atual
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.retail_chains")
            for row in cursor_pg.fetchall():
                migration.retail_chain_id_map[row[1]] = row[0]
            cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.store_segments")
            for row in cursor_pg.fetchall():
                migration.store_segment_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            migration.step3_migrate_store_brands()
        elif step == '4':
            # Carregar mapeamento de store_brands (apenas se houver legacy_id)
            from stores.stores_to_core import get_schema_atual
            schema = get_schema_atual()
            destino = DatabaseConnection.get_destino()
            if destino == 'HML':
                # Em HML, buscar por legacy_id
                conn_pg = DatabaseConnection.get_postgresql_destino_connection()
                cursor_pg = conn_pg.cursor()
                cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.store_brands")
                for row in cursor_pg.fetchall():
                    migration.store_brand_id_map[row[1]] = row[0]
                cursor_pg.close()
                conn_pg.close()
            # Em PRD, o mapeamento será carregado dentro do step4_migrate_stores
            migration.step4_migrate_stores()
        elif step == '5':
            # Carregar mapeamento de stores
            from stores.stores_to_core import get_schema_atual
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.stores")
            for row in cursor_pg.fetchall():
                migration.store_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            migration.step5_migrate_store_cnpjs()
        elif step == '6':
            # Carregar mapeamento de stores
            from stores.stores_to_core import get_schema_atual
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.stores")
            for row in cursor_pg.fetchall():
                migration.store_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            migration.step6_migrate_addresses()
        elif step == '7':
            # Carregar mapeamento de stores
            from stores.stores_to_core import get_schema_atual
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.stores")
            for row in cursor_pg.fetchall():
                migration.store_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            migration.step7_migrate_contacts()
        else:
            print(f"ERRO: Etapa '{step}' invalida para stores")
            print("Etapas disponiveis: 1, 2, 3, 4, 5, 6, 7")
            return
    else:
        # Executar migração completa
        migration.run()


def main():
    """Funcao principal do orchestrator"""
    # Limpar arquivo de log no inicio
    clear_log_file()
    
    print("\n" + "="*80)
    print("ORCHESTRATOR DE MIGRACAO DE DADOS")
    print("="*80)
    print(f"Data/Hora: {datetime.now()}")
    print("="*80)

    # Ler configuracao de limite e destino
    global LIMIT_ROWS, DESTINO_PADRAO
    limit_rows = LIMIT_ROWS
    destino = DESTINO_PADRAO
    
    # Processar argumentos da linha de comando
    # Formato: python orchestrator_tasks.py [task] [step] [--limit N] [--destino HML|PRD]
    # Exemplos:
    #   python orchestrator_tasks.py                    # Executa todas as tasks
    #   python orchestrator_tasks.py --limit 100         # Executa todas as tasks com limite
    #   python orchestrator_tasks.py customers           # Executa apenas customers
    #   python orchestrator_tasks.py customers 1         # Executa apenas etapa 1 de customers
    #   python orchestrator_tasks.py customers 1 --limit 1000
    #   python orchestrator_tasks.py customers --destino PRD
    #   python orchestrator_tasks.py stores 2 --limit 500 --destino HML
    
    task = None
    step = None
    limit = 0
    
    # Processar argumentos
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--limit' and i + 1 < len(sys.argv):
            limit = int(sys.argv[i + 1])
            i += 2
        elif arg == '--destino' and i + 1 < len(sys.argv):
            destino_arg = sys.argv[i + 1].upper()
            if destino_arg in ['HML', 'PRD']:
                destino = destino_arg
                print(f"DEBUG: Destino recebido via argumento: {destino}")
            else:
                print(f"AVISO: Destino invalido '{sys.argv[i + 1]}'. Usando padrao: {destino}")
            i += 2
        elif arg.lower() in ['customers', 'stores']:
            task = arg.lower()
            i += 1
        elif arg.isdigit():
            step = arg
            i += 1
        else:
            i += 1
    
    # Usar limite global se nao especificado
    if limit == 0:
        limit = limit_rows
    
    # Configurar destino ANTES de importar qualquer módulo de migração
    from utils.database_connection import DatabaseConnection
    DatabaseConnection.set_destino(destino)
    destino_verificado = DatabaseConnection.get_destino()
    print(f"\nDestino configurado: {destino} (verificado: {destino_verificado})")
    print("="*80)
    
    # Recarregar módulos de migração para garantir que leiam o destino correto
    import importlib
    modules_to_reload = []
    for module_name in sys.modules.keys():
        if 'customers_to_core' in module_name or 'stores_to_core' in module_name:
            modules_to_reload.append(module_name)
    for module_name in modules_to_reload:
        try:
            importlib.reload(sys.modules[module_name])
        except Exception:
            pass  # Ignorar erros de reload
    
    # Executar task(s)
    if task is None:
        # Executar todas as tasks
        print("\n" + "="*80)
        print("EXECUTANDO TODAS AS TASKS")
        print("="*80)
        
        # Task 1: Customers
        print("\n" + "="*80)
        print("INICIANDO TASK: CUSTOMERS")
        print("="*80)
        run_customers_task(step=None, limit_rows=limit)
        
        # Task 2: Stores
        print("\n" + "="*80)
        print("INICIANDO TASK: STORES")
        print("="*80)
        run_stores_task(step=None, limit_rows=limit)
        
    elif task == 'customers':
        run_customers_task(step=step, limit_rows=limit)
    elif task == 'stores':
        run_stores_task(step=step, limit_rows=limit)
    else:
        print(f"ERRO: Task '{task}' invalida")
        print("Tasks disponiveis: customers, stores")
        print("\nUso:")
        print("  python orchestrator_tasks.py                    # Executa todas as tasks")
        print("  python orchestrator_tasks.py --limit 100         # Executa todas com limite")
        print("  python orchestrator_tasks.py customers           # Executa apenas customers")
        print("  python orchestrator_tasks.py stores              # Executa apenas stores")
        print("  python orchestrator_tasks.py customers 1          # Executa etapa 1 de customers")
        print("  python orchestrator_tasks.py --limit 100 --destino PRD  # Todas as tasks com limite e destino")
        return
    
    # Mostrar destino usado
    destino_usado = DatabaseConnection.get_destino()
    print(f"\nDestino utilizado: {destino_usado}")
    
    print("\n" + "="*80)
    print("ORCHESTRATOR CONCLUIDO")
    print("="*80)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nERRO CRITICO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

