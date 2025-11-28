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
DESTINO_PADRAO = 'HML'

# Adicionar diretorios ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'customers'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'utils'))


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
    destino = DatabaseConnection.get_destino()
    
    print("\n" + "="*80)
    print("TASK 2: STORES")
    print("="*80)
    print(f"Data/Hora: {datetime.now()}")
    print(f"Destino: {destino}")
    print(f"Limite de linhas: {'TODOS' if limit_rows == 0 else limit_rows}")
    if step:
        print(f"Etapa: {step}")
    print("="*80)
    
    # TODO: Implementar quando stores_to_core.py estiver pronto
    print("AVISO: Task de stores ainda nao implementada")
    print("Aguardando criacao do arquivo stores/stores_to_core.py")


def main():
    """Funcao principal do orchestrator"""
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
    # Formato: python orchestrator_tasks.py <task> [step] [--limit N] [--destino HML|PRD]
    # Exemplos:
    #   python orchestrator_tasks.py customers
    #   python orchestrator_tasks.py customers 1
    #   python orchestrator_tasks.py customers 1 --limit 1000
    #   python orchestrator_tasks.py customers --destino PRD
    #   python orchestrator_tasks.py stores 2 --limit 500 --destino HML
    
    if len(sys.argv) < 2:
        print("\nUso:")
        print("  python orchestrator_tasks.py <task> [step] [--limit N] [--destino HML|PRD]")
        print("\nTasks disponiveis:")
        print("  customers - Migracao de customers")
        print("  stores    - Migracao de stores")
        print("\nEtapas (opcional):")
        print("  customers: 1=customer_segments, 2=customers, 3=addresses, 4=contacts")
        print("  stores: (a definir)")
        print("\nOpcoes:")
        print("  --limit N       - Limitar quantidade de linhas (0 = todos)")
        print("  --destino HML|PRD - Escolher destino da migracao (padrao: HML)")
        print("\nExemplos:")
        print("  python orchestrator_tasks.py customers")
        print("  python orchestrator_tasks.py customers 1")
        print("  python orchestrator_tasks.py customers 1 --limit 1000")
        print("  python orchestrator_tasks.py customers --destino PRD")
        print("  python orchestrator_tasks.py stores 2 --limit 500 --destino HML")
        return
    
    task = sys.argv[1].lower()
    step = None
    limit = 0
    
    # Processar argumentos
    i = 2
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--limit' and i + 1 < len(sys.argv):
            limit = int(sys.argv[i + 1])
            i += 2
        elif arg == '--destino' and i + 1 < len(sys.argv):
            destino_arg = sys.argv[i + 1].upper()
            if destino_arg in ['HML', 'PRD']:
                destino = destino_arg
            else:
                print(f"AVISO: Destino invalido '{sys.argv[i + 1]}'. Usando padrao: {destino}")
            i += 2
        elif arg.isdigit():
            step = arg
            i += 1
        else:
            i += 1
    
    # Usar limite global se nao especificado
    if limit == 0:
        limit = limit_rows
    
    # Configurar destino
    from utils.database_connection import DatabaseConnection
    DatabaseConnection.set_destino(destino)
    print(f"\nDestino configurado: {destino}")
    print("="*80)
    
    # Executar task
    if task == 'customers':
        run_customers_task(step=step, limit_rows=limit)
    elif task == 'stores':
        run_stores_task(step=step, limit_rows=limit)
    else:
        print(f"ERRO: Task '{task}' invalida")
        print("Tasks disponiveis: customers, stores")
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

