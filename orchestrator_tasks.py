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
DESTINO_PADRAO = 'HML'  # Ambiente padrão: PRD

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
# ⚠️ IMPORTANTE: Não importar módulos de migração aqui - serão importados depois de configurar destino
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'customers'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'users'))


def run_customers_task(step=None, limit_rows=0, id_orcamento_filter=None,
                      data_aviso_previo_min=None, data_inicio_operacao_max=None, clear_data=False):
    """
    Executa a task de migracao de customers
    
    Args:
        step: None para todas as etapas, ou '1', '2', '3', '4' para etapa especifica
        limit_rows: 0 para todos os dados, > 0 para limitar quantidade
        id_orcamento_filter: Lista opcional de IdOrcamento para filtrar
        data_aviso_previo_min: Data mínima para DataAvisoPrevio (string 'YYYY-MM-DD')
        data_inicio_operacao_max: Data máxima para DataInicioOperacao (string 'YYYY-MM-DD')
        clear_data: Se True, força TRUNCATE mesmo com filtros aplicados
    """
    print("\n" + "="*80)
    print("TASK 1: CUSTOMERS")
    print("="*80)
    print(f"Data/Hora: {datetime.now()}")
    print(f"Limite de linhas: {'TODOS' if limit_rows == 0 else limit_rows}")
    if id_orcamento_filter:
        print(f"Filtro IdOrcamento: {id_orcamento_filter}")
    if step:
        print(f"Etapa: {step}")
    print("="*80)
    
    # ⚠️ CRÍTICO: Garantir que o destino está configurado antes de importar o módulo
    from utils.database_connection import DatabaseConnection
    destino_atual = DatabaseConnection.get_destino()
    print(f"[DEBUG run_customers_task] Destino antes de importar CustomersMigration: {destino_atual}")
    
    from customers.customers_to_core import CustomersMigration
    
    # ⚠️ CRÍTICO: Verificar novamente após importar
    destino_apos_import = DatabaseConnection.get_destino()
    print(f"[DEBUG run_customers_task] Destino após importar CustomersMigration: {destino_apos_import}")
    
    migration = CustomersMigration(
        limit_rows=limit_rows, 
        id_orcamento_filter=id_orcamento_filter,
        data_aviso_previo_min=data_aviso_previo_min,
        data_inicio_operacao_max=data_inicio_operacao_max,
        clear_data=clear_data
    )
    
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


def run_stores_task(step=None, limit_rows=0, id_orcamento_filter=None,
                    data_aviso_previo_min=None, data_inicio_operacao_max=None, clear_data=False):
    """
    Executa a task de migracao de stores
    
    Args:
        step: None para todas as etapas, ou '1', '2', '3', etc. para etapa especifica
        limit_rows: 0 para todos os dados, > 0 para limitar quantidade
        id_orcamento_filter: Lista de IdOrcamento para filtrar (ex: [6192, 6193])
        data_aviso_previo_min: Data mínima para DataAvisoPrevio (string 'YYYY-MM-DD')
        data_inicio_operacao_max: Data máxima para DataInicioOperacao (string 'YYYY-MM-DD')
        clear_data: Se True, força TRUNCATE mesmo com filtros aplicados
    """
    from utils.database_connection import DatabaseConnection
    destino_atual = DatabaseConnection.get_destino()
    
    print("\n" + "="*80)
    print("TASK 2: STORES")
    print("="*80)
    print(f"Data/Hora: {datetime.now()}")
    print(f"Destino: {destino_atual}")
    print(f"Limite de linhas: {'TODOS' if limit_rows == 0 else limit_rows}")
    if id_orcamento_filter:
        print(f"Filtro IdOrcamento: {id_orcamento_filter}")
    if step:
        print(f"Etapa: {step}")
    print("="*80)
    
    # Adicionar diretório stores ao path
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'stores'))
    from stores.stores_to_core import StoresMigration
    
    migration = StoresMigration(
        limit_rows=limit_rows, 
        id_orcamento_filter=id_orcamento_filter,
        data_aviso_previo_min=data_aviso_previo_min,
        data_inicio_operacao_max=data_inicio_operacao_max,
        clear_data=clear_data
    )
    
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


def run_contracts_task(step=None, limit_rows=0, id_orcamento_filter=None, 
                       data_aviso_previo_min=None, data_inicio_operacao_max=None, clear_data=False):
    """
    Executa a task de migracao de contracts
    
    Args:
        step: None para todas as etapas, ou '1', '2', '3', etc. para etapa especifica
        limit_rows: 0 para todos os dados, > 0 para limitar quantidade
        id_orcamento_filter: Lista de IdOrcamento para filtrar (ex: [6192, 6193])
        data_aviso_previo_min: Data mínima para DataAvisoPrevio (string 'YYYY-MM-DD')
        data_inicio_operacao_max: Data máxima para DataInicioOperacao (string 'YYYY-MM-DD')
        clear_data: Se True, força TRUNCATE mesmo com filtros aplicados
    """
    from utils.database_connection import DatabaseConnection
    destino_atual = DatabaseConnection.get_destino()
    
    print("\n" + "="*80)
    print("TASK 3: CONTRACTS")
    print("="*80)
    print(f"Data/Hora: {datetime.now()}")
    print(f"Destino: {destino_atual}")
    print(f"Limite de linhas: {'TODOS' if limit_rows == 0 else limit_rows}")
    if id_orcamento_filter:
        print(f"Filtro IdOrcamento: {id_orcamento_filter}")
    if data_aviso_previo_min:
        print(f"Filtro DataAvisoPrevio (min): {data_aviso_previo_min}")
    if data_inicio_operacao_max:
        print(f"Filtro DataInicioOperacao (max): {data_inicio_operacao_max}")
    if clear_data:
        print("⚠️ FLAG --clear-data ATIVO: TRUNCATE será usado mesmo com filtros")
    if step:
        print(f"Etapa: {step}")
    print("="*80)
    
    # Adicionar diretório contracts ao path
    contracts_path = os.path.join(os.path.dirname(__file__), 'contracts')
    if contracts_path not in sys.path:
        sys.path.insert(0, contracts_path)
    from contracts_to_core import ContractsMigration, get_schema_atual
    
    migration = ContractsMigration(
        limit_rows=limit_rows,
        id_orcamento_filter=id_orcamento_filter,
        data_aviso_previo_min=data_aviso_previo_min,
        data_inicio_operacao_max=data_inicio_operacao_max,
        clear_data=clear_data
    )
    
    if step:
        # Executar apenas uma etapa específica
        if step == '1':
            migration.step1_migrate_contracts()
        elif step == '2':
            # Carregar mapeamento de contracts
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            if migration.should_include_legacy_id():
                cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.contracts WHERE legacy_id IS NOT NULL")
            else:
                # Em PRD, não há legacy_id, então precisamos de outra estratégia
                # Por enquanto, vamos assumir que os dados já existem e precisamos buscar de outra forma
                cursor_pg.execute(f"SELECT id FROM {schema}.contracts LIMIT 1")
            for row in cursor_pg.fetchall():
                if len(row) == 2 and row[1] is not None:
                    migration.contract_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            migration.step2_migrate_contract_scenarios()
        elif step == '3':
            # Carregar mapeamentos de contracts e scenarios
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            if migration.should_include_legacy_id():
                cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.contracts WHERE legacy_id IS NOT NULL")
                for row in cursor_pg.fetchall():
                    if row[1] is not None:
                        migration.contract_id_map[row[1]] = row[0]
                cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.contract_scenarios WHERE legacy_id IS NOT NULL")
                for row in cursor_pg.fetchall():
                    if row[1] is not None:
                        migration.scenario_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            migration.step3_migrate_contract_scenario_stores()
        elif step == '4':
            # Carregar mapeamento de contracts
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            if migration.should_include_legacy_id():
                cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.contracts WHERE legacy_id IS NOT NULL")
                for row in cursor_pg.fetchall():
                    if row[1] is not None:
                        migration.contract_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            migration.step4_migrate_contract_sellers()
        elif step == '5':
            # Carregar mapeamento de contracts
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            if migration.should_include_legacy_id():
                cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.contracts WHERE legacy_id IS NOT NULL")
                for row in cursor_pg.fetchall():
                    if row[1] is not None:
                        migration.contract_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            migration.step5_migrate_contract_team_members()
        elif step == '6':
            # Carregar mapeamento de contracts
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            if migration.should_include_legacy_id():
                cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.contracts WHERE legacy_id IS NOT NULL")
                for row in cursor_pg.fetchall():
                    if row[1] is not None:
                        migration.contract_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            migration.step6_migrate_contract_contacts()
        elif step == '7':
            # Carregar mapeamento de contracts
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            if migration.should_include_legacy_id():
                cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.contracts WHERE legacy_id IS NOT NULL")
                for row in cursor_pg.fetchall():
                    if row[1] is not None:
                        migration.contract_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            migration.step7_migrate_contract_partners()
        elif step == '8':
            # Carregar mapeamento de contracts
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            if migration.should_include_legacy_id():
                cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.contracts WHERE legacy_id IS NOT NULL")
                for row in cursor_pg.fetchall():
                    if row[1] is not None:
                        migration.contract_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            migration.step8_migrate_contract_additional_charges()
        else:
            print(f"ERRO: Etapa '{step}' invalida para contracts")
            print("Etapas disponiveis: 1, 2, 3, 4, 5, 6, 7, 8")
            return
    else:
        # Executar migração completa
        migration.run()


def run_users_task(step=None, limit_rows=0):
    """
    Executa a task de migracao de users
    
    Args:
        step: None para todas as etapas, ou '1' para etapa especifica
        limit_rows: 0 para todos os dados, > 0 para limitar quantidade
    """
    print("\n" + "="*80)
    print("TASK 4: USERS")
    print("="*80)
    print(f"Data/Hora: {datetime.now()}")
    print(f"Limite de linhas: {'TODOS' if limit_rows == 0 else limit_rows}")
    if step:
        print(f"Etapa: {step}")
    print("="*80)
    
    # Adicionar diretório users ao path
    users_path = os.path.join(os.path.dirname(__file__), 'users')
    if users_path not in sys.path:
        sys.path.insert(0, users_path)
    from users_to_core import UsersMigration
    
    migration = UsersMigration(limit_rows=limit_rows)
    
    if step:
        # Executar apenas etapa específica
        if step == '1':
            migration.step1_migrate_users()
        else:
            print(f"ERRO: Etapa '{step}' invalida para users")
            print("Etapas disponiveis: 1")
            return
    else:
        # Executar todas as etapas em sequência
        migration.step1_migrate_users()


def main():
    """Funcao principal do orchestrator"""
    # ⚠️ CRÍTICO: Importar DatabaseConnection primeiro (sem configurar ainda)
    from utils.database_connection import DatabaseConnection
    
    # Ler configuracao de limite e destino padrão
    global LIMIT_ROWS, DESTINO_PADRAO
    limit_rows = LIMIT_ROWS
    destino = DESTINO_PADRAO  # Valor padrão inicial
    
    # ============================================================================
    # ETAPA 1: PROCESSAR TODOS OS ARGUMENTOS PRIMEIRO
    # ============================================================================
    # Processar argumentos da linha de comando ANTES de configurar destino
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
    
    # Variáveis para filtros
    clear_data = False
    id_orcamento_filter = None
    data_aviso_previo_min = None
    data_inicio_operacao_max = None
    
    # Processar TODOS os argumentos primeiro
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--limit' and i + 1 < len(sys.argv):
            limit = int(sys.argv[i + 1])
            i += 2
        elif arg == '--destino' and i + 1 < len(sys.argv):
            # ⚠️ PRIORIDADE 1: Se --destino foi especificado, usar esse valor
            destino_arg = sys.argv[i + 1].upper()
            if destino_arg in ['HML', 'PRD']:
                destino = destino_arg  # Atualizar variável local (será configurado depois)
            else:
                print(f"AVISO: Destino invalido '{sys.argv[i + 1]}'. Usando padrao: {destino}")
            i += 2
        elif arg == '--clear-data':
            clear_data = True
            i += 1
        elif arg == '--id-orcamento' and i + 1 < len(sys.argv):
            # Aceitar lista separada por vírgula: --id-orcamento 6192,6193
            id_orcamento_str = sys.argv[i + 1]
            id_orcamento_filter = [int(x.strip()) for x in id_orcamento_str.split(',')]
            i += 2
        elif arg == '--data-aviso-previo' and i + 1 < len(sys.argv):
            data_aviso_previo_min = sys.argv[i + 1]
            i += 2
        elif arg == '--data-inicio-operacao' and i + 1 < len(sys.argv):
            data_inicio_operacao_max = sys.argv[i + 1]
            i += 2
        elif arg.lower() in ['customers', 'stores', 'contracts', 'users']:
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
    
    # ============================================================================
    # ETAPA 2: CONFIGURAR DESTINO UMA ÚNICA VEZ APÓS PROCESSAR ARGUMENTOS
    # ============================================================================
    # Lógica de prioridade:
    # 1. --destino na linha de comando (já processado acima)
    # 2. DESTINO_PADRAO do orchestrator (valor inicial de 'destino')
    # 3. Variável de ambiente MIGRATION_DESTINO (verificado dentro de get_destino())
    # 4. Valor padrão 'HML' (fallback)
    
    print(f"\n[DEBUG] Configurando destino final: {destino}")
    DatabaseConnection.set_destino(destino)
    
    # Verificar se foi configurado corretamente
    destino_verificado = DatabaseConnection.get_destino()
    if destino_verificado != destino:
        print(f"\n⚠️ AVISO: Destino verificado ({destino_verificado}) diferente do esperado ({destino})")
        print(f"[DEBUG] Reconfigurando destino para: {destino}")
        DatabaseConnection.set_destino(destino)
        destino_verificado = DatabaseConnection.get_destino()
    
    print(f"[DEBUG] Destino configurado com sucesso: {destino_verificado}")
    print(f"[DEBUG] Flag _destino_configurado_explicitamente: {DatabaseConnection._destino_configurado_explicitamente}")
    print(f"[DEBUG] Valor _destino_atual: {DatabaseConnection._destino_atual}")
    print("="*80)
    
    # Limpar arquivo de log no inicio (após configurar destino)
    clear_log_file()
    
    print("\n" + "="*80)
    print("ORCHESTRATOR DE MIGRACAO DE DADOS")
    print("="*80)
    print(f"Data/Hora: {datetime.now()}")
    print(f"Destino configurado: {destino_verificado}")
    print("="*80)
    
    # ⚠️ CRÍTICO: Verificar destino ANTES de importar módulos
    destino_antes_import = DatabaseConnection.get_destino()
    print(f"[DEBUG] Destino ANTES de importar módulos: {destino_antes_import}")
    
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
    
    # ⚠️ CRÍTICO: Verificar destino APÓS recarregar módulos
    destino_apos_reload = DatabaseConnection.get_destino()
    print(f"[DEBUG] Destino APÓS recarregar módulos: {destino_apos_reload}")
    
    if destino_apos_reload != destino_verificado:
        print(f"\n⚠️ ERRO CRÍTICO: Destino mudou após recarregar módulos!")
        print(f"  Esperado: {destino_verificado}, Atual: {destino_apos_reload}")
        print(f"  Reconfigurando destino para: {destino_verificado}")
        DatabaseConnection.set_destino(destino_verificado)
        destino_apos_reload = DatabaseConnection.get_destino()
        print(f"  Destino após reconfiguração: {destino_apos_reload}")
    
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
        run_customers_task(step=None, limit_rows=limit, clear_data=clear_data)
        
        # Task 2: Stores
        print("\n" + "="*80)
        print("INICIANDO TASK: STORES")
        print("="*80)
        run_stores_task(step=None, limit_rows=limit,
                        data_aviso_previo_min=data_aviso_previo_min,
                        data_inicio_operacao_max=data_inicio_operacao_max,
                        clear_data=clear_data)
        
        # Task 4: Users (DEVE SER EXECUTADO ANTES DE CONTRACTS)
        # ⚠️ IMPORTANTE: Users sempre executa FULL quando executado como parte de todas as tasks
        # Users não pode ser parcial porque contracts depende de todos os users
        print("\n" + "="*80)
        print("INICIANDO TASK: USERS")
        print("="*80)
        print("[INFO] Users deve ser executado antes de Contracts (dependencia)")
        print("[INFO] ⚠️ Users sempre executa FULL (sem filtros/limites) quando executado automaticamente")
        print("[INFO] Isso garante que todos os users estejam disponíveis para contracts")
        # ⚠️ IMPORTANTE: Sempre executar users com limit_rows=0 (FULL)
        # Não usar 'limit' porque users precisa de todos os registros para contracts funcionar
        run_users_task(step=None, limit_rows=0)
        
        # Task 3: Contracts (DEPENDE DE USERS)
        print("\n" + "="*80)
        print("INICIANDO TASK: CONTRACTS")
        print("="*80)
        print("[INFO] Contracts depende de Users (ja executado)")
        run_contracts_task(step=step, limit_rows=limit, 
                          id_orcamento_filter=id_orcamento_filter,
                          data_aviso_previo_min=data_aviso_previo_min,
                          data_inicio_operacao_max=data_inicio_operacao_max,
                          clear_data=clear_data)
        
    elif task == 'customers':
        run_customers_task(step=step, limit_rows=limit, 
                          id_orcamento_filter=id_orcamento_filter,
                          data_aviso_previo_min=data_aviso_previo_min,
                          data_inicio_operacao_max=data_inicio_operacao_max,
                          clear_data=clear_data)
    elif task == 'stores':
        run_stores_task(step=step, limit_rows=limit,
                        data_aviso_previo_min=data_aviso_previo_min,
                        data_inicio_operacao_max=data_inicio_operacao_max,
                        clear_data=clear_data)
    elif task == 'users':
        run_users_task(step=step, limit_rows=limit)
    elif task == 'contracts':
        # Executar dependências automaticamente ANTES de contracts
        print("\n" + "="*80)
        print("⚠️  VERIFICANDO E EXECUTANDO DEPENDÊNCIAS AUTOMATICAMENTE")
        print("="*80)
        print("[INFO] Contracts requer Customers e Stores antes da execução")
        print("[INFO] Executando dependências automaticamente com os mesmos filtros...")
        print("="*80)
        
        # 1. Executar Customers steps 1 e 2 (se necessário)
        print("\n[1/2] Executando Customers (dependência de Contracts)...")
        try:
            # Step 1 de customers (customer_segments) - necessário antes do step 2
            print("  → Executando Customers step 1 (customer_segments)...")
            run_customers_task(step='1', limit_rows=limit, id_orcamento_filter=id_orcamento_filter, clear_data=clear_data)
            print("  [OK] Customers step 1 concluído")
            
            # Step 2 de customers (customers)
            print("  → Executando Customers step 2 (customers)...")
            run_customers_task(step='2', limit_rows=limit, id_orcamento_filter=id_orcamento_filter, clear_data=clear_data)
            print("  [OK] Customers step 2 concluído")
            
            print("[OK] Customers concluído")
        except Exception as e:
            print(f"[AVISO] Erro ao executar Customers: {e}")
            print("[AVISO] Continuando mesmo assim...")
        
        # 2. Executar Stores steps 2, 3, 4 (se necessário)
        print("\n[2/2] Executando Stores steps 2, 3, 4 (dependência de Contracts)...")
        try:
            # Step 1 de stores (store_segments) - necessário antes dos outros
            print("  → Executando Stores step 1 (store_segments)...")
            run_stores_task(step='1', limit_rows=limit, id_orcamento_filter=id_orcamento_filter,
                            data_aviso_previo_min=data_aviso_previo_min,
                            data_inicio_operacao_max=data_inicio_operacao_max,
                            clear_data=clear_data)
            print("  [OK] Stores step 1 concluído")
            
            # Step 2 de stores (retail_chains)
            print("  → Executando Stores step 2 (retail_chains)...")
            run_stores_task(step='2', limit_rows=limit, id_orcamento_filter=id_orcamento_filter,
                            data_aviso_previo_min=data_aviso_previo_min,
                            data_inicio_operacao_max=data_inicio_operacao_max,
                            clear_data=clear_data)
            print("  [OK] Stores step 2 concluído")
            
            # Step 3 de stores (store_brands)
            print("  → Executando Stores step 3 (store_brands)...")
            run_stores_task(step='3', limit_rows=limit, id_orcamento_filter=id_orcamento_filter,
                            data_aviso_previo_min=data_aviso_previo_min,
                            data_inicio_operacao_max=data_inicio_operacao_max,
                            clear_data=clear_data)
            print("  [OK] Stores step 3 concluído")
            
            # Step 4 de stores (stores)
            print("  → Executando Stores step 4 (stores)...")
            run_stores_task(step='4', limit_rows=limit, id_orcamento_filter=id_orcamento_filter,
                            data_aviso_previo_min=data_aviso_previo_min,
                            data_inicio_operacao_max=data_inicio_operacao_max,
                            clear_data=clear_data)
            print("  [OK] Stores step 4 concluído")
            
            print("[OK] Todos os Stores steps concluídos")
        except Exception as e:
            print(f"[AVISO] Erro ao executar Stores: {e}")
            print("[AVISO] Continuando mesmo assim...")
        
        # 3. Verificar se users foi executado antes (dependencia)
        # ⚠️ IMPORTANTE: Users sempre deve executar FULL quando executado automaticamente
        # Users não pode ser parcial porque contracts depende de todos os users
        print("\n[INFO] Verificando dependencia: Contracts requer Users...")
        from utils.database_connection import DatabaseConnection
        schema_users = 'gmcore' if DatabaseConnection.get_destino() == 'HML' else 'core'
        conn_check = DatabaseConnection.get_postgresql_destino_connection()
        cursor_check = conn_check.cursor()
        cursor_check.execute(f"SELECT COUNT(*) FROM {schema_users}.users")
        users_count = cursor_check.fetchone()[0]
        cursor_check.close()
        conn_check.close()
        
        if users_count == 0:
            print(f"[AVISO] Tabela {schema_users}.users esta vazia!")
            print("[AVISO] Contracts depende de Users. Executando Users primeiro...")
            print("[INFO] ⚠️ Users sempre executa FULL (sem filtros/limites) quando executado automaticamente")
            print("[INFO] Isso garante que todos os users estejam disponíveis para contracts")
            print("\n" + "="*80)
            print("EXECUTANDO TASK: USERS (DEPENDENCIA DE CONTRACTS) - FULL")
            print("="*80)
            # ⚠️ IMPORTANTE: Sempre executar users com limit_rows=0 (FULL)
            # Não usar 'limit' porque users precisa de todos os registros para contracts funcionar
            run_users_task(step=None, limit_rows=0)
            print("\n[INFO] Users executado (FULL). Continuando com Contracts...")
        
        run_contracts_task(step=step, limit_rows=limit,
                          id_orcamento_filter=id_orcamento_filter,
                          data_aviso_previo_min=data_aviso_previo_min,
                          data_inicio_operacao_max=data_inicio_operacao_max,
                          clear_data=clear_data)
    else:
        print(f"ERRO: Task '{task}' invalida")
        print("Tasks disponiveis: customers, stores, contracts, users")
        print("\nUso:")
        print("  python orchestrator_tasks.py                    # Executa todas as tasks")
        print("  python orchestrator_tasks.py --limit 100         # Executa todas com limite")
        print("  python orchestrator_tasks.py customers           # Executa apenas customers")
        print("  python orchestrator_tasks.py stores              # Executa apenas stores")
        print("  python orchestrator_tasks.py contracts              # Executa apenas contracts")
        print("  python orchestrator_tasks.py users              # Executa apenas users")
        print("  python orchestrator_tasks.py customers 1          # Executa etapa 1 de customers")
        print("  python orchestrator_tasks.py contracts 1          # Executa etapa 1 de contracts")
        print("  python orchestrator_tasks.py users 1              # Executa etapa 1 de users")
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

