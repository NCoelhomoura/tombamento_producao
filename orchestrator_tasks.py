"""
Orchestrator para gerenciar as tasks de migracao de dados.

Escopo agregado (JSON) e alinhamento com --id-orcamento / --destino / filtros:
  docs/migracao/FILTRO_ESCOPO_ORQUESTRACAO.md
Arquivo central: contracts/contracts_filter_main.json (aggregated_ids).

--xlsx-filter: lê contratos_ativos.xlsx (ativo + migrar=true), atualiza o JSON e define
  id_orcamento_filter como em --id-orcamento; demais flags (datas, --clear-data, etc.) seguem iguais.

Billings: após contracts step1, ContractsMigration.ensure_billings_after_step1()
(migração em billing/billings_to_core; fallback placeholder). Usado também nos
steps isolados contracts 2 e 8 deste orquestrador.
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
DESTINO_PADRAO = 'PRD'  # Ambiente padrão: PRD

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
                      data_aviso_previo_min=None, data_inicio_operacao_max=None, 
                      status_pedido_filter=None, clear_data=False):
    """
    Executa a task de migracao de customers
    
    Args:
        step: None para todas as etapas, ou '1', '2', '3', '4' para etapa especifica
        limit_rows: 0 para todos os dados, > 0 para limitar quantidade
        id_orcamento_filter: Lista opcional de IdOrcamento para filtrar
        data_aviso_previo_min: Data mínima para DataAvisoPrevio (string 'YYYY-MM-DD')
        data_inicio_operacao_max: Data máxima para DataInicioOperacao (string 'YYYY-MM-DD')
        status_pedido_filter: Lista opcional de StatusPedido para filtrar (ex: [6, 7, 8])
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
        status_pedido_filter=status_pedido_filter,
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
            # Carregar customers primeiro (core.customers usa legacy_ids jsonb, não legacy_id)
            from utils.database_connection import DatabaseConnection
            from customers.customers_to_core import get_schema_atual
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT id, legacy_ids FROM {schema}.customers")
            for row in cursor_pg.fetchall():
                uuid_row, leg_ids = row[0], row[1]
                if leg_ids:
                    for leg_id in leg_ids:
                        migration.customer_id_map[leg_id] = uuid_row
            cursor_pg.close()
            conn_pg.close()
            migration.step3_migrate_addresses()
        elif step == '4':
            # Carregar customers primeiro (legacy_ids jsonb)
            from utils.database_connection import DatabaseConnection
            from customers.customers_to_core import get_schema_atual
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT id, legacy_ids FROM {schema}.customers")
            for row in cursor_pg.fetchall():
                uuid_row, leg_ids = row[0], row[1]
                if leg_ids:
                    for leg_id in leg_ids:
                        migration.customer_id_map[leg_id] = uuid_row
            cursor_pg.close()
            conn_pg.close()
            migration.step4_migrate_contacts()
        elif step == '5':
            # Carregar customer_segments primeiro
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
            migration.step5_migrate_customer_brands()
        elif step == '6':
            # Carregar customers e customer_brands primeiro (legacy_ids jsonb)
            from utils.database_connection import DatabaseConnection
            from customers.customers_to_core import get_schema_atual
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT id, legacy_ids FROM {schema}.customers")
            for row in cursor_pg.fetchall():
                uuid_row, leg_ids = row[0], row[1]
                if leg_ids:
                    for leg_id in leg_ids:
                        migration.customer_id_map[leg_id] = uuid_row
            cursor_pg.execute(f"SELECT id, name FROM {schema}.customer_brands")
            for row in cursor_pg.fetchall():
                migration.customer_brands_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            migration.step6_migrate_customer_customer_brand()
        else:
            print(f"ERRO: Etapa {step} invalida. Use 1, 2, 3, 4, 5 ou 6")
            return
    else:
        # Executar todas as etapas
        migration.run()


def run_stores_task(step=None, limit_rows=0, id_orcamento_filter=None,
                    data_aviso_previo_min=None, data_inicio_operacao_max=None, 
                    status_pedido_filter=None, clear_data=False):
    """
    Executa a task de migracao de stores
    
    Args:
        step: None para todas as etapas, ou '1', '2', '3', etc. para etapa especifica
        limit_rows: 0 para todos os dados, > 0 para limitar quantidade
        id_orcamento_filter: Lista de IdOrcamento para filtrar (ex: [6192, 6193])
        data_aviso_previo_min: Data mínima para DataAvisoPrevio (string 'YYYY-MM-DD')
        data_inicio_operacao_max: Data máxima para DataInicioOperacao (string 'YYYY-MM-DD')
        status_pedido_filter: Lista opcional de StatusPedido para filtrar (ex: [6, 7, 8])
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
        status_pedido_filter=status_pedido_filter,
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
                       data_aviso_previo_min=None, data_inicio_operacao_max=None, 
                       status_pedido_filter=None, clear_data=False):
    """
    Executa a task de migracao de contracts
    
    Args:
        step: None para todas as etapas, ou '1'–'10' (10 = contract_scenarios_brands no destino)
        limit_rows: 0 para todos os dados, > 0 para limitar quantidade
        id_orcamento_filter: Lista de IdOrcamento para filtrar (ex: [6192, 6193])
        data_aviso_previo_min: Data mínima para DataAvisoPrevio (string 'YYYY-MM-DD')
        data_inicio_operacao_max: Data máxima para DataInicioOperacao (string 'YYYY-MM-DD')
        status_pedido_filter: Lista opcional de StatusPedido para filtrar (ex: [6, 7, 8])
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
    if status_pedido_filter:
        print(f"Filtro StatusPedido: {status_pedido_filter}")
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
        status_pedido_filter=status_pedido_filter,
        clear_data=clear_data
    )
    
    if step:
        # Executar apenas uma etapa específica
        if step == '1':
            migration.step1_migrate_contracts()
        elif step == '2':
            # Garantir step1 (contracts) antes de billings (migração real ou placeholder)
            filter_data = migration.load_filter_json()
            if not filter_data or 'aggregated_ids' not in filter_data:
                print("[ETAPA 2] Step1 não executado. Executando step1 primeiro...")
                migration.step1_migrate_contracts()
            migration.ensure_billings_after_step1()
            # Verificar se step9 foi executado (promoter_tasks deve estar migrado)
            if not migration.promoter_task_map:
                print("[ETAPA 2] Step9 (promoter_tasks) não executado. Executando step9 primeiro...")
                migration.step9_migrate_promoter_tasks()
            # Carregar mapeamento de contracts
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            if migration.should_include_legacy_id():
                cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.contracts WHERE legacy_id IS NOT NULL")
            else:
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
            # Garantir step1 (contracts) antes de billings (migração real ou placeholder)
            filter_data = migration.load_filter_json()
            if not filter_data or 'aggregated_ids' not in filter_data:
                print("[ETAPA 8] Step1 não executado. Executando step1 primeiro...")
                migration.step1_migrate_contracts()
            migration.ensure_billings_after_step1()
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
        elif step == '9':
            # Executar step9 (promoter_tasks) - requer step1 executado antes
            # Carregar filtros do step1 se necessário
            filter_data = migration.load_filter_json()
            if not filter_data or 'aggregated_ids' not in filter_data:
                print("[ETAPA 9] AVISO: Step1 deve ser executado antes do step9")
                print("[ETAPA 9] Executando step1 primeiro...")
                migration.step1_migrate_contracts()
            migration.step9_migrate_promoter_tasks()
        elif step == '10':
            migration.step10_migrate_contract_scenarios_brands()
        else:
            print(f"ERRO: Etapa '{step}' invalida para contracts")
            print("Etapas disponiveis: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10")
            return
    else:
        # Executar migração completa
        migration.run()


def run_users_task(step=None, limit_rows=0, clear_users: bool = False):
    """
    Executa a task de migracao de users
    
    Args:
        step: None para todas as etapas, '1' para step1_migrate_users, ou '2' para step2_migrate_user_roles
        limit_rows: 0 para todos os dados, > 0 para limitar quantidade
        clear_users: Se True, TRUNCATE em users + Sys Admin antes da carga (use --clear-users).
                     Se False (padrao), apenas INSERT de usuarios com legacy_id ainda inexistente.
    """
    print("\n" + "="*80)
    print("TASK 4: USERS")
    print("="*80)
    print(f"Data/Hora: {datetime.now()}")
    print(f"Limite de linhas: {'TODOS' if limit_rows == 0 else limit_rows}")
    print(f"--clear-users (TRUNCATE + Sys Admin): {'SIM' if clear_users else 'NAO (incremental)'}")
    if step:
        print(f"Etapa: {step}")
    print("="*80)
    
    # ⚠️ CRÍTICO: Garantir que o destino está configurado antes de importar o módulo
    from utils.database_connection import DatabaseConnection
    destino_atual = DatabaseConnection.get_destino()
    print(f"[DEBUG run_users_task] Destino antes de importar UsersMigration: {destino_atual}")
    
    # Adicionar diretório users ao path
    users_path = os.path.join(os.path.dirname(__file__), 'users')
    if users_path not in sys.path:
        sys.path.insert(0, users_path)
    from users_to_core import UsersMigration
    
    # ⚠️ CRÍTICO: Verificar novamente após importar
    destino_apos_import = DatabaseConnection.get_destino()
    print(f"[DEBUG run_users_task] Destino após importar UsersMigration: {destino_apos_import}")
    
    # ⚠️ CRÍTICO: Garantir que o destino está correto antes de criar a instância
    # Se o destino não está correto, reconfigurar
    if destino_apos_import != destino_atual:
        print(f"[DEBUG run_users_task] Destino mudou após importar. Reconfigurando para: {destino_atual}")
        DatabaseConnection.set_destino(destino_atual)
        destino_apos_import = DatabaseConnection.get_destino()
        print(f"[DEBUG run_users_task] Destino após reconfiguração: {destino_apos_import}")
    
    migration = UsersMigration(limit_rows=limit_rows, clear_users=clear_users)
    
    # ⚠️ CRÍTICO: Passar destino explicitamente como parâmetro
    # Isso garante que o destino correto seja usado, independente do estado global
    print(f"[DEBUG run_users_task] Passando destino explicitamente: {destino_atual}")
    
    if step:
        # Executar apenas etapa específica
        if step == '1':
            migration.step1_migrate_users(destino=destino_atual)
        elif step == '2':
            migration.step2_migrate_user_roles(destino=destino_atual)
        else:
            print(f"ERRO: Etapa '{step}' invalida para users")
            print("Etapas disponiveis: 1, 2")
            return
    else:
        # Executar todas as etapas em sequência
        migration.step1_migrate_users(destino=destino_atual)
        migration.step2_migrate_user_roles(destino=destino_atual)


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
    clear_users = False
    xlsx_filter = False
    id_orcamento_filter = None
    data_aviso_previo_min = None
    data_inicio_operacao_max = None
    status_pedido_filter = None
    
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
        elif arg == '--clear-users':
            clear_users = True
            i += 1
        elif arg == '--xlsx-filter':
            xlsx_filter = True
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
        elif arg == '--status-pedido' and i + 1 < len(sys.argv):
            # Aceitar lista separada por vírgula: --status-pedido 6,7,8
            status_pedido_str = sys.argv[i + 1]
            status_pedido_filter = [int(x.strip()) for x in status_pedido_str.split(',')]
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
    
    if xlsx_filter:
        if id_orcamento_filter:
            print("\n[XLSX-FILTER] AVISO: --id-orcamento na linha de comando sera ignorado; escopo = XLSX (ativo + migrar=true).")
        import importlib.util
        _orch_root = os.path.dirname(os.path.abspath(__file__))
        _sync_path = os.path.join(_orch_root, "contracts", "sync_filter_json_from_xlsx.py")
        if not os.path.isfile(_sync_path):
            print(f"\n[ERRO] --xlsx-filter: script nao encontrado: {_sync_path}")
            sys.exit(2)
        _spec = importlib.util.spec_from_file_location("sync_filter_json_from_xlsx", _sync_path)
        _sync_mod = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sync_mod)
        xlsx_path = os.path.abspath(_sync_mod._default_xlsx_path())
        json_path = os.path.abspath(_sync_mod.DEFAULT_FILTER_JSON)
        try:
            ids_xlsx = _sync_mod.collect_id_orcamentos_migrar(xlsx_path, require_ativo=True)
        except Exception as e:
            print(f"\n[ERRO] --xlsx-filter: falha ao ler XLSX: {e}")
            sys.exit(2)
        if not ids_xlsx:
            print(
                "\n[ERRO] --xlsx-filter: nenhum IdOrcamento com migrar=true e ativo no XLSX. "
                "Marque migrar no Excel ou use o script contracts/sync_filter_json_from_xlsx.py com --no-require-ativo."
            )
            sys.exit(2)
        fa_updates = {
            "limit_rows": int(limit),
            "clear_data": bool(clear_data),
        }
        if data_aviso_previo_min is not None:
            fa_updates["data_aviso_previo_min"] = str(data_aviso_previo_min)
        if data_inicio_operacao_max is not None:
            fa_updates["data_inicio_operacao_max"] = str(data_inicio_operacao_max)
        if status_pedido_filter:
            fa_updates["status_pedido"] = list(status_pedido_filter)
        try:
            _sync_mod.merge_into_filter_json(
                json_path,
                ids_xlsx,
                xlsx_path,
                require_ativo=True,
                filters_applied_updates=fa_updates,
                reset_dependent_aggregated_ids=True,
                sync_source_label="orchestrator_tasks.py --xlsx-filter",
            )
        except Exception as e:
            print(f"\n[ERRO] --xlsx-filter: falha ao gravar JSON: {e}")
            sys.exit(2)
        id_orcamento_filter = list(ids_xlsx)
        print("\n" + "="*80)
        print("[XLSX-FILTER] Escopo aplicado")
        print("="*80)
        print(f"  XLSX: {xlsx_path}")
        print(f"  JSON: {json_path}")
        print(f"  IdOrcamento (ativo + migrar=true): {len(id_orcamento_filter)} -> {id_orcamento_filter[:20]}{'...' if len(id_orcamento_filter) > 20 else ''}")
        print(f"  filters_applied (CLI): limit_rows={limit}, clear_data={clear_data}, datas/status conforme argumentos")
        print("="*80 + "\n")
    
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
        if 'customers_to_core' in module_name or 'stores_to_core' in module_name or 'users_to_core' in module_name:
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
        run_customers_task(step=None, limit_rows=limit,
                          id_orcamento_filter=id_orcamento_filter,
                          data_aviso_previo_min=data_aviso_previo_min,
                          data_inicio_operacao_max=data_inicio_operacao_max,
                          status_pedido_filter=status_pedido_filter,
                          clear_data=clear_data)
        
        # Task 2: Stores
        print("\n" + "="*80)
        print("INICIANDO TASK: STORES")
        print("="*80)
        run_stores_task(step=None, limit_rows=limit,
                        id_orcamento_filter=id_orcamento_filter,
                        data_aviso_previo_min=data_aviso_previo_min,
                        data_inicio_operacao_max=data_inicio_operacao_max,
                        status_pedido_filter=status_pedido_filter,
                        clear_data=clear_data)
        
        # Task 4: Users (DEVE SER EXECUTADO ANTES DE CONTRACTS)
        print("\n" + "="*80)
        print("INICIANDO TASK: USERS")
        print("="*80)
        print("[INFO] Users deve ser executado antes de Contracts (dependencia)")
        print("[INFO] Padrao: INSERT incremental (sem TRUNCATE). Para TRUNCATE + Sys Admin: --clear-users")
        print("[INFO] Sempre limit_rows=0 para incluir todos os usuarios da origem na verificacao incremental")
        run_users_task(step=None, limit_rows=0, clear_users=clear_users)
        
        # Task 3: Contracts (DEPENDE DE USERS)
        print("\n" + "="*80)
        print("INICIANDO TASK: CONTRACTS")
        print("="*80)
        print("[INFO] Contracts depende de Users (ja executado)")
        run_contracts_task(step=step, limit_rows=limit, 
                          id_orcamento_filter=id_orcamento_filter,
                          data_aviso_previo_min=data_aviso_previo_min,
                          data_inicio_operacao_max=data_inicio_operacao_max,
                          status_pedido_filter=status_pedido_filter,
                          clear_data=clear_data)
        
    elif task == 'customers':
        run_customers_task(step=step, limit_rows=limit, 
                          id_orcamento_filter=id_orcamento_filter,
                          data_aviso_previo_min=data_aviso_previo_min,
                          data_inicio_operacao_max=data_inicio_operacao_max,
                          status_pedido_filter=status_pedido_filter,
                          clear_data=clear_data)
    elif task == 'stores':
        run_stores_task(step=step, limit_rows=limit,
                        id_orcamento_filter=id_orcamento_filter,
                        data_aviso_previo_min=data_aviso_previo_min,
                        data_inicio_operacao_max=data_inicio_operacao_max,
                        status_pedido_filter=status_pedido_filter,
                        clear_data=clear_data)
    elif task == 'users':
        run_users_task(step=step, limit_rows=limit, clear_users=clear_users)
    elif task == 'contracts':
        # Executar dependências automaticamente ANTES de contracts
        print("\n" + "="*80)
        print("⚠️  VERIFICANDO E EXECUTANDO DEPENDÊNCIAS AUTOMATICAMENTE")
        print("="*80)
        print("[INFO] Contracts requer Customers e Stores antes da execução")
        print("[INFO] Executando dependências automaticamente com os mesmos filtros...")
        print("="*80)
        
        # 1. Executar Customers steps 1, 2, 3, 4, 5 e 6 (se necessário)
        #     Steps 3 (addresses) e 4 (contacts) são obrigatórios para popular gmcore/core.addresses e contacts.
        print("\n[1/2] Executando Customers (dependência de Contracts)...")
        try:
            # Step 1 de customers (customer_segments) - necessário antes do step 2
            print("  → Executando Customers step 1 (customer_segments)...")
            run_customers_task(step='1', limit_rows=limit, id_orcamento_filter=id_orcamento_filter, 
                              status_pedido_filter=status_pedido_filter,
                              clear_data=clear_data)
            print("  [OK] Customers step 1 concluído")
            
            # Step 2 de customers (customers)
            print("  → Executando Customers step 2 (customers)...")
            run_customers_task(step='2', limit_rows=limit, id_orcamento_filter=id_orcamento_filter,
                              data_aviso_previo_min=data_aviso_previo_min,
                              data_inicio_operacao_max=data_inicio_operacao_max,
                              status_pedido_filter=status_pedido_filter,
                              clear_data=clear_data)
            print("  [OK] Customers step 2 concluído")
            
            # Step 3 de customers (addresses na tabela polimórfica addresses)
            print("  → Executando Customers step 3 (addresses)...")
            run_customers_task(step='3', limit_rows=limit, id_orcamento_filter=id_orcamento_filter,
                              data_aviso_previo_min=data_aviso_previo_min,
                              data_inicio_operacao_max=data_inicio_operacao_max,
                              status_pedido_filter=status_pedido_filter,
                              clear_data=clear_data)
            print("  [OK] Customers step 3 concluído")
            
            # Step 4 de customers (contacts)
            print("  → Executando Customers step 4 (contacts)...")
            run_customers_task(step='4', limit_rows=limit, id_orcamento_filter=id_orcamento_filter,
                              data_aviso_previo_min=data_aviso_previo_min,
                              data_inicio_operacao_max=data_inicio_operacao_max,
                              status_pedido_filter=status_pedido_filter,
                              clear_data=clear_data)
            print("  [OK] Customers step 4 concluído")
            
            # Step 5 de customers (customer_brands) - necessário porque step2 com CASCADE limpa essa tabela
            print("  → Executando Customers step 5 (customer_brands)...")
            run_customers_task(step='5', limit_rows=limit, id_orcamento_filter=id_orcamento_filter,
                              data_aviso_previo_min=data_aviso_previo_min,
                              data_inicio_operacao_max=data_inicio_operacao_max,
                              status_pedido_filter=status_pedido_filter,
                              clear_data=clear_data)
            print("  [OK] Customers step 5 concluído")
            
            # Step 6 de customers (customer_customer_brand) - necessário porque step2 com CASCADE limpa essa tabela
            print("  → Executando Customers step 6 (customer_customer_brand)...")
            run_customers_task(step='6', limit_rows=limit, id_orcamento_filter=id_orcamento_filter,
                              data_aviso_previo_min=data_aviso_previo_min,
                              data_inicio_operacao_max=data_inicio_operacao_max,
                              status_pedido_filter=status_pedido_filter,
                              clear_data=clear_data)
            print("  [OK] Customers step 6 concluído")
            
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
                            status_pedido_filter=status_pedido_filter,
                            clear_data=clear_data)
            print("  [OK] Stores step 1 concluído")
            
            # Step 2 de stores (retail_chains)
            print("  → Executando Stores step 2 (retail_chains)...")
            run_stores_task(step='2', limit_rows=limit, id_orcamento_filter=id_orcamento_filter,
                            data_aviso_previo_min=data_aviso_previo_min,
                            data_inicio_operacao_max=data_inicio_operacao_max,
                            status_pedido_filter=status_pedido_filter,
                            clear_data=clear_data)
            print("  [OK] Stores step 2 concluído")
            
            # Step 3 de stores (store_brands)
            print("  → Executando Stores step 3 (store_brands)...")
            run_stores_task(step='3', limit_rows=limit, id_orcamento_filter=id_orcamento_filter,
                            data_aviso_previo_min=data_aviso_previo_min,
                            data_inicio_operacao_max=data_inicio_operacao_max,
                            status_pedido_filter=status_pedido_filter,
                            clear_data=clear_data)
            print("  [OK] Stores step 3 concluído")
            
            # Step 4 de stores (stores)
            print("  → Executando Stores step 4 (stores)...")
            run_stores_task(step='4', limit_rows=limit, id_orcamento_filter=id_orcamento_filter,
                            data_aviso_previo_min=data_aviso_previo_min,
                            data_inicio_operacao_max=data_inicio_operacao_max,
                            status_pedido_filter=status_pedido_filter,
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
            print("[INFO] Modo incremental: insere todos os usuarios da origem que ainda nao existem (por legacy_id).")
            print("[INFO] Para TRUNCATE completo + Sys Admin na primeira carga, use --clear-users ao rodar o orquestrador.")
            print("\n" + "="*80)
            print("EXECUTANDO TASK: USERS (DEPENDENCIA DE CONTRACTS)")
            print("="*80)
            run_users_task(step=None, limit_rows=0, clear_users=clear_users)
            print("\n[INFO] Users executado (FULL). Continuando com Contracts...")
        
        run_contracts_task(step=step, limit_rows=limit,
                          id_orcamento_filter=id_orcamento_filter,
                          data_aviso_previo_min=data_aviso_previo_min,
                          data_inicio_operacao_max=data_inicio_operacao_max,
                          status_pedido_filter=status_pedido_filter,
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
        print("  python orchestrator_tasks.py users 1              # Executa etapa 1 de users (migrate_users)")
        print("  python orchestrator_tasks.py users 2              # Executa etapa 2 de users (migrate_user_roles)")
        print("  python orchestrator_tasks.py users 1 --clear-users  # TRUNCATE users + Sys Admin, depois carga")
        print("  python orchestrator_tasks.py --clear-users         # Todas as tasks com TRUNCATE em users no passo users")
        print("  python orchestrator_tasks.py ... --xlsx-filter     # IdOrcamento do XLSX (ativo+migrar) -> JSON + mesmo efeito que --id-orcamento")
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

