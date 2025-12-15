"""
Script de migração de dados: SQL Server PRD -> PostgreSQL Destino
Migra dados das tabelas: contracts, contract_scenarios, contract_scenario_stores, 
contract_sellers, contract_team_members, contract_contacts, contract_partners, 
contract_additional_charges
"""

import sys
import os
import uuid
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple

# Configurar encoding para evitar problemas no Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Adicionar diretório utils ao path
utils_path = os.path.join(os.path.dirname(__file__), '..', 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)
from database_connection import DatabaseConnection

# ============================================================================
# CONFIGURACAO DE SCHEMAS POR AMBIENTE
# ============================================================================
# Definir schema de destino conforme ambiente:
# HML: gmcommercial
# PRD: commercial
SCHEMA_HML = 'gmcommercial'
SCHEMA_PRD = 'commercial'

# Schema atual será determinado automaticamente baseado no destino configurado
def get_schema_atual():
    """Retorna o schema atual baseado no destino configurado"""
    destino = DatabaseConnection.get_destino()
    if destino == 'PRD':
        return SCHEMA_PRD
    else:
        return SCHEMA_HML

# Configurar logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Remover handlers existentes para evitar duplicacao
if logger.handlers:
    logger.handlers.clear()

# Handler para arquivo (modo 'a' para append - log é truncado apenas no orchestrator)
try:
    log_file_path = os.path.join(os.path.dirname(__file__), '..', 'log_execution.txt')
    log_file_path = os.path.abspath(log_file_path)
    file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
except Exception:
    pass

# Handler para console (terminal) - mostrar em tempo real
try:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
except Exception:
    pass

# Tamanho do chunk para processamento
CHUNK_SIZE = 1000


class ContractsMigration:
    """Classe para executar a migração de dados de contracts"""
    
    def __init__(self, limit_rows=0):
        self.stats = {
            'contracts': 0,
            'contract_scenarios': 0,
            'contract_scenario_stores': 0,
            'contract_sellers': 0,
            'contract_team_members': 0,
            'contract_contacts': 0,
            'contract_partners': 0,
            'contract_additional_charges': 0,
            'errors': []
        }
        self.contract_id_map = {}      # Map: legado_id (IdOrcamento) -> uuid
        self.customer_id_map = {}      # Map: legado_id -> uuid (carregado de customers)
        self.store_id_map = {}         # Map: legado_id -> uuid (carregado de stores)
        self.scenario_id_map = {}      # Map: legado_id (IdOrcamentoLoja) -> uuid
        self.limit_rows = limit_rows   # 0 = todos, > 0 = limitar quantidade
    
    def should_include_legacy_id(self):
        """Retorna True se deve incluir legacy_id (apenas em HML)"""
        destino = DatabaseConnection.get_destino()
        return destino == 'HML'
    
    def clean_string(self, value: Optional[str], max_length: Optional[int] = None) -> Optional[str]:
        """Limpa e trunca string"""
        if value is None:
            return None
        cleaned = str(value).strip()
        if not cleaned or cleaned.lower() in ['null', 'none', '']:
            return None
        if max_length and len(cleaned) > max_length:
            cleaned = cleaned[:max_length]
        return cleaned
    
    def truncate_table(self, table_name: str, schema: str = None):
        """Faz TRUNCATE em uma tabela"""
        if schema is None:
            schema = get_schema_atual()
        
        conn = None
        try:
            conn = DatabaseConnection.get_postgresql_destino_connection()
            cursor = conn.cursor()
            
            query = f"TRUNCATE TABLE {schema}.{table_name} CASCADE"
            cursor.execute(query)
            conn.commit()
            
            logger.info(f"Tabela {schema}.{table_name} truncada com sucesso")
            print(f"OK - Tabela {schema}.{table_name} truncada")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Erro ao truncar tabela {schema}.{table_name}: {e}")
            if conn:
                conn.rollback()
                conn.close()
            raise
    
    def convert_status_pedido(self, status_pedido: Optional[int]) -> str:
        """Converte StatusPedido (int) para status (string)"""
        if status_pedido is None:
            return 'pending'
        # Mapear valores comuns de StatusPedido para strings
        status_map = {
            0: 'pending',
            1: 'approved',
            2: 'rejected',
            3: 'cancelled',
            4: 'active',
            5: 'inactive'
        }
        return status_map.get(status_pedido, f'status_{status_pedido}')
    
    def map_billing_type(self, modo_faturamento: Optional[str]) -> str:
        """Mapeia ModoFaturamento para billing_type"""
        if not modo_faturamento:
            return 'monthly'
        modo = str(modo_faturamento).strip().upper()
        if modo in ['M', 'MONTHLY', 'MENSAL']:
            return 'monthly'
        elif modo in ['W', 'WEEKLY', 'SEMANAL']:
            return 'weekly'
        elif modo in ['D', 'DAILY', 'DIARIO']:
            return 'daily'
        else:
            return 'monthly'  # padrão
    
    def map_operation_type(self, tipo_orcamento: Optional[str]) -> str:
        """Mapeia TipoOrcamento para operation_type"""
        if not tipo_orcamento:
            return 'standard'
        tipo = str(tipo_orcamento).strip().upper()
        # Mapear conforme valores possíveis
        if tipo in ['ST', 'STANDARD', 'PADRAO']:
            return 'standard'
        elif tipo in ['SP', 'SPECIAL', 'ESPECIAL']:
            return 'special'
        else:
            return 'standard'  # padrão
    
    def map_thirteenth_salary_type(self, tipo_calculo: Optional[int]) -> str:
        """Mapeia TipoCalculoDecimoTerceiro para thirteenth_salary_type"""
        if tipo_calculo is None:
            return 'none'
        # Mapear valores comuns
        tipo_map = {
            0: 'none',
            1: 'proportional',
            2: 'full',
            3: 'custom'
        }
        return tipo_map.get(tipo_calculo, 'none')
    
    def map_trade_type(self, trade_marketing: Optional[str]) -> str:
        """Mapeia TradeMarketing para trade_type"""
        if not trade_marketing:
            return 'none'
        trade = str(trade_marketing).strip().upper()
        if trade in ['N', 'NONE', 'NAO', 'NÃO']:
            return 'none'
        elif trade in ['S', 'SIM', 'YES']:
            return 'yes'
        else:
            return 'none'  # padrão
    
    def validate_step1_contracts(self):
        """Validação e relatório de qualidade - ETAPA 1"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 1: CONTRACTS")
        print("-"*80)
        
        try:
            # Contar origem (aplicando limite se especificado)
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            # Contar registros únicos de IdOrcamento na view
            if self.limit_rows > 0:
                cursor_sql.execute(f"""
                    SELECT COUNT(DISTINCT IdOrcamento) 
                    FROM (
                        SELECT DISTINCT TOP {self.limit_rows} IdOrcamento 
                        FROM ViewOrcamentosLojas 
                        ORDER BY IdOrcamento
                    ) AS limited
                """)
            else:
                cursor_sql.execute("SELECT COUNT(DISTINCT IdOrcamento) FROM ViewOrcamentosLojas")
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contracts")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - ViewOrcamentosLojas - IdOrcamento único):")
            print(f"  Total de registros: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.contracts):")
            print(f"  Total de registros: {destino_count}")
            
            diferenca = origem_count - destino_count
            
            if diferenca == 0:
                print(f"\nOK - Todos os registros foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 1: OK - Origem: {origem_count}, Destino: {destino_count}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} registros")
                logger.warning(f"VALIDACAO ETAPA 1: Diferenca - Origem: {origem_count}, Destino: {destino_count}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 1: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step1_migrate_contracts(self):
        """ETAPA 1: Migrar contracts"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 1: MIGRANDO CONTRACTS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 1: Migrando contracts")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        # Verificar se precisa criar coluna legacy_id em HML
        include_legacy = self.should_include_legacy_id()
        if include_legacy:
            try:
                # Tentar adicionar coluna legacy_id se não existir
                conn_check = DatabaseConnection.get_postgresql_destino_connection()
                cursor_check = conn_check.cursor()
                cursor_check.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema}' 
                    AND table_name = 'contracts' 
                    AND column_name = 'legacy_id'
                """)
                if not cursor_check.fetchone():
                    print("[ETAPA 1] Criando coluna legacy_id...")
                    cursor_check.execute(f"ALTER TABLE {schema}.contracts ADD COLUMN legacy_id INTEGER")
                    conn_check.commit()
                    print("OK - Coluna legacy_id criada")
                cursor_check.close()
                conn_check.close()
            except Exception as e:
                logger.warning(f"Nao foi possivel criar/verificar coluna legacy_id: {e}")
        
        # Carregar mapeamento de customers
        print("[ETAPA 1] Carregando mapeamento de customers...")
        try:
            schema_customers = 'gmcore' if destino == 'HML' else 'core'
            conn_customers = DatabaseConnection.get_postgresql_destino_connection()
            cursor_customers = conn_customers.cursor()
            cursor_customers.execute(f"SELECT id, legacy_id FROM {schema_customers}.customers WHERE legacy_id IS NOT NULL")
            for row in cursor_customers.fetchall():
                if row[1] is not None:  # Verificar se legacy_id não é NULL
                    self.customer_id_map[row[1]] = row[0]
            cursor_customers.close()
            conn_customers.close()
            print(f"OK - {len(self.customer_id_map)} customers carregados")
            logger.info(f"Carregados {len(self.customer_id_map)} customers para mapeamento")
        except Exception as e:
            logger.warning(f"Erro ao carregar customers: {e}")
            print(f"AVISO - Nao foi possivel carregar customers: {e}")
        
        # Truncate
        print("\n[ETAPA 1] Limpando tabela contracts...")
        self.truncate_table('contracts')
        
        # Buscar dados do SQL Server usando ViewOrcamentosLojas (agrupado por IdOrcamento)
        # Usar GROUP BY para garantir um único registro por IdOrcamento
        print("[ETAPA 1] Buscando dados do SQL Server...")
        sql_query = """
        SELECT 
            v.IdOrcamento,
            MAX(v.IdCliente) AS IdCliente,
            MAX(v.DiaFaturamento) AS DiaFaturamento,
            MAX(v.DiaVencimento) AS DiaVencimento,
            MAX(v.StatusPedido) AS StatusPedido,
            MAX(v.DataInicioOperacao) AS DataInicioOperacao,
            MAX(v.DataInclusaoOrcamento) AS DataInclusaoOrcamento,
            MAX(v.DataAlteracaoOrcamento) AS DataAlteracaoOrcamento,
            MAX(o.ModoFaturamento) AS ModoFaturamento,
            MAX(o.TipoOrcamento) AS TipoOrcamento,
            MAX(o.TipoCalculoDecimoTerceiro) AS TipoCalculoDecimoTerceiro,
            MAX(o.TradeMarketing) AS TradeMarketing
        FROM ViewOrcamentosLojas v
        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
        GROUP BY v.IdOrcamento
        ORDER BY v.IdOrcamento
        """
        
        # Adicionar LIMIT se especificado
        if self.limit_rows > 0:
            sql_query = sql_query.replace("ORDER BY v.IdOrcamento", 
                f"ORDER BY v.IdOrcamento OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute(sql_query)
        
        # Processar em chunks
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        chunk_num = 0
        total_processed = 0
        
        try:
            while True:
                rows = cursor_sql.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                
                chunk_num += 1
                print(f"[ETAPA 1] Processando chunk {chunk_num} ({len(rows)} registros)...")
                logger.info(f"[ETAPA 1] Processando chunk {chunk_num} ({len(rows)} registros)...")
                
                chunk_errors = 0
                for row in rows:
                    try:
                        contract_id = uuid.uuid4()
                        legado_id = row[0]  # IdOrcamento
                        self.contract_id_map[legado_id] = contract_id
                        
                        # Mapear customer_id
                        customer_legacy_id = row[1]  # IdCliente
                        customer_uuid = self.customer_id_map.get(customer_legacy_id)
                        if not customer_uuid:
                            error_msg = f"Customer nao encontrado: IdCliente={customer_legacy_id} para Orcamento Id={legado_id}"
                            logger.warning(error_msg)
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            continue
                        
                        # Preparar valores
                        billing_day = row[2] if row[2] is not None else 1
                        due_day = row[3] if row[3] is not None else 1
                        billing_type = self.map_billing_type(row[8])  # ModoFaturamento
                        operation_type = self.map_operation_type(row[9])  # TipoOrcamento
                        thirteenth_salary_type = self.map_thirteenth_salary_type(row[10])  # TipoCalculoDecimoTerceiro
                        trade_type = self.map_trade_type(row[11])  # TradeMarketing
                        start_date = row[5]  # DataInicioOperacao
                        status = self.convert_status_pedido(row[4])  # StatusPedido
                        created_at = row[6] if row[6] else datetime.now()  # DataInclusaoOrcamento
                        updated_at = row[7] if row[7] else datetime.now()  # DataAlteracaoOrcamento
                        
                        # Montar query de inserção
                        if include_legacy:
                            insert_query = f"""
                            INSERT INTO {schema}.contracts (
                                id, legacy_id, customer_id, billing_day, due_day,
                                billing_type, operation_type, thirteenth_salary_type, trade_type,
                                start_date, status, deleted_at, created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor_pg.execute(insert_query, (
                                str(contract_id),
                                legado_id,  # legacy_id
                                str(customer_uuid),
                                billing_day,
                                due_day,
                                billing_type,
                                operation_type,
                                thirteenth_salary_type,
                                trade_type,
                                start_date,
                                status,
                                None,  # deleted_at sempre NULL
                                created_at,
                                updated_at
                            ))
                        else:
                            insert_query = f"""
                            INSERT INTO {schema}.contracts (
                                id, customer_id, billing_day, due_day,
                                billing_type, operation_type, thirteenth_salary_type, trade_type,
                                start_date, status, deleted_at, created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor_pg.execute(insert_query, (
                                str(contract_id),
                                str(customer_uuid),
                                billing_day,
                                due_day,
                                billing_type,
                                operation_type,
                                thirteenth_salary_type,
                                trade_type,
                                start_date,
                                status,
                                None,  # deleted_at sempre NULL
                                created_at,
                                updated_at
                            ))
                        
                        self.stats['contracts'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir contract IdOrcamento={legado_id}: {e}"
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        continue
                
                # Commit após cada chunk
                conn_pg.commit()
                total_processed += len(rows)
                print(f"[ETAPA 1] Chunk {chunk_num} processado: {len(rows) - chunk_errors} registros inseridos, {chunk_errors} erros")
                logger.info(f"[ETAPA 1] Chunk {chunk_num}: {len(rows) - chunk_errors} inseridos, {chunk_errors} erros")
            
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\n[ETAPA 1] CONCLUIDA! Total de contracts migrados: {self.stats['contracts']}")
            logger.info(f"[ETAPA 1] CONCLUIDA! Total: {self.stats['contracts']}")
            
            # Validação
            self.validate_step1_contracts()
            
        except Exception as e:
            logger.error(f"Erro critico na ETAPA 1: {e}")
            if conn_pg:
                conn_pg.rollback()
                conn_pg.close()
            if conn_sql:
                conn_sql.close()
            raise
    
    def convert_hours_to_float(self, horas: Optional[str]) -> Optional[float]:
        """Converte Horas (nvarchar) para double precision"""
        if horas is None:
            return None
        try:
            # Remover espaços e tentar converter
            horas_clean = str(horas).strip().replace(',', '.')
            if not horas_clean or horas_clean.lower() in ['null', 'none', '']:
                return None
            return float(horas_clean)
        except (ValueError, AttributeError):
            return None
    
    def convert_status_to_int(self, status_pedido: Optional[int], ativo: Optional[bool] = None) -> int:
        """Converte StatusPedido ou Ativo para status (integer)"""
        # Se StatusPedido estiver disponível, usar ele
        if status_pedido is not None:
            return int(status_pedido)
        # Caso contrário, usar Ativo (True = 1, False = 0)
        if ativo is not None:
            return 1 if ativo else 0
        # Padrão: 0 (inativo/pending)
        return 0
    
    def validate_step2_contract_scenarios(self):
        """Validação e relatório de qualidade - ETAPA 2"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 2: CONTRACT_SCENARIOS")
        print("-"*80)
        
        try:
            # Contar origem (aplicando limite se especificado)
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            # Contar registros únicos de IdOrcamentoLoja na view
            if self.limit_rows > 0:
                cursor_sql.execute(f"""
                    SELECT COUNT(DISTINCT IdOrcamentoLoja) 
                    FROM (
                        SELECT DISTINCT TOP {self.limit_rows} IdOrcamentoLoja 
                        FROM ViewOrcamentosLojas 
                        ORDER BY IdOrcamentoLoja
                    ) AS limited
                """)
            else:
                cursor_sql.execute("SELECT COUNT(DISTINCT IdOrcamentoLoja) FROM ViewOrcamentosLojas")
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contract_scenarios")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - ViewOrcamentosLojas - IdOrcamentoLoja único):")
            print(f"  Total de registros: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.contract_scenarios):")
            print(f"  Total de registros: {destino_count}")
            
            diferenca = origem_count - destino_count
            
            if diferenca == 0:
                print(f"\nOK - Todos os registros foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 2: OK - Origem: {origem_count}, Destino: {destino_count}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} registros")
                logger.warning(f"VALIDACAO ETAPA 2: Diferenca - Origem: {origem_count}, Destino: {destino_count}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 2: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step2_migrate_contract_scenarios(self):
        """ETAPA 2: Migrar contract_scenarios"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 2: MIGRANDO CONTRACT_SCENARIOS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 2: Migrando contract_scenarios")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        # Carregar mapeamento de stores
        print("[ETAPA 2] Carregando mapeamento de stores...")
        try:
            schema_stores = 'gmcore' if destino == 'HML' else 'core'
            conn_stores = DatabaseConnection.get_postgresql_destino_connection()
            cursor_stores = conn_stores.cursor()
            cursor_stores.execute(f"SELECT id, legacy_id FROM {schema_stores}.stores WHERE legacy_id IS NOT NULL")
            for row in cursor_stores.fetchall():
                if row[1] is not None:
                    self.store_id_map[row[1]] = row[0]
            cursor_stores.close()
            conn_stores.close()
            print(f"OK - {len(self.store_id_map)} stores carregados")
            logger.info(f"Carregados {len(self.store_id_map)} stores para mapeamento")
        except Exception as e:
            logger.warning(f"Erro ao carregar stores: {e}")
            print(f"AVISO - Nao foi possivel carregar stores: {e}")
        
        # Truncate
        print("\n[ETAPA 2] Limpando tabela contract_scenarios...")
        self.truncate_table('contract_scenarios')
        
        # Buscar dados do SQL Server usando ViewOrcamentosLojas
        print("[ETAPA 2] Buscando dados do SQL Server...")
        sql_query = """
        SELECT 
            v.IdOrcamentoLoja,
            v.IdOrcamento,
            v.IdEstabelecimento,
            v.NomeTarefa,
            v.Frequencia,
            v.Horas,
            v.ValorHora,
            v.DataInicioOperacao,
            v.StatusPedido,
            v.DataInclusaoOrcamentoLojas,
            v.DataAlteracaoOrcamentoLojas,
            ol.Ativo,
            ol.IdTarefa
        FROM ViewOrcamentosLojas v
        LEFT JOIN OrcamentoLojas ol ON ol.Id = v.IdOrcamentoLoja
        ORDER BY v.IdOrcamentoLoja
        """
        
        # Adicionar LIMIT se especificado
        if self.limit_rows > 0:
            sql_query = sql_query.replace("ORDER BY v.IdOrcamentoLoja", 
                f"ORDER BY v.IdOrcamentoLoja OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute(sql_query)
        
        # Processar em chunks
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        chunk_num = 0
        total_processed = 0
        
        try:
            while True:
                rows = cursor_sql.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                
                chunk_num += 1
                print(f"[ETAPA 2] Processando chunk {chunk_num} ({len(rows)} registros)...")
                logger.info(f"[ETAPA 2] Processando chunk {chunk_num} ({len(rows)} registros)...")
                
                chunk_errors = 0
                for row in rows:
                    try:
                        scenario_id = uuid.uuid4()
                        legado_id_orcamento_loja = row[0]  # IdOrcamentoLoja
                        legado_id_orcamento = row[1]  # IdOrcamento
                        legado_id_estabelecimento = row[2]  # IdEstabelecimento
                        
                        # Guardar mapeamento para uso nas próximas etapas
                        self.scenario_id_map[legado_id_orcamento_loja] = scenario_id
                        
                        # Mapear contract_id usando contract_id_map da etapa 1
                        contract_uuid = self.contract_id_map.get(legado_id_orcamento)
                        if not contract_uuid:
                            error_msg = f"Contract nao encontrado: IdOrcamento={legado_id_orcamento} para Scenario IdOrcamentoLoja={legado_id_orcamento_loja}"
                            logger.warning(error_msg)
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            continue
                        
                        # Mapear store_id
                        store_uuid = self.store_id_map.get(legado_id_estabelecimento)
                        if not store_uuid:
                            error_msg = f"Store nao encontrado: IdEstabelecimento={legado_id_estabelecimento} para Scenario IdOrcamentoLoja={row[0]}"
                            logger.warning(error_msg)
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            continue
                        
                        # Preparar valores
                        task = self.clean_string(row[3]) or self.clean_string(row[12])  # NomeTarefa ou IdTarefa
                        frequency = self.clean_string(row[4])  # Frequencia
                        hours = self.convert_hours_to_float(row[5])  # Horas (nvarchar -> float)
                        hour_value = row[6] if row[6] is not None else 0.0  # ValorHora
                        start_date = row[7]  # DataInicioOperacao
                        status = self.convert_status_to_int(row[8], row[11])  # StatusPedido ou Ativo
                        created_at = row[9] if row[9] else datetime.now()  # DataInclusaoOrcamentoLojas
                        updated_at = row[10] if row[10] else datetime.now()  # DataAlteracaoOrcamentoLojas
                        
                        # Montar query de inserção
                        insert_query = f"""
                        INSERT INTO {schema}.contract_scenarios (
                            id, contract_id, store_id, task, frequency, hours, hour_value,
                            start_date, status, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        cursor_pg.execute(insert_query, (
                            str(scenario_id),
                            str(contract_uuid),
                            str(store_uuid),
                            task,
                            frequency,
                            hours,
                            hour_value,
                            start_date,
                            status,
                            created_at,
                            updated_at
                        ))
                        
                        self.stats['contract_scenarios'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir contract_scenario IdOrcamentoLoja={row[0]}: {e}"
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        continue
                
                # Commit após cada chunk
                conn_pg.commit()
                total_processed += len(rows)
                print(f"[ETAPA 2] Chunk {chunk_num} processado: {len(rows) - chunk_errors} registros inseridos, {chunk_errors} erros")
                logger.info(f"[ETAPA 2] Chunk {chunk_num}: {len(rows) - chunk_errors} inseridos, {chunk_errors} erros")
            
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\n[ETAPA 2] CONCLUIDA! Total de contract_scenarios migrados: {self.stats['contract_scenarios']}")
            logger.info(f"[ETAPA 2] CONCLUIDA! Total: {self.stats['contract_scenarios']}")
            
            # Validação
            self.validate_step2_contract_scenarios()
            
        except Exception as e:
            logger.error(f"Erro critico na ETAPA 2: {e}")
            if conn_pg:
                conn_pg.rollback()
                conn_pg.close()
            if conn_sql:
                conn_sql.close()
            raise
    
    def validate_step3_contract_scenario_stores(self):
        """Validação e relatório de qualidade - ETAPA 3"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 3: CONTRACT_SCENARIO_STORES")
        print("-"*80)
        
        try:
            # Contar origem (aplicando limite se especificado)
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            # Contar registros únicos de IdOrcamentoLoja na view
            if self.limit_rows > 0:
                cursor_sql.execute(f"""
                    SELECT COUNT(DISTINCT IdOrcamentoLoja) 
                    FROM (
                        SELECT DISTINCT TOP {self.limit_rows} IdOrcamentoLoja 
                        FROM ViewOrcamentosLojas 
                        ORDER BY IdOrcamentoLoja
                    ) AS limited
                """)
            else:
                cursor_sql.execute("SELECT COUNT(DISTINCT IdOrcamentoLoja) FROM ViewOrcamentosLojas")
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contract_scenario_stores")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - ViewOrcamentosLojas - IdOrcamentoLoja único):")
            print(f"  Total de registros: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.contract_scenario_stores):")
            print(f"  Total de registros: {destino_count}")
            
            diferenca = origem_count - destino_count
            
            if diferenca == 0:
                print(f"\nOK - Todos os registros foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 3: OK - Origem: {origem_count}, Destino: {destino_count}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} registros")
                logger.warning(f"VALIDACAO ETAPA 3: Diferenca - Origem: {origem_count}, Destino: {destino_count}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 3: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step3_migrate_contract_scenario_stores(self):
        """ETAPA 3: Migrar contract_scenario_stores"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 3: MIGRANDO CONTRACT_SCENARIO_STORES")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 3: Migrando contract_scenario_stores")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        # Verificar se precisa criar coluna legacy_id em HML
        include_legacy = self.should_include_legacy_id()
        if include_legacy:
            try:
                conn_check = DatabaseConnection.get_postgresql_destino_connection()
                cursor_check = conn_check.cursor()
                cursor_check.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema}' 
                    AND table_name = 'contract_scenario_stores' 
                    AND column_name = 'legacy_id'
                """)
                if not cursor_check.fetchone():
                    print("[ETAPA 3] Criando coluna legacy_id...")
                    cursor_check.execute(f"ALTER TABLE {schema}.contract_scenario_stores ADD COLUMN legacy_id INTEGER")
                    conn_check.commit()
                    print("OK - Coluna legacy_id criada")
                cursor_check.close()
                conn_check.close()
            except Exception as e:
                logger.warning(f"Nao foi possivel criar/verificar coluna legacy_id: {e}")
        
        # Truncate
        print("\n[ETAPA 3] Limpando tabela contract_scenario_stores...")
        self.truncate_table('contract_scenario_stores')
        
        # Buscar dados do SQL Server usando ViewOrcamentosLojas + OrcamentoLojas
        print("[ETAPA 3] Buscando dados do SQL Server...")
        sql_query = """
        SELECT 
            v.IdOrcamentoLoja,
            v.IdEstabelecimento,
            v.DataInicioOperacao,
            v.DataInclusaoOrcamentoLojas,
            v.DataAlteracaoOrcamentoLojas,
            ol.Ativo,
            ol.DataExclusao
        FROM ViewOrcamentosLojas v
        LEFT JOIN OrcamentoLojas ol ON ol.Id = v.IdOrcamentoLoja
        ORDER BY v.IdOrcamentoLoja
        """
        
        # Adicionar LIMIT se especificado
        if self.limit_rows > 0:
            sql_query = sql_query.replace("ORDER BY v.IdOrcamentoLoja", 
                f"ORDER BY v.IdOrcamentoLoja OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute(sql_query)
        
        # Processar em chunks
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        chunk_num = 0
        total_processed = 0
        
        try:
            while True:
                rows = cursor_sql.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                
                chunk_num += 1
                print(f"[ETAPA 3] Processando chunk {chunk_num} ({len(rows)} registros)...")
                logger.info(f"[ETAPA 3] Processando chunk {chunk_num} ({len(rows)} registros)...")
                
                chunk_errors = 0
                for row in rows:
                    try:
                        scenario_store_id = uuid.uuid4()
                        legado_id_orcamento_loja = row[0]  # IdOrcamentoLoja
                        legado_id_estabelecimento = row[1]  # IdEstabelecimento
                        
                        # Mapear scenario_id usando scenario_id_map da etapa 2
                        scenario_uuid = self.scenario_id_map.get(legado_id_orcamento_loja)
                        if not scenario_uuid:
                            error_msg = f"Scenario nao encontrado: IdOrcamentoLoja={legado_id_orcamento_loja}"
                            logger.warning(error_msg)
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            continue
                        
                        # Mapear store_id
                        store_uuid = self.store_id_map.get(legado_id_estabelecimento)
                        if not store_uuid:
                            error_msg = f"Store nao encontrado: IdEstabelecimento={legado_id_estabelecimento} para ScenarioStore IdOrcamentoLoja={legado_id_orcamento_loja}"
                            logger.warning(error_msg)
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            continue
                        
                        # Preparar valores
                        start_date = row[2]  # DataInicioOperacao
                        status = 1 if (row[5] is True) else 0  # Ativo -> status (1=ativo, 0=inativo)
                        removed_at = row[6] if row[6] else None  # DataExclusao
                        created_at = row[3] if row[3] else datetime.now()  # DataInclusaoOrcamentoLojas
                        updated_at = row[4] if row[4] else datetime.now()  # DataAlteracaoOrcamentoLojas
                        
                        # Montar query de inserção
                        if include_legacy:
                            insert_query = f"""
                            INSERT INTO {schema}.contract_scenario_stores (
                                id, legacy_id, scenario_id, store_id, start_date,
                                status, removed_at, created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor_pg.execute(insert_query, (
                                str(scenario_store_id),
                                legado_id_orcamento_loja,  # legacy_id
                                str(scenario_uuid),
                                str(store_uuid),
                                start_date,
                                status,
                                removed_at,
                                created_at,
                                updated_at
                            ))
                        else:
                            insert_query = f"""
                            INSERT INTO {schema}.contract_scenario_stores (
                                id, scenario_id, store_id, start_date,
                                status, removed_at, created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor_pg.execute(insert_query, (
                                str(scenario_store_id),
                                str(scenario_uuid),
                                str(store_uuid),
                                start_date,
                                status,
                                removed_at,
                                created_at,
                                updated_at
                            ))
                        
                        self.stats['contract_scenario_stores'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir contract_scenario_store IdOrcamentoLoja={row[0]}: {e}"
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        continue
                
                # Commit após cada chunk
                conn_pg.commit()
                total_processed += len(rows)
                print(f"[ETAPA 3] Chunk {chunk_num} processado: {len(rows) - chunk_errors} registros inseridos, {chunk_errors} erros")
                logger.info(f"[ETAPA 3] Chunk {chunk_num}: {len(rows) - chunk_errors} inseridos, {chunk_errors} erros")
            
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\n[ETAPA 3] CONCLUIDA! Total de contract_scenario_stores migrados: {self.stats['contract_scenario_stores']}")
            logger.info(f"[ETAPA 3] CONCLUIDA! Total: {self.stats['contract_scenario_stores']}")
            
            # Validação
            self.validate_step3_contract_scenario_stores()
            
        except Exception as e:
            logger.error(f"Erro critico na ETAPA 3: {e}")
            if conn_pg:
                conn_pg.rollback()
                conn_pg.close()
            if conn_sql:
                conn_sql.close()
            raise
    
    def validate_step4_contract_sellers(self):
        """Validação e relatório de qualidade - ETAPA 4"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 4: CONTRACT_SELLERS")
        print("-"*80)
        
        try:
            # Contar origem: Orcamento (IdUsuarioVendedor) + FaturamentoOrcamentoComissao
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            # Contar Orcamentos com IdUsuarioVendedor
            if self.limit_rows > 0:
                cursor_sql.execute(f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT TOP {self.limit_rows} Id 
                        FROM Orcamento 
                        WHERE IdUsuarioVendedor IS NOT NULL
                        ORDER BY Id
                    ) AS limited
                """)
            else:
                cursor_sql.execute("SELECT COUNT(*) FROM Orcamento WHERE IdUsuarioVendedor IS NOT NULL")
            origem_orcamento = cursor_sql.fetchone()[0]
            
            # Contar FaturamentoOrcamentoComissao
            if self.limit_rows > 0:
                cursor_sql.execute(f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT TOP {self.limit_rows} Id 
                        FROM FaturamentoOrcamentoComissao 
                        ORDER BY Id
                    ) AS limited
                """)
            else:
                cursor_sql.execute("SELECT COUNT(*) FROM FaturamentoOrcamentoComissao")
            origem_comissao = cursor_sql.fetchone()[0]
            
            origem_total = origem_orcamento + origem_comissao
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contract_sellers")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD):")
            print(f"  Orcamentos com vendedor: {origem_orcamento}")
            print(f"  Comissoes: {origem_comissao}")
            print(f"  Total esperado: {origem_total}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.contract_sellers):")
            print(f"  Total de registros: {destino_count}")
            
            diferenca = origem_total - destino_count
            
            if diferenca == 0:
                print(f"\nOK - Todos os registros foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 4: OK - Origem: {origem_total}, Destino: {destino_count}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} registros")
                logger.warning(f"VALIDACAO ETAPA 4: Diferenca - Origem: {origem_total}, Destino: {destino_count}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 4: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step4_migrate_contract_sellers(self):
        """ETAPA 4: Migrar contract_sellers"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 4: MIGRANDO CONTRACT_SELLERS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 4: Migrando contract_sellers")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        # Carregar mapeamento de users
        print("[ETAPA 4] Carregando mapeamento de users...")
        user_id_map = {}
        try:
            schema_users = 'gmcore' if destino == 'HML' else 'core'
            conn_users = DatabaseConnection.get_postgresql_destino_connection()
            cursor_users = conn_users.cursor()
            cursor_users.execute(f"SELECT id, legacy_id FROM {schema_users}.users WHERE legacy_id IS NOT NULL")
            for row in cursor_users.fetchall():
                if row[1] is not None:
                    user_id_map[row[1]] = row[0]
            cursor_users.close()
            conn_users.close()
            print(f"OK - {len(user_id_map)} users carregados")
            logger.info(f"Carregados {len(user_id_map)} users para mapeamento")
        except Exception as e:
            logger.warning(f"Erro ao carregar users: {e}")
            print(f"AVISO - Nao foi possivel carregar users: {e}")
        
        # Truncate
        print("\n[ETAPA 4] Limpando tabela contract_sellers...")
        self.truncate_table('contract_sellers')
        
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # PARTE 1: Migrar sellers de Orcamento (IdUsuarioVendedor)
        print("[ETAPA 4] Migrando sellers de Orcamento...")
        sql_query_orcamento = """
        SELECT 
            Id,
            IdUsuarioVendedor
        FROM Orcamento
        WHERE IdUsuarioVendedor IS NOT NULL
        ORDER BY Id
        """
        
        if self.limit_rows > 0:
            sql_query_orcamento = sql_query_orcamento.replace("ORDER BY Id", 
                f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute(sql_query_orcamento)
        
        chunk_num = 0
        chunk_errors = 0
        
        try:
            while True:
                rows = cursor_sql.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                
                chunk_num += 1
                print(f"[ETAPA 4] Processando chunk Orcamento {chunk_num} ({len(rows)} registros)...")
                
                for row in rows:
                    try:
                        seller_id = uuid.uuid4()
                        legado_id_orcamento = row[0]  # Id
                        legado_id_usuario = row[1]  # IdUsuarioVendedor
                        
                        # Mapear contract_id
                        contract_uuid = self.contract_id_map.get(legado_id_orcamento)
                        if not contract_uuid:
                            error_msg = f"Contract nao encontrado: IdOrcamento={legado_id_orcamento}"
                            logger.warning(error_msg)
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            continue
                        
                        # Mapear user_id
                        user_uuid = user_id_map.get(legado_id_usuario)
                        if not user_uuid:
                            error_msg = f"User nao encontrado: IdUsuarioVendedor={legado_id_usuario} para Orcamento={legado_id_orcamento}"
                            logger.warning(error_msg)
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            continue
                        
                        insert_query = f"""
                        INSERT INTO {schema}.contract_sellers (
                            id, contract_id, user_id, seller_type, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        cursor_pg.execute(insert_query, (
                            str(seller_id),
                            str(contract_uuid),
                            str(user_uuid),
                            'main',  # seller_type para Orcamento
                            datetime.now(),
                            datetime.now()
                        ))
                        
                        self.stats['contract_sellers'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir contract_seller de Orcamento Id={row[0]}: {e}"
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        continue
                
                conn_pg.commit()
            
            cursor_sql.close()
            conn_sql.close()
            
            # PARTE 2: Migrar sellers de FaturamentoOrcamentoComissao
            print("[ETAPA 4] Migrando sellers de FaturamentoOrcamentoComissao...")
            sql_query_comissao = """
            SELECT 
                foc.IdOrcamento,
                foc.IdUsuarioVendedor,
                foc.DataInclusao,
                foc.DataAlteracao
            FROM FaturamentoOrcamentoComissao foc
            ORDER BY foc.Id
            """
            
            if self.limit_rows > 0:
                sql_query_comissao = sql_query_comissao.replace("ORDER BY foc.Id", 
                    f"ORDER BY foc.Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
            
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            cursor_sql.execute(sql_query_comissao)
            
            chunk_num = 0
            chunk_errors = 0
            
            while True:
                rows = cursor_sql.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                
                chunk_num += 1
                print(f"[ETAPA 4] Processando chunk Comissao {chunk_num} ({len(rows)} registros)...")
                
                for row in rows:
                    try:
                        seller_id = uuid.uuid4()
                        legado_id_orcamento = row[0]  # IdOrcamento
                        legado_id_usuario = row[1]  # IdUsuarioVendedor
                        
                        # Mapear contract_id
                        contract_uuid = self.contract_id_map.get(legado_id_orcamento)
                        if not contract_uuid:
                            error_msg = f"Contract nao encontrado: IdOrcamento={legado_id_orcamento} para Comissao"
                            logger.warning(error_msg)
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            continue
                        
                        # Mapear user_id
                        user_uuid = user_id_map.get(legado_id_usuario)
                        if not user_uuid:
                            error_msg = f"User nao encontrado: IdUsuarioVendedor={legado_id_usuario} para Comissao Orcamento={legado_id_orcamento}"
                            logger.warning(error_msg)
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            continue
                        
                        insert_query = f"""
                        INSERT INTO {schema}.contract_sellers (
                            id, contract_id, user_id, seller_type, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        cursor_pg.execute(insert_query, (
                            str(seller_id),
                            str(contract_uuid),
                            str(user_uuid),
                            'commission',  # seller_type para Comissao
                            row[2] if row[2] else datetime.now(),  # DataInclusao
                            row[3] if row[3] else datetime.now()  # DataAlteracao
                        ))
                        
                        self.stats['contract_sellers'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir contract_seller de Comissao IdOrcamento={row[0]}: {e}"
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        continue
                
                conn_pg.commit()
            
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\n[ETAPA 4] CONCLUIDA! Total de contract_sellers migrados: {self.stats['contract_sellers']}")
            logger.info(f"[ETAPA 4] CONCLUIDA! Total: {self.stats['contract_sellers']}")
            
            # Validação
            self.validate_step4_contract_sellers()
            
        except Exception as e:
            logger.error(f"Erro critico na ETAPA 4: {e}")
            if conn_pg:
                conn_pg.rollback()
                conn_pg.close()
            if conn_sql:
                conn_sql.close()
            raise
    
    def validate_step5_contract_team_members(self):
        """Validação e relatório de qualidade - ETAPA 5"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 5: CONTRACT_TEAM_MEMBERS")
        print("-"*80)
        
        try:
            # Contar origem: Orcamentos com IdUsuarioVendedor
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            if self.limit_rows > 0:
                cursor_sql.execute(f"""
                    SELECT COUNT(DISTINCT IdOrcamento) 
                    FROM (
                        SELECT DISTINCT TOP {self.limit_rows} v.IdOrcamento 
                        FROM ViewOrcamentosLojas v
                        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                        WHERE o.IdUsuarioVendedor IS NOT NULL
                        ORDER BY v.IdOrcamento
                    ) AS limited
                """)
            else:
                cursor_sql.execute("""
                    SELECT COUNT(DISTINCT v.IdOrcamento) 
                    FROM ViewOrcamentosLojas v
                    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                    WHERE o.IdUsuarioVendedor IS NOT NULL
                """)
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contract_team_members")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - Orcamentos com IdUsuarioVendedor):")
            print(f"  Total de registros: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.contract_team_members):")
            print(f"  Total de registros: {destino_count}")
            
            diferenca = origem_count - destino_count
            
            if diferenca == 0:
                print(f"\nOK - Todos os registros foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 5: OK - Origem: {origem_count}, Destino: {destino_count}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} registros")
                logger.warning(f"VALIDACAO ETAPA 5: Diferenca - Origem: {origem_count}, Destino: {destino_count}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 5: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step5_migrate_contract_team_members(self):
        """ETAPA 5: Migrar contract_team_members (apenas seller)"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 5: MIGRANDO CONTRACT_TEAM_MEMBERS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 5: Migrando contract_team_members")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        # Carregar mapeamento de users
        print("[ETAPA 5] Carregando mapeamento de users...")
        user_id_map = {}
        try:
            schema_users = 'gmcore' if destino == 'HML' else 'core'
            conn_users = DatabaseConnection.get_postgresql_destino_connection()
            cursor_users = conn_users.cursor()
            cursor_users.execute(f"SELECT id, legacy_id FROM {schema_users}.users WHERE legacy_id IS NOT NULL")
            for row in cursor_users.fetchall():
                if row[1] is not None:
                    user_id_map[row[1]] = row[0]
            cursor_users.close()
            conn_users.close()
            print(f"OK - {len(user_id_map)} users carregados")
            logger.info(f"Carregados {len(user_id_map)} users para mapeamento")
        except Exception as e:
            logger.warning(f"Erro ao carregar users: {e}")
            print(f"AVISO - Nao foi possivel carregar users: {e}")
        
        # Truncate
        print("\n[ETAPA 5] Limpando tabela contract_team_members...")
        self.truncate_table('contract_team_members')
        
        # Buscar dados do SQL Server (apenas seller - IdUsuarioVendedor)
        print("[ETAPA 5] Buscando dados do SQL Server...")
        sql_query = """
        SELECT DISTINCT
            v.IdOrcamento,
            o.IdUsuarioVendedor,
            v.DataInclusaoOrcamento,
            v.DataAlteracaoOrcamento
        FROM ViewOrcamentosLojas v
        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
        WHERE o.IdUsuarioVendedor IS NOT NULL
        ORDER BY v.IdOrcamento
        """
        
        if self.limit_rows > 0:
            sql_query = sql_query.replace("ORDER BY v.IdOrcamento", 
                f"ORDER BY v.IdOrcamento OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute(sql_query)
        
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        chunk_num = 0
        chunk_errors = 0
        
        try:
            while True:
                rows = cursor_sql.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                
                chunk_num += 1
                print(f"[ETAPA 5] Processando chunk {chunk_num} ({len(rows)} registros)...")
                logger.info(f"[ETAPA 5] Processando chunk {chunk_num} ({len(rows)} registros)...")
                
                for row in rows:
                    try:
                        team_member_id = uuid.uuid4()
                        legado_id_orcamento = row[0]  # IdOrcamento
                        legado_id_usuario = row[1]  # IdUsuarioVendedor
                        
                        # Mapear contract_id
                        contract_uuid = self.contract_id_map.get(legado_id_orcamento)
                        if not contract_uuid:
                            error_msg = f"Contract nao encontrado: IdOrcamento={legado_id_orcamento}"
                            logger.warning(error_msg)
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            continue
                        
                        # Mapear user_id
                        user_uuid = user_id_map.get(legado_id_usuario)
                        if not user_uuid:
                            error_msg = f"User nao encontrado: IdUsuarioVendedor={legado_id_usuario} para Orcamento={legado_id_orcamento}"
                            logger.warning(error_msg)
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            continue
                        
                        insert_query = f"""
                        INSERT INTO {schema}.contract_team_members (
                            id, contract_id, user_id, position, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        cursor_pg.execute(insert_query, (
                            str(team_member_id),
                            str(contract_uuid),
                            str(user_uuid),
                            'seller',  # position sempre "seller" na primeira carga
                            row[2] if row[2] else datetime.now(),  # DataInclusaoOrcamento
                            row[3] if row[3] else datetime.now()  # DataAlteracaoOrcamento
                        ))
                        
                        self.stats['contract_team_members'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir contract_team_member IdOrcamento={row[0]}: {e}"
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        continue
                
                conn_pg.commit()
                print(f"[ETAPA 5] Chunk {chunk_num} processado: {len(rows) - chunk_errors} registros inseridos, {chunk_errors} erros")
                logger.info(f"[ETAPA 5] Chunk {chunk_num}: {len(rows) - chunk_errors} inseridos, {chunk_errors} erros")
            
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\n[ETAPA 5] CONCLUIDA! Total de contract_team_members migrados: {self.stats['contract_team_members']}")
            logger.info(f"[ETAPA 5] CONCLUIDA! Total: {self.stats['contract_team_members']}")
            
            # Validação
            self.validate_step5_contract_team_members()
            
        except Exception as e:
            logger.error(f"Erro critico na ETAPA 5: {e}")
            if conn_pg:
                conn_pg.rollback()
                conn_pg.close()
            if conn_sql:
                conn_sql.close()
            raise


    def validate_step6_contract_contacts(self):
        """Validação e relatório de qualidade - ETAPA 6"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 6: CONTRACT_CONTACTS")
        print("-"*80)
        
        try:
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            if self.limit_rows > 0:
                cursor_sql.execute(f"""
                    SELECT COUNT(DISTINCT IdOrcamento) 
                    FROM (
                        SELECT DISTINCT TOP {self.limit_rows} IdOrcamento 
                        FROM ViewOrcamentosLojas 
                        WHERE NomeCliente IS NOT NULL
                        ORDER BY IdOrcamento
                    ) AS limited
                """)
            else:
                cursor_sql.execute("SELECT COUNT(DISTINCT IdOrcamento) FROM ViewOrcamentosLojas WHERE NomeCliente IS NOT NULL")
            origem_count = cursor_sql.fetchone()[0]
            
            if self.limit_rows > 0:
                cursor_sql.execute(f"""
                    SELECT COUNT(DISTINCT IdOrcamento) 
                    FROM (
                        SELECT DISTINCT TOP {self.limit_rows} IdOrcamento 
                        FROM ViewOrcamentosLojas 
                        WHERE NomeCliente IS NOT NULL 
                        AND NomeSistemaCliente IS NOT NULL 
                        AND NomeSistemaCliente != NomeCliente
                        ORDER BY IdOrcamento
                    ) AS limited
                """)
            else:
                cursor_sql.execute("""
                    SELECT COUNT(DISTINCT IdOrcamento) 
                    FROM ViewOrcamentosLojas 
                    WHERE NomeCliente IS NOT NULL 
                    AND NomeSistemaCliente IS NOT NULL 
                    AND NomeSistemaCliente != NomeCliente
                """)
            origem_com_sistema = cursor_sql.fetchone()[0]
            origem_total_esperado = origem_count + origem_com_sistema
            
            cursor_sql.close()
            conn_sql.close()
            
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contract_contacts")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - ViewOrcamentosLojas):")
            print(f"  Orcamentos com NomeCliente: {origem_count}")
            print(f"  Com NomeSistemaCliente diferente: {origem_com_sistema}")
            print(f"  Total esperado: {origem_total_esperado}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.contract_contacts):")
            print(f"  Total de registros: {destino_count}")
            
            diferenca = origem_total_esperado - destino_count
            
            if diferenca == 0:
                print(f"\nOK - Todos os registros foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 6: OK - Origem: {origem_total_esperado}, Destino: {destino_count}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} registros")
                logger.warning(f"VALIDACAO ETAPA 6: Diferenca - Origem: {origem_total_esperado}, Destino: {destino_count}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 6: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step6_migrate_contract_contacts(self):
        """ETAPA 6: Migrar contract_contacts"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 6: MIGRANDO CONTRACT_CONTACTS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 6: Migrando contract_contacts")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        print("\n[ETAPA 6] Limpando tabela contract_contacts...")
        self.truncate_table('contract_contacts')
        
        print("[ETAPA 6] Buscando dados do SQL Server...")
        sql_query = """
        SELECT DISTINCT
            v.IdOrcamento,
            v.NomeCliente,
            v.NomeSistemaCliente,
            v.DataInclusaoOrcamento,
            v.DataAlteracaoOrcamento
        FROM ViewOrcamentosLojas v
        WHERE v.NomeCliente IS NOT NULL
        ORDER BY v.IdOrcamento
        """
        
        if self.limit_rows > 0:
            sql_query = sql_query.replace("ORDER BY v.IdOrcamento", 
                f"ORDER BY v.IdOrcamento OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute(sql_query)
        
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        chunk_num = 0
        chunk_errors = 0
        
        try:
            while True:
                rows = cursor_sql.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                
                chunk_num += 1
                print(f"[ETAPA 6] Processando chunk {chunk_num} ({len(rows)} registros)...")
                logger.info(f"[ETAPA 6] Processando chunk {chunk_num} ({len(rows)} registros)...")
                
                for row in rows:
                    try:
                        legado_id_orcamento = row[0]
                        nome_cliente = self.clean_string(row[1])
                        nome_sistema = self.clean_string(row[2])
                        
                        contract_uuid = self.contract_id_map.get(legado_id_orcamento)
                        if not contract_uuid:
                            error_msg = f"Contract nao encontrado: IdOrcamento={legado_id_orcamento}"
                            logger.warning(error_msg)
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            continue
                        
                        if not nome_cliente:
                            continue
                        
                        contact_id_1 = uuid.uuid4()
                        insert_query = f"""
                        INSERT INTO {schema}.contract_contacts (
                            id, contract_id, name, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s)
                        """
                        cursor_pg.execute(insert_query, (
                            str(contact_id_1),
                            str(contract_uuid),
                            nome_cliente,
                            row[3] if row[3] else datetime.now(),
                            row[4] if row[4] else datetime.now()
                        ))
                        self.stats['contract_contacts'] += 1
                        
                        if nome_sistema and nome_sistema != nome_cliente:
                            contact_id_2 = uuid.uuid4()
                            cursor_pg.execute(insert_query, (
                                str(contact_id_2),
                                str(contract_uuid),
                                nome_sistema,
                                row[3] if row[3] else datetime.now(),
                                row[4] if row[4] else datetime.now()
                            ))
                            self.stats['contract_contacts'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir contract_contact IdOrcamento={row[0]}: {e}"
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        continue
                
                conn_pg.commit()
                print(f"[ETAPA 6] Chunk {chunk_num} processado: {len(rows) - chunk_errors} registros processados, {chunk_errors} erros")
                logger.info(f"[ETAPA 6] Chunk {chunk_num}: {chunk_errors} erros")
            
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\n[ETAPA 6] CONCLUIDA! Total de contract_contacts migrados: {self.stats['contract_contacts']}")
            logger.info(f"[ETAPA 6] CONCLUIDA! Total: {self.stats['contract_contacts']}")
            
            self.validate_step6_contract_contacts()
            
        except Exception as e:
            logger.error(f"Erro critico na ETAPA 6: {e}")
            if conn_pg:
                conn_pg.rollback()
                conn_pg.close()
            if conn_sql:
                conn_sql.close()
            raise
    
    def validate_step7_contract_partners(self):
        """Validação e relatório de qualidade - ETAPA 7"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 7: CONTRACT_PARTNERS")
        print("-"*80)
        
        try:
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            if self.limit_rows > 0:
                cursor_sql.execute(f"""
                    SELECT COUNT(DISTINCT IdOrcamento) 
                    FROM (
                        SELECT DISTINCT TOP {self.limit_rows} IdOrcamento 
                        FROM ViewOrcamentosLojas 
                        WHERE IdClienteLoja IS NOT NULL
                        ORDER BY IdOrcamento
                    ) AS limited
                """)
            else:
                cursor_sql.execute("SELECT COUNT(DISTINCT IdOrcamento) FROM ViewOrcamentosLojas WHERE IdClienteLoja IS NOT NULL")
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contract_partners")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - ViewOrcamentosLojas com IdClienteLoja):")
            print(f"  Total de registros: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.contract_partners):")
            print(f"  Total de registros: {destino_count}")
            
            diferenca = origem_count - destino_count
            
            if diferenca == 0:
                print(f"\nOK - Todos os registros foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 7: OK - Origem: {origem_count}, Destino: {destino_count}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} registros")
                logger.warning(f"VALIDACAO ETAPA 7: Diferenca - Origem: {origem_count}, Destino: {destino_count}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 7: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step7_migrate_contract_partners(self):
        """ETAPA 7: Migrar contract_partners"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 7: MIGRANDO CONTRACT_PARTNERS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 7: Migrando contract_partners")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        print("[ETAPA 7] Carregando mapeamento de persons...")
        person_id_map = {}
        try:
            schema_persons = 'gmcore' if destino == 'HML' else 'core'
            conn_persons = DatabaseConnection.get_postgresql_destino_connection()
            cursor_persons = conn_persons.cursor()
            cursor_persons.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = '{schema_persons}' 
                    AND table_name = 'persons'
                )
            """)
            if cursor_persons.fetchone()[0]:
                cursor_persons.execute(f"SELECT id, legacy_id FROM {schema_persons}.persons WHERE legacy_id IS NOT NULL")
                for row in cursor_persons.fetchall():
                    if row[1] is not None:
                        person_id_map[row[1]] = row[0]
            cursor_persons.close()
            conn_persons.close()
            print(f"OK - {len(person_id_map)} persons carregados")
            logger.info(f"Carregados {len(person_id_map)} persons para mapeamento")
        except Exception as e:
            logger.warning(f"Erro ao carregar persons: {e}")
            print(f"AVISO - Nao foi possivel carregar persons: {e}")
        
        print("\n[ETAPA 7] Limpando tabela contract_partners...")
        self.truncate_table('contract_partners')
        
        print("[ETAPA 7] Buscando dados do SQL Server...")
        sql_query = """
        SELECT DISTINCT
            v.IdOrcamento,
            v.IdClienteLoja,
            v.NomeClienteLoja,
            v.DataInclusaoOrcamento,
            v.DataAlteracaoOrcamento
        FROM ViewOrcamentosLojas v
        WHERE v.IdClienteLoja IS NOT NULL AND v.NomeClienteLoja IS NOT NULL
        ORDER BY v.IdOrcamento
        """
        
        if self.limit_rows > 0:
            sql_query = sql_query.replace("ORDER BY v.IdOrcamento", 
                f"ORDER BY v.IdOrcamento OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute(sql_query)
        
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        chunk_num = 0
        chunk_errors = 0
        
        try:
            while True:
                rows = cursor_sql.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                
                chunk_num += 1
                print(f"[ETAPA 7] Processando chunk {chunk_num} ({len(rows)} registros)...")
                logger.info(f"[ETAPA 7] Processando chunk {chunk_num} ({len(rows)} registros)...")
                
                for row in rows:
                    try:
                        partner_id = uuid.uuid4()
                        legado_id_orcamento = row[0]
                        legado_id_cliente_loja = row[1]
                        
                        contract_uuid = self.contract_id_map.get(legado_id_orcamento)
                        if not contract_uuid:
                            error_msg = f"Contract nao encontrado: IdOrcamento={legado_id_orcamento}"
                            logger.warning(error_msg)
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            continue
                        
                        person_uuid = person_id_map.get(legado_id_cliente_loja) if person_id_map else None
                        
                        insert_query = f"""
                        INSERT INTO {schema}.contract_partners (
                            id, contract_id, person_id, position, phone, email, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        cursor_pg.execute(insert_query, (
                            str(partner_id),
                            str(contract_uuid),
                            str(person_uuid) if person_uuid else None,
                            None,
                            None,
                            None,
                            row[3] if row[3] else datetime.now(),
                            row[4] if row[4] else datetime.now()
                        ))
                        
                        self.stats['contract_partners'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir contract_partner IdOrcamento={row[0]}: {e}"
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        continue
                
                conn_pg.commit()
                print(f"[ETAPA 7] Chunk {chunk_num} processado: {len(rows) - chunk_errors} registros inseridos, {chunk_errors} erros")
                logger.info(f"[ETAPA 7] Chunk {chunk_num}: {len(rows) - chunk_errors} inseridos, {chunk_errors} erros")
            
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\n[ETAPA 7] CONCLUIDA! Total de contract_partners migrados: {self.stats['contract_partners']}")
            logger.info(f"[ETAPA 7] CONCLUIDA! Total: {self.stats['contract_partners']}")
            
            self.validate_step7_contract_partners()
            
        except Exception as e:
            logger.error(f"Erro critico na ETAPA 7: {e}")
            if conn_pg:
                conn_pg.rollback()
                conn_pg.close()
            if conn_sql:
                conn_sql.close()
            raise
    
    def validate_step8_contract_additional_charges(self):
        """Validação e relatório de qualidade - ETAPA 8"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 8: CONTRACT_ADDITIONAL_CHARGES")
        print("-"*80)
        
        try:
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            if self.limit_rows > 0:
                cursor_sql.execute(f"""
                    SELECT 
                        SUM(CASE WHEN (ValorEPI > 0 OR CobrarEPI = 1) THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN ValorTradeMarketing > 0 THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN ValorOutros > 0 THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN Juros > 0 THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN Desconto > 0 THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN Multa > 0 THEN 1 ELSE 0 END)
                    FROM (
                        SELECT TOP {self.limit_rows} * FROM Orcamento ORDER BY Id
                    ) AS limited
                """)
            else:
                cursor_sql.execute("""
                    SELECT 
                        SUM(CASE WHEN (ValorEPI > 0 OR CobrarEPI = 1) THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN ValorTradeMarketing > 0 THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN ValorOutros > 0 THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN Juros > 0 THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN Desconto > 0 THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN Multa > 0 THEN 1 ELSE 0 END)
                    FROM Orcamento
                """)
            origem_total_esperado = cursor_sql.fetchone()[0] or 0
            
            cursor_sql.close()
            conn_sql.close()
            
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contract_additional_charges")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - Orcamento com valores adicionais):")
            print(f"  Total de registros esperados: {origem_total_esperado}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.contract_additional_charges):")
            print(f"  Total de registros: {destino_count}")
            
            diferenca = origem_total_esperado - destino_count
            
            if diferenca == 0:
                print(f"\nOK - Todos os registros foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 8: OK - Origem: {origem_total_esperado}, Destino: {destino_count}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} registros")
                logger.warning(f"VALIDACAO ETAPA 8: Diferenca - Origem: {origem_total_esperado}, Destino: {destino_count}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 8: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step8_migrate_contract_additional_charges(self):
        """ETAPA 8: Migrar contract_additional_charges"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 8: MIGRANDO CONTRACT_ADDITIONAL_CHARGES")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 8: Migrando contract_additional_charges")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        print("\n[ETAPA 8] Limpando tabela contract_additional_charges...")
        self.truncate_table('contract_additional_charges')
        
        print("[ETAPA 8] Buscando dados do SQL Server...")
        sql_query = """
        SELECT 
            Id,
            ValorEPI,
            ValorTradeMarketing,
            ValorOutros,
            CobrarEPI,
            InicioCobrancaEPI,
            Juros,
            Desconto,
            Multa,
            DataInclusao,
            DataAlteracao
        FROM Orcamento
        WHERE (ValorEPI > 0 OR CobrarEPI = 1)
           OR ValorTradeMarketing > 0
           OR ValorOutros > 0
           OR Juros > 0
           OR Desconto > 0
           OR Multa > 0
        ORDER BY Id
        """
        
        if self.limit_rows > 0:
            sql_query = sql_query.replace("ORDER BY Id", 
                f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute(sql_query)
        
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        chunk_num = 0
        chunk_errors = 0
        
        try:
            while True:
                rows = cursor_sql.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                
                chunk_num += 1
                print(f"[ETAPA 8] Processando chunk {chunk_num} ({len(rows)} registros)...")
                logger.info(f"[ETAPA 8] Processando chunk {chunk_num} ({len(rows)} registros)...")
                
                for row in rows:
                    try:
                        legado_id_orcamento = row[0]
                        
                        contract_uuid = self.contract_id_map.get(legado_id_orcamento)
                        if not contract_uuid:
                            error_msg = f"Contract nao encontrado: IdOrcamento={legado_id_orcamento}"
                            logger.warning(error_msg)
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            continue
                        
                        insert_query = f"""
                        INSERT INTO {schema}.contract_additional_charges (
                            id, contract_id, amount, charge_type, billing_model, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        # REGISTRO 1: EPI
                        if (row[1] and row[1] > 0) or (row[4] is True):
                            charge_id = uuid.uuid4()
                            # billing_model: se tem InicioCobrancaEPI, é recurring (cobrança recorrente), senão one_time
                            billing_model_epi = 'recurring' if row[5] else 'one_time'
                            cursor_pg.execute(insert_query, (
                                str(charge_id),
                                str(contract_uuid),
                                row[1] if row[1] else 0.0,
                                'epi',
                                billing_model_epi,
                                row[5] if row[5] else (row[9] if row[9] else datetime.now()),  # InicioCobrancaEPI ou DataInclusao
                                row[10] if row[10] else datetime.now()  # DataAlteracao
                            ))
                            self.stats['contract_additional_charges'] += 1
                        
                        # REGISTRO 2: Trade Marketing
                        if row[2] and row[2] > 0:
                            charge_id = uuid.uuid4()
                            cursor_pg.execute(insert_query, (
                                str(charge_id),
                                str(contract_uuid),
                                row[2],
                                'trade_marketing',
                                'recurring',
                                row[9] if row[9] else datetime.now(),
                                row[10] if row[10] else datetime.now()
                            ))
                            self.stats['contract_additional_charges'] += 1
                        
                        # REGISTRO 3: Outros
                        if row[3] and row[3] > 0:
                            charge_id = uuid.uuid4()
                            cursor_pg.execute(insert_query, (
                                str(charge_id),
                                str(contract_uuid),
                                row[3],
                                'others',
                                'one_time',
                                row[9] if row[9] else datetime.now(),
                                row[10] if row[10] else datetime.now()
                            ))
                            self.stats['contract_additional_charges'] += 1
                        
                        # REGISTRO 4: Juros
                        if row[6] and row[6] > 0:
                            charge_id = uuid.uuid4()
                            cursor_pg.execute(insert_query, (
                                str(charge_id),
                                str(contract_uuid),
                                row[6],
                                'interest',
                                'recurring',
                                row[9] if row[9] else datetime.now(),
                                row[10] if row[10] else datetime.now()
                            ))
                            self.stats['contract_additional_charges'] += 1
                        
                        # REGISTRO 5: Desconto
                        if row[7] and row[7] > 0:
                            charge_id = uuid.uuid4()
                            cursor_pg.execute(insert_query, (
                                str(charge_id),
                                str(contract_uuid),
                                row[7],
                                'discount',
                                'one_time',
                                row[9] if row[9] else datetime.now(),
                                row[10] if row[10] else datetime.now()
                            ))
                            self.stats['contract_additional_charges'] += 1
                        
                        # REGISTRO 6: Multa
                        if row[8] and row[8] > 0:
                            charge_id = uuid.uuid4()
                            cursor_pg.execute(insert_query, (
                                str(charge_id),
                                str(contract_uuid),
                                row[8],
                                'fine',
                                'one_time',
                                row[9] if row[9] else datetime.now(),
                                row[10] if row[10] else datetime.now()
                            ))
                            self.stats['contract_additional_charges'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir contract_additional_charge IdOrcamento={row[0]}: {e}"
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        continue
                
                conn_pg.commit()
                print(f"[ETAPA 8] Chunk {chunk_num} processado: {chunk_errors} erros")
                logger.info(f"[ETAPA 8] Chunk {chunk_num}: {chunk_errors} erros")
            
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\n[ETAPA 8] CONCLUIDA! Total de contract_additional_charges migrados: {self.stats['contract_additional_charges']}")
            logger.info(f"[ETAPA 8] CONCLUIDA! Total: {self.stats['contract_additional_charges']}")
            
            self.validate_step8_contract_additional_charges()
            
        except Exception as e:
            logger.error(f"Erro critico na ETAPA 8: {e}")
            if conn_pg:
                conn_pg.rollback()
                conn_pg.close()
            if conn_sql:
                conn_sql.close()
            raise
    
    def run(self):
        """Executa a migração completa"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        
        print("\n" + "="*80)
        print(f"INICIANDO MIGRACAO: SQL Server PRD -> PostgreSQL {destino} ({schema})")
        print("="*80)
        print(f"Data/Hora: {datetime.now()}")
        print("="*80)
        
        logger.info("="*80)
        logger.info("INICIANDO MIGRACAO COMPLETA")
        logger.info("="*80)
        logger.info(f"Data/Hora: {datetime.now()}")
        logger.info(f"Ambiente Destino: {destino}")
        logger.info(f"Schema Destino: {schema}")
        logger.info(f"Limite de Linhas: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info(f"Tamanho do Chunk: {CHUNK_SIZE}")
        logger.info(f"Origem: SQL Server PRD (Database: FINANCEIRO)")
        logger.info(f"Destino: PostgreSQL {destino} (Schema: {schema})")
        logger.info("="*80)
        
        start_time = datetime.now()
        
        try:
            self.step1_migrate_contracts()
            self.step2_migrate_contract_scenarios()
            self.step3_migrate_contract_scenario_stores()
            self.step4_migrate_contract_sellers()
            self.step5_migrate_contract_team_members()
            self.step6_migrate_contract_contacts()
            self.step7_migrate_contract_partners()
            self.step8_migrate_contract_additional_charges()
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            print("\n" + "="*80)
            print("MIGRACAO CONCLUIDA COM SUCESSO!")
            print("="*80)
            logger.info("="*80)
            logger.info("MIGRACAO CONCLUIDA COM SUCESSO!")
            logger.info("="*80)
            
            print(f"\nDuracao total: {duration}")
            print(f"\nESTATISTICAS FINAIS:")
            print(f"  Contracts: {self.stats['contracts']}")
            print(f"  Contract Scenarios: {self.stats['contract_scenarios']}")
            print(f"  Contract Scenario Stores: {self.stats['contract_scenario_stores']}")
            print(f"  Contract Sellers: {self.stats['contract_sellers']}")
            print(f"  Contract Team Members: {self.stats['contract_team_members']}")
            print(f"  Contract Contacts: {self.stats['contract_contacts']}")
            print(f"  Contract Partners: {self.stats['contract_partners']}")
            print(f"  Contract Additional Charges: {self.stats['contract_additional_charges']}")
            print(f"  Erros: {len(self.stats['errors'])}")
            
            logger.info(f"Duracao: {duration}")
            logger.info(f"Contracts: {self.stats['contracts']}")
            logger.info(f"Contract Scenarios: {self.stats['contract_scenarios']}")
            logger.info(f"Contract Scenario Stores: {self.stats['contract_scenario_stores']}")
            logger.info(f"Contract Sellers: {self.stats['contract_sellers']}")
            logger.info(f"Contract Team Members: {self.stats['contract_team_members']}")
            logger.info(f"Contract Contacts: {self.stats['contract_contacts']}")
            logger.info(f"Contract Partners: {self.stats['contract_partners']}")
            logger.info(f"Contract Additional Charges: {self.stats['contract_additional_charges']}")
            logger.info(f"Erros: {len(self.stats['errors'])}")
            
            if self.stats['errors']:
                print(f"\nAVISOS/ERROS ENCONTRADOS ({len(self.stats['errors'])}):")
                for i, error in enumerate(self.stats['errors'][:10], 1):
                    print(f"  {i}. {error}")
                if len(self.stats['errors']) > 10:
                    print(f"  ... e mais {len(self.stats['errors']) - 10} erros")
            
        except Exception as e:
            logger.error(f"ERRO CRITICO NA MIGRACAO: {e}")
            print(f"\nERRO CRITICO: {e}")
            raise
