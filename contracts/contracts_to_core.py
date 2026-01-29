"""
Script de migração de dados: SQL Server PRD -> PostgreSQL Destino
Migra dados das tabelas: contracts, contract_scenarios, contract_scenario_stores, 
contract_sellers, contract_team_members, contract_contacts, contract_partners, 
contract_additional_charges
"""

import sys
import os
import uuid
import json
import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from psycopg2.extras import execute_values

# Adicionar diretório utils ao path
utils_path = os.path.join(os.path.dirname(__file__), '..', 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)
# ⚠️ CRÍTICO: Importar usando o mesmo caminho do orchestrator para garantir mesma referência
from utils.database_connection import DatabaseConnection

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

# Schema PDV será determinado automaticamente baseado no destino configurado
# Em HML sempre usa prefixo "gm" antes do schema
def get_schema_pdv():
    """Retorna o schema PDV baseado no destino configurado"""
    destino = DatabaseConnection.get_destino()
    if destino == 'PRD':
        return 'pdv'
    else:
        return 'gmpdv'  # HML: prefixo "gm" + schema

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
CHUNK_SIZE = 20000

# ============================================================================
# FUNÇÕES COMPARTILHADAS PARA PROMOTER_TASKS (UNIFICADAS)
# ============================================================================

def normalize_promoter_task_name(nome_tarefa, nome_cliente):
    """
    Normaliza e constrói o name de promoter_task de forma consistente.
    Esta função DEVE ser usada tanto no step9 quanto no step2.
    
    Args:
        nome_tarefa: str ou None da ViewOrcamentosLojas.NomeTarefa
        nome_cliente: str ou None da ViewOrcamentosLojas.NomeCliente
    
    Returns:
        tuple: (name_normalizado, nome_tarefa_clean) ou (None, None) se NomeCliente inválido
    """
    # 1. Normalizar NomeTarefa
    if nome_tarefa:
        nome_tarefa_clean = str(nome_tarefa).strip().upper()
        if not nome_tarefa_clean or nome_tarefa_clean in ['NULL', 'NONE', '']:
            nome_tarefa_clean = None
    else:
        nome_tarefa_clean = None
    
    # 2. Normalizar NomeCliente (obrigatório)
    if nome_cliente:
        nome_cliente_clean = str(nome_cliente).strip()
        if not nome_cliente_clean or nome_cliente_clean in ['NULL', 'NONE', '']:
            nome_cliente_clean = None
    else:
        nome_cliente_clean = None
    
    # 3. Validar NomeCliente (obrigatório)
    if not nome_cliente_clean:
        # Se não tem cliente, não pode criar tarefa
        return None, None
    
    # 4. Construir name no formato padrão
    if nome_tarefa_clean:
        name = f"{nome_tarefa_clean} {nome_cliente_clean}"
    else:
        name = f"INDEFINIDA {nome_cliente_clean}"
    
    # 5. Remover espaços extras e normalizar
    name = ' '.join(name.split())  # Remove espaços múltiplos
    
    return name, nome_tarefa_clean


def get_task_type_from_nome_tarefa(nome_tarefa_upper):
    """
    Mapeia NomeTarefa para task_type usando o dicionário.
    Esta função DEVE ser usada tanto no step9 quanto no step2.
    
    Args:
        nome_tarefa_upper: str em UPPERCASE (já normalizado) ou None
    
    Returns:
        str: task_type ('undefined' se não encontrado)
    """
    task_type_mapping = {
        'CONTAGEM PDV': 'undefined',
        'DESRUPTURA': 'disruption',
        'ESTOQUE': 'undefined',
        'GALERIA DE FOTOS': 'undefined',
        'MANUTENÇÃO DE PONTO EXTRA': 'extra_point_maintenance',
        'MONTAGEM PONTO EXTRA': 'extra_point_assembly',
        'PESQUISA': 'undefined',
        'PESQUISA CONCORRENTE': 'undefined',
        'PESQUISA DE PREÇO': 'undefined',
        'PONTO EXTRA': 'undefined',
        'PRODUTO NA LOJA': 'undefined',
        'REABASTECIMENTO & RUPTURA': 'supply_and_disruption',
        'REGISTRO DE FOTOS': 'undefined',
        'SHARE GONDOLA': 'undefined',
        'VALIDADE': 'undefined'
    }
    
    if nome_tarefa_upper:
        return task_type_mapping.get(nome_tarefa_upper, 'Undefined')
    else:
        return 'Undefined'


class ContractsMigration:
    """Classe para executar a migração de dados de contracts"""
    
    def __init__(self, limit_rows=0, id_orcamento_filter=None, data_aviso_previo_min=None, 
                 data_inicio_operacao_max=None, clear_data=False):
        """
        Args:
            limit_rows: 0 = todos, > 0 = limitar quantidade
            id_orcamento_filter: Lista de IdOrcamento para filtrar (ex: [6192, 6193])
            data_aviso_previo_min: Data mínima para DataAvisoPrevio (datetime ou string 'YYYY-MM-DD')
            data_inicio_operacao_max: Data máxima para DataInicioOperacao (datetime ou string 'YYYY-MM-DD')
            clear_data: Se True, força TRUNCATE mesmo com filtros aplicados
        """
        self.stats = {
            'contracts': 0,
            'contract_scenarios': 0,
            'contract_scenario_stores': 0,
            'contract_sellers': 0,
            'contract_team_members': 0,
            'contract_contacts': 0,
            'contract_partners': 0,
            'contract_additional_charges': 0,
            'promoter_tasks': 0,
            'errors': []
        }
        self.contract_id_map = {}      # Map: legado_id (IdOrcamento) -> uuid
        self.customer_id_map = {}      # Map: legado_id -> uuid (carregado de customers)
        self.store_id_map = {}         # Map: legado_id -> uuid (carregado de stores)
        self.scenario_id_map = {}      # Map: legado_id (IdOrcamentoLoja) -> uuid
        self.promoter_task_map = {}    # Map: (IdTarefa, NomeTarefa_normalizado) -> uuid (promoter_tasks.id)
        self.limit_rows = limit_rows   # 0 = todos, > 0 = limitar quantidade
        
        # Filtros opcionais
        self.id_orcamento_filter = id_orcamento_filter if id_orcamento_filter else []
        self.data_aviso_previo_min = data_aviso_previo_min
        self.data_inicio_operacao_max = data_inicio_operacao_max
        self.clear_data = clear_data
        
        # Caminho do arquivo JSON de filtros
        self.filter_json_path = os.path.join(os.path.dirname(__file__), 'contracts_filter_main.json')
    
    def should_include_legacy_id(self):
        """Retorna True se deve incluir legacy_id (HML e PRD)"""
        # legacy_id existe tanto em HML quanto em PRD
        return True
    
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
    
    def has_filters(self):
        """Verifica se há filtros aplicados"""
        return (len(self.id_orcamento_filter) > 0 or 
                self.data_aviso_previo_min is not None or 
                self.data_inicio_operacao_max is not None or
                self.limit_rows > 0)
    
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
    
    def delete_table_with_filter(self, table_name: str, legacy_ids: List[int], schema: str = None):
        """
        Faz DELETE em uma tabela usando filtro de legacy_id.
        Não falha se não encontrar registros - apenas loga o total deletado.
        """
        if schema is None:
            schema = get_schema_atual()
        
        if not legacy_ids:
            logger.info(f"Nenhum legacy_id fornecido para DELETE em {table_name}")
            return
        
        conn = None
        try:
            conn = DatabaseConnection.get_postgresql_destino_connection()
            cursor = conn.cursor()
            
            query = f"DELETE FROM {schema}.{table_name} WHERE legacy_id = ANY(%s)"
            cursor.execute(query, (legacy_ids,))
            deleted_count = cursor.rowcount
            conn.commit()
            
            # Log apenas o total deletado (não precisa detalhar quais não existiam)
            logger.info(f"Tabela {schema}.{table_name}: {deleted_count} registros deletados (de {len(legacy_ids)} legacy_ids fornecidos)")
            print(f"OK - Tabela {schema}.{table_name}: {deleted_count} registros deletados")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Erro ao deletar registros de {schema}.{table_name}: {e}")
            if conn:
                conn.rollback()
                conn.close()
            raise
    
    def clean_table(self, table_name: str, legacy_ids: List[int] = None, schema: str = None):
        """
        Limpa tabela usando TRUNCATE ou DELETE baseado em filtros e flag --clear-data
        
        Args:
            table_name: Nome da tabela
            legacy_ids: Lista de legacy_ids para DELETE (se None e houver filtros, não limpa)
            schema: Schema (None = usar schema atual)
        """
        if self.clear_data:
            # Flag --clear-data: sempre TRUNCATE
            self.truncate_table(table_name, schema)
        elif self.has_filters() and legacy_ids:
            # Com filtros: usar DELETE
            self.delete_table_with_filter(table_name, legacy_ids, schema)
        elif not self.has_filters():
            # Sem filtros: usar TRUNCATE
            self.truncate_table(table_name, schema)
        else:
            # Tem filtros mas não tem legacy_ids: não limpar (segurança)
            logger.warning(f"Não foi possível limpar {table_name}: filtros aplicados mas legacy_ids não disponíveis")
            print(f"AVISO - Não foi possível limpar {table_name}: filtros aplicados mas legacy_ids não disponíveis")
    
    def save_filter_json(self, aggregated_ids: Dict[str, List]):
        """
        Salva arquivo JSON com filtros aplicados e IDs agregados
        
        Args:
            aggregated_ids: Dicionário com listas de IDs agregados
        """
        try:
            # Preparar estrutura do JSON
            filter_data = {
                'filters_applied': {
                    'id_orcamento': self.id_orcamento_filter,
                    'data_aviso_previo_min': str(self.data_aviso_previo_min) if self.data_aviso_previo_min else None,
                    'data_inicio_operacao_max': str(self.data_inicio_operacao_max) if self.data_inicio_operacao_max else None,
                    'limit_rows': self.limit_rows,
                    'clear_data': self.clear_data
                },
                'aggregated_ids': {
                    'IdOrcamento': sorted(aggregated_ids['IdOrcamento']),
                    'IdCliente': sorted([x for x in aggregated_ids['IdCliente'] if x is not None]),
                    'IdEstabelecimento': sorted([x for x in aggregated_ids['IdEstabelecimento'] if x is not None]),
                    'IdBandeira': sorted([x for x in aggregated_ids['IdBandeira'] if x is not None]),
                    'IdRede': sorted([x for x in aggregated_ids['IdRede'] if x is not None])
                },
                'execution_info': {
                    'timestamp': datetime.now().isoformat(),
                    'total_contracts_migrated': self.stats['contracts']
                }
            }
            
            # Salvar JSON
            with open(self.filter_json_path, 'w', encoding='utf-8') as f:
                json.dump(filter_data, f, indent=2, ensure_ascii=False)
            
            print(f"[ETAPA 1] Arquivo de filtros salvo: {self.filter_json_path}")
            logger.info(f"Arquivo de filtros salvo: {self.filter_json_path}")
            logger.info(f"Total de IdOrcamento: {len(filter_data['aggregated_ids']['IdOrcamento'])}")
            
        except Exception as e:
            logger.error(f"Erro ao salvar arquivo de filtros: {e}")
            print(f"AVISO - Erro ao salvar arquivo de filtros: {e}")
    
    def load_filter_json(self) -> Optional[Dict]:
        """
        Carrega arquivo JSON com filtros e IDs agregados
        
        Returns:
            Dicionário com dados do JSON ou None se arquivo não existir
        """
        try:
            if os.path.exists(self.filter_json_path):
                with open(self.filter_json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                logger.info(f"Arquivo de filtros carregado: {self.filter_json_path}")
                return data
            else:
                logger.warning(f"Arquivo de filtros não encontrado: {self.filter_json_path}")
                return None
        except Exception as e:
            logger.error(f"Erro ao carregar arquivo de filtros: {e}")
            return None
    
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
        return 'current'

        # # lógica anterior
        # if not modo_faturamento:
        #     return 'monthly'
        # modo = str(modo_faturamento).strip().upper()
        # if modo in ['M', 'MONTHLY', 'MENSAL']:
        #     return 'monthly'
        # elif modo in ['W', 'WEEKLY', 'SEMANAL']:
        #     return 'weekly'
        # elif modo in ['D', 'DAILY', 'DIARIO']:
        #     return 'daily'
        # else:
        #     return 'monthly'  # padrão
    
    def map_operation_type(self, tipo_orcamento: Optional[str]) -> str:
        """Mapeia TipoOrcamento para operation_type"""
        return 'shared'
        # # abaixo lógica antiga
        # if not tipo_orcamento:
        #     return 'standard'
        # tipo = str(tipo_orcamento).strip().upper()
        # # Mapear conforme valores possíveis
        # if tipo in ['ST', 'STANDARD', 'PADRAO']:
        #     return 'standard'
        # elif tipo in ['SP', 'SPECIAL', 'ESPECIAL']:
        #     return 'special'
        # else:
        #     return 'standard'  # padrão
    
    def map_thirteenth_salary_type(self, tipo_calculo: Optional[int]) -> str:
        """Mapeia TipoCalculoDecimoTerceiro para thirteenth_salary_type"""
        return 'monthly'

        # # lógica anterior
        # if tipo_calculo is None:
        #     return 'none'
        # # Mapear valores comuns
        # tipo_map = {
        #     0: 'none',
        #     1: 'proportional',
        #     2: 'full',
        #     3: 'custom'
        # }
        # return tipo_map.get(tipo_calculo, 'none')
    
    def map_trade_type(self, trade_marketing: Optional[str]) -> str:
        """Mapeia TradeMarketing para trade_type"""
        return 'shared'

        # # lógica anterior
        # if not trade_marketing:
        #     return 'none'
        # trade = str(trade_marketing).strip().upper()
        # if trade in ['N', 'NONE', 'NAO', 'NÃO']:
        #     return 'none'
        # elif trade in ['S', 'SIM', 'YES']:
        #     return 'yes'
        # else:
        #     return 'none'  # padrão
    
    def _get_filter_ids_for_validation(self):
        """
        Retorna os IDs filtrados para validação (do JSON ou filtros aplicados)
        Retorna: dict com IdOrcamento, IdOrcamentoLoja, etc.
        """
        filter_data = self.load_filter_json()
        if filter_data and 'aggregated_ids' in filter_data:
            return filter_data['aggregated_ids']
        return {}
    
    def validate_step1_contracts(self):
        """Validação e relatório de qualidade - ETAPA 1"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 1: CONTRACTS")
        print("-"*80)
        
        try:
            # Contar origem aplicando os mesmos filtros usados na migração
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            # Construir query de contagem com os mesmos filtros
            count_query = """
            SELECT COUNT(DISTINCT v.IdOrcamento)
            FROM ViewOrcamentosLojas v
            INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
            """
            
            # Aplicar os mesmos filtros usados na migração
            where_conditions = []
            query_params = []
            
            if len(self.id_orcamento_filter) > 0:
                placeholders = ','.join(['?' for _ in self.id_orcamento_filter])
                where_conditions.append(f"v.IdOrcamento IN ({placeholders})")
                query_params.extend(self.id_orcamento_filter)
            
            if self.data_aviso_previo_min is not None:
                if isinstance(self.data_aviso_previo_min, str):
                    data_aviso_previo_str = self.data_aviso_previo_min
                else:
                    data_aviso_previo_str = self.data_aviso_previo_min.strftime('%Y-%m-%d')
                where_conditions.append("(CONVERT(DATE, v.DataAvisoPrevio) >= ? OR v.DataAvisoPrevio IS NULL)")
                query_params.append(data_aviso_previo_str)
            
            if self.data_inicio_operacao_max is not None:
                if isinstance(self.data_inicio_operacao_max, str):
                    data_inicio_str = self.data_inicio_operacao_max
                else:
                    data_inicio_str = self.data_inicio_operacao_max.strftime('%Y-%m-%d')
                where_conditions.append("CONVERT(DATE, v.DataInicioOperacao) <= ?")
                query_params.append(data_inicio_str)
            
            if where_conditions:
                count_query += " WHERE " + " AND ".join(where_conditions)
            
            # Aplicar limite se especificado
            if self.limit_rows > 0:
                count_query = f"""
                SELECT COUNT(DISTINCT IdOrcamento)
                FROM (
                    SELECT DISTINCT TOP {self.limit_rows} v.IdOrcamento
                    FROM ViewOrcamentosLojas v
                    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                    {('WHERE ' + ' AND '.join(where_conditions)) if where_conditions else ''}
                    ORDER BY v.IdOrcamento
                ) AS limited
                """
                if query_params:
                    cursor_sql.execute(count_query, query_params)
                else:
                    cursor_sql.execute(count_query)
            else:
                if query_params:
                    cursor_sql.execute(count_query, query_params)
                else:
                    cursor_sql.execute(count_query)
            
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino aplicando os mesmos filtros (via legacy_id)
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            # Se há filtros aplicados, contar apenas os registros que correspondem aos filtros
            if self.has_filters():
                # Carregar o JSON para pegar os IdOrcamento migrados nesta execução
                filter_data = self.load_filter_json()
                if filter_data and 'aggregated_ids' in filter_data:
                    id_orcamento_migrados = filter_data['aggregated_ids'].get('IdOrcamento', [])
                    if id_orcamento_migrados:
                        cursor_pg.execute(
                            f"SELECT COUNT(*) FROM {schema}.contracts WHERE legacy_id = ANY(%s)",
                            (id_orcamento_migrados,)
                        )
                        destino_count = cursor_pg.fetchone()[0]
                    else:
                        # Se não há IdOrcamento no JSON, contar todos (fallback)
                        cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contracts")
                        destino_count = cursor_pg.fetchone()[0]
                else:
                    # Se não há JSON, contar todos (fallback)
                    cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contracts")
                    destino_count = cursor_pg.fetchone()[0]
            else:
                # Sem filtros: contar todos os registros
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
        
        # Buscar dados do SQL Server primeiro para identificar quais customers são necessários
        print("[ETAPA 1] Buscando dados do SQL Server para identificar customers necessários...")
        sql_query_preview = """
        SELECT 
            v.IdOrcamento,
            MAX(v.IdCliente) AS IdCliente
        FROM ViewOrcamentosLojas v
        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
        """
        
        # Construir WHERE clause com filtros opcionais (mesma lógica do query principal)
        where_conditions_preview = []
        query_params_preview = []
        
        if len(self.id_orcamento_filter) > 0:
            placeholders = ','.join(['?' for _ in self.id_orcamento_filter])
            where_conditions_preview.append(f"v.IdOrcamento IN ({placeholders})")
            query_params_preview.extend(self.id_orcamento_filter)
        
        if self.data_aviso_previo_min is not None:
            if isinstance(self.data_aviso_previo_min, str):
                data_aviso_previo_str = self.data_aviso_previo_min
            else:
                data_aviso_previo_str = self.data_aviso_previo_min.strftime('%Y-%m-%d')
            where_conditions_preview.append("(CONVERT(DATE, v.DataAvisoPrevio) >= ? OR v.DataAvisoPrevio IS NULL)")
            query_params_preview.append(data_aviso_previo_str)
        
        if self.data_inicio_operacao_max is not None:
            if isinstance(self.data_inicio_operacao_max, str):
                data_inicio_str = self.data_inicio_operacao_max
            else:
                data_inicio_str = self.data_inicio_operacao_max.strftime('%Y-%m-%d')
            where_conditions_preview.append("CONVERT(DATE, v.DataInicioOperacao) <= ?")
            query_params_preview.append(data_inicio_str)
        
        if where_conditions_preview:
            sql_query_preview += " WHERE " + " AND ".join(where_conditions_preview)
        
        sql_query_preview += """
        GROUP BY v.IdOrcamento
        """
        
        if self.limit_rows > 0:
            sql_query_preview += f" ORDER BY v.IdOrcamento OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY"
        
        conn_sql_preview = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql_preview = conn_sql_preview.cursor()
        
        if query_params_preview:
            cursor_sql_preview.execute(sql_query_preview, query_params_preview)
        else:
            cursor_sql_preview.execute(sql_query_preview)
        
        preview_rows = cursor_sql_preview.fetchall()
        cursor_sql_preview.close()
        conn_sql_preview.close()
        
        # Extrair IdCliente únicos necessários e preparar dados agregados para JSON
        id_cliente_necessarios = list(set([row[1] for row in preview_rows if row[1] is not None]))
        id_orcamento_preview = [row[0] for row in preview_rows]
        
        # Preparar aggregated_ids para gerar JSON antes de executar customers
        aggregated_ids_preview = {
            'IdOrcamento': sorted(id_orcamento_preview),
            'IdCliente': sorted(id_cliente_necessarios),
            'IdEstabelecimento': [],
            'IdBandeira': [],
            'IdRede': []
        }
        
        # Gerar JSON temporário ANTES de executar customers (para que customers possa ler)
        temp_filter_data = {
            'filters_applied': {
                'id_orcamento': self.id_orcamento_filter,
                'data_aviso_previo_min': str(self.data_aviso_previo_min) if self.data_aviso_previo_min else None,
                'data_inicio_operacao_max': str(self.data_inicio_operacao_max) if self.data_inicio_operacao_max else None,
                'limit_rows': self.limit_rows,
                'clear_data': self.clear_data
            },
            'aggregated_ids': aggregated_ids_preview,
            'execution_info': {
                'timestamp': datetime.now().isoformat(),
                'total_contracts_migrated': 0
            }
        }
        temp_json_path = os.path.join(os.path.dirname(__file__), 'contracts_filter_main.json')
        with open(temp_json_path, 'w', encoding='utf-8') as f:
            json.dump(temp_filter_data, f, indent=2, ensure_ascii=False)
        
        # Carregar mapeamento de customers existentes
        print("[ETAPA 1] Carregando mapeamento de customers existentes...")
        schema_customers = 'gmcore' if destino == 'HML' else 'core'
        # ⚠️ CRÍTICO: Usar conexão PRD diretamente quando destino for PRD
        if destino == 'PRD':
            conn_customers = DatabaseConnection.get_postgresql_prd_destino_connection()
        else:
            conn_customers = DatabaseConnection.get_postgresql_hml_destino_connection()
        cursor_customers = conn_customers.cursor()
        cursor_customers.execute(f"SELECT id, legacy_id FROM {schema_customers}.customers WHERE legacy_id IS NOT NULL")
        for row in cursor_customers.fetchall():
            if row[1] is not None:
                self.customer_id_map[row[1]] = row[0]
        cursor_customers.close()
        conn_customers.close()
        print(f"OK - {len(self.customer_id_map)} customers existentes carregados")
        
        # Verificar quais customers estão faltando
        customers_faltantes = [cid for cid in id_cliente_necessarios if cid not in self.customer_id_map]
        
        # Se houver customers faltantes, executar customers step2 ANTES de processar contracts
        if customers_faltantes:
            print(f"\n[ETAPA 1] Detectados {len(customers_faltantes)} customers faltantes.")
            print("[ETAPA 1] Executando customers step2 ANTES de processar contracts...")
            logger.info(f"[ETAPA 1] Executando customers step2 para sincronizar {len(customers_faltantes)} customers faltantes")
            
            try:
                # Importar e executar customers step2 (ele vai ler o JSON que acabamos de gerar)
                customers_path = os.path.join(os.path.dirname(__file__), '..', 'customers')
                if customers_path not in sys.path:
                    sys.path.insert(0, customers_path)
                from customers_to_core import CustomersMigration
                
                customers_migration = CustomersMigration(limit_rows=self.limit_rows)
                customers_migration.step2_migrate_customers()
                
                # Recarregar mapeamento de customers após migração
                print("[ETAPA 1] Recarregando mapeamento de customers após migração...")
                # ⚠️ CRÍTICO: Usar conexão PRD diretamente quando destino for PRD
                if destino == 'PRD':
                    conn_customers = DatabaseConnection.get_postgresql_prd_destino_connection()
                else:
                    conn_customers = DatabaseConnection.get_postgresql_hml_destino_connection()
                cursor_customers = conn_customers.cursor()
                cursor_customers.execute(f"SELECT id, legacy_id FROM {schema_customers}.customers WHERE legacy_id IS NOT NULL")
                for row in cursor_customers.fetchall():
                    if row[1] is not None:
                        self.customer_id_map[row[1]] = row[0]
                cursor_customers.close()
                conn_customers.close()
                print(f"OK - {len(self.customer_id_map)} customers carregados após migração")
                logger.info(f"[ETAPA 1] Customers step2 executado. {len(self.customer_id_map)} customers disponíveis")
                
            except Exception as e:
                logger.error(f"Erro ao executar customers step2: {e}")
                print(f"ERRO - Não foi possível executar customers step2: {e}")
                print("[ETAPA 1] Execute manualmente: python orchestrator_tasks.py customers 2")
                raise
        
        # Limpar tabela (TRUNCATE ou DELETE baseado em filtros)
        # Se não há filtros ou clear_data está ativo: fazer TRUNCATE antes
        # Se há filtros: buscar dados primeiro, depois fazer DELETE
        if not self.has_filters() or self.clear_data:
            print("\n[ETAPA 1] Limpando tabela contracts...")
            self.clean_table('contracts')
        
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
            MAX(o.TradeMarketing) AS TradeMarketing,
            MAX(v.IdEstabelecimento) AS IdEstabelecimento,
            MAX(v.IdBandeira) AS IdBandeira,
            MAX(v.IdRede) AS IdRede,
            MAX(v.DataAvisoPrevio) AS DataAvisoPrevio
        FROM ViewOrcamentosLojas v
        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
        """
        
        # Construir WHERE clause com filtros opcionais
        where_conditions = []
        query_params = []
        
        # Filtro IdOrcamento
        if len(self.id_orcamento_filter) > 0:
            placeholders = ','.join(['?' for _ in self.id_orcamento_filter])
            where_conditions.append(f"v.IdOrcamento IN ({placeholders})")
            query_params.extend(self.id_orcamento_filter)
        
        # Filtro DataAvisoPrevio (data mínima)
        if self.data_aviso_previo_min is not None:
            if isinstance(self.data_aviso_previo_min, str):
                data_aviso_previo_str = self.data_aviso_previo_min
            else:
                data_aviso_previo_str = self.data_aviso_previo_min.strftime('%Y-%m-%d')
            where_conditions.append("(CONVERT(DATE, v.DataAvisoPrevio) >= ? OR v.DataAvisoPrevio IS NULL)")
            query_params.append(data_aviso_previo_str)
        
        # Filtro DataInicioOperacao (data máxima)
        if self.data_inicio_operacao_max is not None:
            if isinstance(self.data_inicio_operacao_max, str):
                data_inicio_str = self.data_inicio_operacao_max
            else:
                data_inicio_str = self.data_inicio_operacao_max.strftime('%Y-%m-%d')
            where_conditions.append("CONVERT(DATE, v.DataInicioOperacao) <= ?")
            query_params.append(data_inicio_str)
        
        # Adicionar WHERE se houver condições
        if where_conditions:
            sql_query += " WHERE " + " AND ".join(where_conditions)
        
        sql_query += """
        GROUP BY v.IdOrcamento
        ORDER BY v.IdOrcamento
        """
        
        # Adicionar LIMIT se especificado
        if self.limit_rows > 0:
            sql_query = sql_query.replace("ORDER BY v.IdOrcamento", 
                f"ORDER BY v.IdOrcamento OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
        # Log dos filtros aplicados
        if self.has_filters():
            logger.info(f"Filtros aplicados: IdOrcamento={self.id_orcamento_filter}, "
                       f"DataAvisoPrevio_min={self.data_aviso_previo_min}, "
                       f"DataInicioOperacao_max={self.data_inicio_operacao_max}, "
                       f"limit_rows={self.limit_rows}, clear_data={self.clear_data}")
            print(f"[ETAPA 1] Filtros aplicados: IdOrcamento={self.id_orcamento_filter}, "
                  f"DataAvisoPrevio_min={self.data_aviso_previo_min}, "
                  f"DataInicioOperacao_max={self.data_inicio_operacao_max}")
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        
        # Executar query com parâmetros se houver
        if query_params:
            cursor_sql.execute(sql_query, query_params)
        else:
            cursor_sql.execute(sql_query)
        
        # Carregar TODOS os dados na memória de uma vez (otimizado)
        print("[ETAPA 1] Carregando dados na memória...")
        all_rows = cursor_sql.fetchall()
        cursor_sql.close()
        conn_sql.close()
        
        print(f"[ETAPA 1] {len(all_rows)} registros carregados. Processando conversões...")
        
        # ⚠️ LÓGICA SIMPLIFICADA: Query direta sem subquery (conforme validação)
        # Coletar TODOS os IDs únicos da ViewOrcamentosLojas usando queries separadas para cada tipo
        print("[ETAPA 1] Coletando IDs únicos da ViewOrcamentosLojas...")
        
        # Construir WHERE clause comum para todas as queries
        where_clause_ids = ""
        query_params_ids = []
        
        if self.limit_rows > 0:
            # Quando há LIMIT, primeiro identificar quais IdOrcamento serão migrados
            id_orcamento_migrados = [row[0] for row in all_rows]  # IdOrcamento da query principal (já com LIMIT aplicado)
            
            if id_orcamento_migrados:
                placeholders = ','.join(['?' for _ in id_orcamento_migrados])
                where_conditions_ids = [f"v.IdOrcamento IN ({placeholders})"]
                query_params_ids = list(id_orcamento_migrados)
                
                # Aplicar os mesmos filtros adicionais (DataAvisoPrevio, DataInicioOperacao) se existirem
                if self.data_aviso_previo_min is not None:
                    if isinstance(self.data_aviso_previo_min, str):
                        data_aviso_previo_str = self.data_aviso_previo_min
                    else:
                        data_aviso_previo_str = self.data_aviso_previo_min.strftime('%Y-%m-%d')
                    where_conditions_ids.append("(CONVERT(DATE, v.DataAvisoPrevio) >= ? OR v.DataAvisoPrevio IS NULL)")
                    query_params_ids.append(data_aviso_previo_str)
                
                if self.data_inicio_operacao_max is not None:
                    if isinstance(self.data_inicio_operacao_max, str):
                        data_inicio_str = self.data_inicio_operacao_max
                    else:
                        data_inicio_str = self.data_inicio_operacao_max.strftime('%Y-%m-%d')
                    where_conditions_ids.append("CONVERT(DATE, v.DataInicioOperacao) <= ?")
                    query_params_ids.append(data_inicio_str)
                
                where_clause_ids = "WHERE " + " AND ".join(where_conditions_ids)
                print(f"[ETAPA 1] Coletando TODOS os IDs únicos para {len(id_orcamento_migrados)} contratos: {id_orcamento_migrados[:5]}{'...' if len(id_orcamento_migrados) > 5 else ''}")
            else:
                where_clause_ids = "WHERE 1=0"
                query_params_ids = []
        else:
            # Sem LIMIT, usar os mesmos filtros da query principal
            if where_conditions:
                where_clause_ids = "WHERE " + " AND ".join(where_conditions)
            query_params_ids = query_params
            print(f"[ETAPA 1] Coletando IDs únicos sem LIMIT (aplicando filtros)")
            logger.info(f"[ETAPA 1] Filtros aplicados: {where_conditions}")
        
        conn_sql_ids = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql_ids = conn_sql_ids.cursor()
        
        # Executar query separada para cada tipo de ID usando estrutura simplificada
        aggregated_ids = {}
        
        # IdOrcamento
        query_id_orcamento = f"""
        SELECT DISTINCT
            v.IdOrcamento
        FROM ViewOrcamentosLojas v
        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
        {where_clause_ids} AND v.IdOrcamento IS NOT NULL
        """
        if query_params_ids:
            cursor_sql_ids.execute(query_id_orcamento, query_params_ids)
        else:
            cursor_sql_ids.execute(query_id_orcamento)
        aggregated_ids['IdOrcamento'] = [row[0] for row in cursor_sql_ids.fetchall()]
        
        # IdCliente
        query_id_cliente = f"""
        SELECT DISTINCT
            v.IdCliente
        FROM ViewOrcamentosLojas v
        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
        {where_clause_ids} AND v.IdCliente IS NOT NULL
        """
        if query_params_ids:
            cursor_sql_ids.execute(query_id_cliente, query_params_ids)
        else:
            cursor_sql_ids.execute(query_id_cliente)
        aggregated_ids['IdCliente'] = [row[0] for row in cursor_sql_ids.fetchall()]
        
        # IdEstabelecimento
        query_id_estabelecimento = f"""
        SELECT DISTINCT
            v.IdEstabelecimento
        FROM ViewOrcamentosLojas v
        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
        {where_clause_ids} AND v.IdEstabelecimento IS NOT NULL
        """
        if query_params_ids:
            cursor_sql_ids.execute(query_id_estabelecimento, query_params_ids)
        else:
            cursor_sql_ids.execute(query_id_estabelecimento)
        aggregated_ids['IdEstabelecimento'] = [row[0] for row in cursor_sql_ids.fetchall()]
        
        # IdBandeira
        query_id_bandeira = f"""
        SELECT DISTINCT
            v.IdBandeira
        FROM ViewOrcamentosLojas v
        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
        {where_clause_ids} AND v.IdBandeira IS NOT NULL
        """
        if query_params_ids:
            cursor_sql_ids.execute(query_id_bandeira, query_params_ids)
        else:
            cursor_sql_ids.execute(query_id_bandeira)
        aggregated_ids['IdBandeira'] = [row[0] for row in cursor_sql_ids.fetchall()]
        
        # IdRede
        query_id_rede = f"""
        SELECT DISTINCT
            v.IdRede
        FROM ViewOrcamentosLojas v
        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
        {where_clause_ids} AND v.IdRede IS NOT NULL
        """
        if query_params_ids:
            cursor_sql_ids.execute(query_id_rede, query_params_ids)
        else:
            cursor_sql_ids.execute(query_id_rede)
        aggregated_ids['IdRede'] = [row[0] for row in cursor_sql_ids.fetchall()]
        
        cursor_sql_ids.close()
        conn_sql_ids.close()
        
        # Log dos resultados
        logger.info(f"[ETAPA 1] Query de coleta de IDs (estrutura simplificada)")
        logger.info(f"[ETAPA 1] Parâmetros da query: {query_params_ids}")
        logger.info(f"[ETAPA 1] Exemplo de query (IdEstabelecimento): {query_id_estabelecimento[:200]}...")
        
        print(f"[ETAPA 1] IDs únicos coletados: {len(aggregated_ids['IdOrcamento'])} contratos, "
              f"{len(aggregated_ids['IdEstabelecimento'])} estabelecimentos, "
              f"{len(aggregated_ids['IdBandeira'])} bandeiras, "
              f"{len(aggregated_ids['IdRede'])} redes")
        logger.info(f"[ETAPA 1] IDs únicos coletados - Contratos: {len(aggregated_ids['IdOrcamento'])}, "
                   f"Estabelecimentos: {len(aggregated_ids['IdEstabelecimento'])}, "
                   f"Bandeiras: {len(aggregated_ids['IdBandeira'])}, "
                   f"Redes: {len(aggregated_ids['IdRede'])}")
        
        # Log detalhado dos IDs coletados
        if aggregated_ids['IdEstabelecimento']:
            print(f"[ETAPA 1] IdEstabelecimento coletados: {aggregated_ids['IdEstabelecimento'][:10]}{'...' if len(aggregated_ids['IdEstabelecimento']) > 10 else ''}")
        if aggregated_ids['IdBandeira']:
            print(f"[ETAPA 1] IdBandeira coletados: {aggregated_ids['IdBandeira'][:10]}{'...' if len(aggregated_ids['IdBandeira']) > 10 else ''}")
        if aggregated_ids['IdRede']:
            print(f"[ETAPA 1] IdRede coletados: {aggregated_ids['IdRede']}")
        
        # ⚠️ IMPORTANTE: Gerar JSON ANTES de migrar os dados
        # Isso garante que outros módulos (stores, customers) possam ler o JSON correto
        print("[ETAPA 1] Gerando arquivo JSON com filtros e IDs agregados (ANTES da migração)...")
        self.save_filter_json(aggregated_ids)
        print("[ETAPA 1] Arquivo JSON gerado com sucesso. Outros módulos podem ler os filtros corretos.")
        
        # Se há filtros e não é clear_data: fazer DELETE antes de inserir
        if self.has_filters() and not self.clear_data:
            legacy_ids_to_delete = [row[0] for row in all_rows]  # IdOrcamento
            if legacy_ids_to_delete:
                print("\n[ETAPA 1] Limpando registros filtrados da tabela contracts...")
                self.delete_table_with_filter('contracts', legacy_ids_to_delete)
        
        # Processar tudo em memória e preparar batch_values
        batch_values = []
        legacy_ids_list = []
        
        for row in all_rows:
            try:
                legado_id = row[0]  # IdOrcamento
                customer_legacy_id = row[1]  # IdCliente
                
                # Mapear customer_id
                customer_uuid = self.customer_id_map.get(customer_legacy_id)
                if not customer_uuid:
                    # Não logar individualmente para não prejudicar performance
                    # Apenas contar o erro e continuar
                    self.stats['errors'].append(f"Customer nao encontrado: IdCliente={customer_legacy_id} para Orcamento Id={legado_id}")
                    continue
                
                # Preparar valores
                billing_day = row[2] if row[2] is not None else 1
                due_day = row[3] if row[3] is not None else 1
                billing_type = self.map_billing_type(row[8])  # ModoFaturamento
                operation_type = self.map_operation_type(row[9])  # TipoOrcamento
                thirteenth_salary_type = self.map_thirteenth_salary_type(row[10])  # TipoCalculoDecimoTerceiro
                trade_type = self.map_trade_type(row[11])  # TradeMarketing
                start_date = row[5] if row[5] is not None else row[6]  # DataInicioOperacao ou DataInclusaoOrcamento como fallback
                if start_date is None:
                    start_date = datetime.now()  # Se ainda for None, usar data atual
                # anterior
                # status = self.convert_status_pedido(row[4])  # StatusPedido
                status = 'active'
                created_at = row[6] if row[6] else datetime.now()  # DataInclusaoOrcamento
                updated_at = row[7] if row[7] else datetime.now()  # DataAlteracaoOrcamento
                
                if include_legacy:
                    batch_values.append((
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
                    legacy_ids_list.append(legado_id)
                else:
                    batch_values.append((
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
                    legacy_ids_list.append(legado_id)
                
            except Exception as e:
                error_msg = f"Erro ao preparar contract IdOrcamento={row[0]}: {e}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)
                continue
        
        print(f"[ETAPA 1] {len(batch_values)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL e inserir em bulk
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Query de insert usando gen_random_uuid() - formato para execute_values
        if include_legacy:
            insert_query = f"""
            INSERT INTO {schema}.contracts (
                id, legacy_id, customer_id, billing_day, due_day,
                billing_type, operation_type, thirteenth_salary_type, trade_type,
                start_date, status, deleted_at, created_at, updated_at
            ) VALUES %s
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        else:
            insert_query = f"""
            INSERT INTO {schema}.contracts (
                id, customer_id, billing_day, due_day,
                billing_type, operation_type, thirteenth_salary_type, trade_type,
                start_date, status, deleted_at, created_at, updated_at
            ) VALUES %s
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        
        chunk_num = 0
        total_processed = 0
        all_legacy_ids_inserted = []
        
        try:
            for i in range(0, len(batch_values), CHUNK_SIZE):
                chunk = batch_values[i:i + CHUNK_SIZE]
                chunk_num += 1
                
                if chunk:
                    try:
                        # Usar execute_values() para inserção otimizada em bulk
                        execute_values(
                            cursor_pg,
                            insert_query,
                            chunk,
                            template=insert_template,
                            page_size=CHUNK_SIZE,
                            fetch=False
                        )
                        
                        # Coletar legacy_ids para lookup depois (se necessário)
                        chunk_legacy_ids = legacy_ids_list[i:i + CHUNK_SIZE]
                        all_legacy_ids_inserted.extend(chunk_legacy_ids)
                        
                        total_processed += len(chunk)
                        self.stats['contracts'] += len(chunk)
                        
                        conn_pg.commit()
                        print(f"[ETAPA 1] Chunk {chunk_num} processado: {total_processed}/{len(batch_values)} registros inseridos")
                        logger.info(f"[ETAPA 1] Chunk {chunk_num} processado: {total_processed}/{len(batch_values)} registros inseridos")
                        
                    except Exception as batch_error:
                        error_msg = f"Erro ao inserir batch de contracts: {batch_error}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
            
            # Buscar UUIDs gerados para mapeamento (uma única query após todas as inserções)
            if include_legacy and all_legacy_ids_inserted:
                print(f"[ETAPA 1] Buscando UUIDs gerados para {len(all_legacy_ids_inserted)} registros...")
                cursor_pg.execute(f"""
                    SELECT id, legacy_id 
                    FROM {schema}.contracts 
                    WHERE legacy_id = ANY(%s)
                """, (all_legacy_ids_inserted,))
                for uuid_row, leg_id in cursor_pg.fetchall():
                    self.contract_id_map[leg_id] = uuid_row
                print(f"[ETAPA 1] {len(self.contract_id_map)} UUIDs mapeados")
            
            cursor_pg.close()
            conn_pg.close()
            
            # Log de resumo: total de novos registros inseridos
            total_inseridos = self.stats['contracts']
            total_processados = len(all_rows)
            total_erros = len(self.stats['errors'])
            
            print(f"\n[ETAPA 1] CONCLUIDA!")
            print(f"  Total de novos registros inseridos: {total_inseridos}")
            print(f"  Total de registros processados: {total_processados}")
            if total_erros > 0:
                print(f"  Total de erros (registros não migrados): {total_erros}")
                # Agrupar erros por tipo para mostrar resumo
                error_summary = {}
                for error in self.stats['errors']:
                    # Extrair tipo de erro (primeira palavra antes dos dois pontos)
                    error_type = error.split(':')[0] if ':' in error else 'Erro desconhecido'
                    error_summary[error_type] = error_summary.get(error_type, 0) + 1
                
                print(f"  Resumo de erros na ETAPA 1:")
                for error_type, count in error_summary.items():
                    print(f"    - {error_type}: {count} ocorrência(s)")
            
            logger.info(f"[ETAPA 1] CONCLUIDA! Total inseridos: {total_inseridos}, Processados: {total_processados}, Erros: {total_erros}")
            
            # ⚠️ NOTA: O JSON já foi gerado ANTES da migração (linha ~898)
            # Não é necessário gerar novamente aqui
            
            # Validação
            self.validate_step1_contracts()
            
        except Exception as e:
            logger.error(f"Erro critico na ETAPA 1: {e}")
            if 'conn_pg' in locals():
                try:
                    conn_pg.rollback()
                    if 'cursor_pg' in locals():
                        cursor_pg.close()
                    conn_pg.close()
                except:
                    pass
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
    
    # ========================================================================
    # MÉTODOS VETORIZADOS COM PANDAS
    # ========================================================================
    
    def clean_string_vectorized(self, series: pd.Series, max_length: Optional[int] = None) -> pd.Series:
        """Limpa e trunca strings de forma vetorizada"""
        cleaned = series.astype(str)
        cleaned = cleaned.replace(['nan', 'None', 'NULL', 'NONE'], '')
        cleaned = cleaned.str.strip()
        cleaned = cleaned.replace(['', 'null', 'none'], None)
        if max_length:
            cleaned = cleaned.str[:max_length]
        return cleaned
    
    def convert_hours_to_float_vectorized(self, series: pd.Series) -> pd.Series:
        """Converte Horas (nvarchar) para double precision de forma vetorizada"""
        def convert_single(val):
            if pd.isna(val) or val is None:
                return None
            try:
                horas_clean = str(val).strip().replace(',', '.')
                if not horas_clean or horas_clean.lower() in ['null', 'none', '']:
                    return None
                return float(horas_clean)
            except (ValueError, AttributeError):
                return None
        return series.apply(convert_single)
    
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
            # Carregar filtros do step1
            filter_ids = self._get_filter_ids_for_validation()
            id_orcamento_list = filter_ids.get('IdOrcamento', [])
            
            # Contar origem aplicando os mesmos filtros do step1
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            if id_orcamento_list:
                # Aplicar filtro de IdOrcamento - contar TODOS os registros (não apenas DISTINCT)
                placeholders = ','.join(['?' for _ in id_orcamento_list])
                count_query = f"""
                SELECT COUNT(*)
                FROM ViewOrcamentosLojas v
                LEFT JOIN OrcamentoLojas ol ON ol.Id = v.IdOrcamentoLoja
                WHERE v.IdOrcamento IN ({placeholders})
                """
                cursor_sql.execute(count_query, id_orcamento_list)
            elif self.limit_rows > 0:
                cursor_sql.execute(f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT TOP {self.limit_rows} v.IdOrcamentoLoja
                        FROM ViewOrcamentosLojas v
                        LEFT JOIN OrcamentoLojas ol ON ol.Id = v.IdOrcamentoLoja
                        ORDER BY v.IdOrcamentoLoja
                    ) AS limited
                """)
            else:
                cursor_sql.execute("SELECT COUNT(*) FROM ViewOrcamentosLojas v LEFT JOIN OrcamentoLojas ol ON ol.Id = v.IdOrcamentoLoja")
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino aplicando os mesmos filtros
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            if id_orcamento_list:
                # Contar apenas os contract_scenarios relacionados aos contracts migrados
                cursor_pg.execute(f"""
                    SELECT COUNT(*) 
                    FROM {schema}.contract_scenarios cs
                    INNER JOIN {schema}.contracts c ON c.id = cs.contract_id
                    WHERE c.legacy_id = ANY(%s)
                """, (id_orcamento_list,))
            else:
                cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contract_scenarios")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - ViewOrcamentosLojas):")
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
    
    def translate_task_name_to_english(self, nome_tarefa: Optional[str]) -> str:
        """Traduz nome de tarefa do português para inglês em snake_case"""
        if not nome_tarefa:
            return "standard_task"
        
        nome_tarefa = str(nome_tarefa).strip().upper()
        
        # Dicionário de traduções conhecidas
        translations = {
            'REABASTECIMENTO': 'supply_and_disruption',
            'REPOSIÇÃO': 'replenishment',
            'MERCHANDISING': 'merchandising',
            'PROMOÇÃO': 'promotion',
            'FACING': 'facing',
            'GONDOLA': 'gondola',
            'EXPOSIÇÃO': 'exhibition',
            'ORGANIZAÇÃO': 'organization',
            'LIMPEZA': 'cleaning',
            'ATENDIMENTO': 'service',
            'VENDAS': 'sales',
            'TREINAMENTO': 'training',
            'AUDITORIA': 'audit',
            'INVENTÁRIO': 'inventory',
            'CONTROLE': 'control',
            'VERIFICAÇÃO': 'verification',
            'MONITORAMENTO': 'monitoring',
            'SUPERVISÃO': 'supervision',
            'GESTÃO': 'management',
            'ADMINISTRAÇÃO': 'administration'
        }
        
        # Verificar tradução direta
        if nome_tarefa in translations:
            return translations[nome_tarefa]
        
        # Se não encontrar tradução direta, normalizar para snake_case
        # Converter para minúsculas e substituir espaços/caracteres especiais por underscore
        import re
        normalized = nome_tarefa.lower()
        normalized = re.sub(r'[^a-z0-9]+', '_', normalized)
        normalized = re.sub(r'_+', '_', normalized)  # Remover underscores duplicados
        normalized = normalized.strip('_')  # Remover underscores no início/fim
        
        return normalized if normalized else "standard_task"
    
    def _ensure_default_promoter_task(self):
        """Garante que existe uma tarefa padrão no banco e no mapeamento"""
        DEFAULT_TASK_NAME = "standard_task"
        
        # Se já está no mapeamento, não precisa fazer nada
        if DEFAULT_TASK_NAME in self.promoter_task_map:
            return
        
        conn_pg = None
        cursor_pg = None
        
        try:
            destino = DatabaseConnection.get_destino()
            # ⚠️ CRÍTICO: Usar conexão PRD diretamente quando destino for PRD
            if destino == 'PRD':
                conn_pg = DatabaseConnection.get_postgresql_prd_destino_connection()
            else:
                conn_pg = DatabaseConnection.get_postgresql_hml_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            # Verificar se já existe no banco
            cursor_pg.execute("""
                SELECT id FROM pdv.promoter_tasks WHERE name = %s
            """, (DEFAULT_TASK_NAME,))
            existing = cursor_pg.fetchone()
            
            if existing:
                # Já existe, adicionar ao mapeamento
                task_uuid = str(existing[0])
                self.promoter_task_map[DEFAULT_TASK_NAME] = task_uuid
                logger.info(f"[ETAPA 2] Tarefa padrão '{DEFAULT_TASK_NAME}' encontrada no banco: {task_uuid}")
            else:
                # Criar tarefa padrão
                task_uuid_obj = uuid.uuid4()
                task_uuid = str(task_uuid_obj)
                now = datetime.now()
                
                cursor_pg.execute("""
                    INSERT INTO pdv.promoter_tasks 
                    (id, name, task_type, is_active, deleted_at, created_at, updated_at)
                    VALUES (%s::uuid, %s, %s, %s, %s, %s, %s)
                """, (
                    task_uuid,
                    DEFAULT_TASK_NAME,
                    "undefined",
                    True,
                    None,
                    now,
                    now
                ))
                conn_pg.commit()
                
                # Adicionar ao mapeamento
                self.promoter_task_map[DEFAULT_TASK_NAME] = task_uuid
                logger.info(f"[ETAPA 2] Tarefa padrão '{DEFAULT_TASK_NAME}' criada: {task_uuid}")
                print(f"[ETAPA 2] Tarefa padrão '{DEFAULT_TASK_NAME}' criada: {task_uuid}")
                
        except Exception as e:
            logger.error(f"Erro ao garantir tarefa padrão: {e}")
            print(f"ERRO ao garantir tarefa padrão: {e}")
            if conn_pg:
                conn_pg.rollback()
            raise
        finally:
            if cursor_pg:
                cursor_pg.close()
            if conn_pg:
                conn_pg.close()
    
    def step9_migrate_promoter_tasks(self):
        """ETAPA 9: Migrar promoter_tasks (deve ser executado antes do step2)"""
        destino = DatabaseConnection.get_destino()
        schema_pdv = get_schema_pdv()  # pdv (PRD) ou gmpdv (HML)
        print("\n" + "="*80)
        print("ETAPA 9: MIGRANDO PROMOTER_TASKS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 9: Migrando promoter_tasks")
        logger.info(f"Ambiente: {destino} | Schema: {schema_pdv} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        # Usar função compartilhada para mapeamento de task_type
        # (task_type_mapping está dentro da função get_task_type_from_nome_tarefa)
        
        # Carregar filtros do step1 (se existir)
        filter_data = self.load_filter_json()
        id_orcamento_filter_list = []
        
        if filter_data and 'aggregated_ids' in filter_data:
            id_orcamento_filter_list = filter_data['aggregated_ids'].get('IdOrcamento', [])
            logger.info(f"[ETAPA 9] Carregados {len(id_orcamento_filter_list)} IdOrcamento do arquivo de filtros")
            print(f"[ETAPA 9] Carregados {len(id_orcamento_filter_list)} IdOrcamento do arquivo de filtros")
        else:
            logger.warning("[ETAPA 9] Arquivo de filtros não encontrado. Buscando todos os IdOrcamento de contracts...")
            # Se não há JSON, buscar todos os IdOrcamento de contracts
            try:
                schema = get_schema_atual()
                conn_pg = DatabaseConnection.get_postgresql_destino_connection()
                cursor_pg = conn_pg.cursor()
                if self.should_include_legacy_id():
                    cursor_pg.execute(f"SELECT DISTINCT legacy_id FROM {schema}.contracts WHERE legacy_id IS NOT NULL")
                    id_orcamento_filter_list = [row[0] for row in cursor_pg.fetchall()]
                cursor_pg.close()
                conn_pg.close()
            except Exception as e:
                logger.error(f"Erro ao buscar IdOrcamento de contracts: {e}")
                print(f"ERRO ao buscar IdOrcamento: {e}")
                return
        
        if not id_orcamento_filter_list:
            logger.warning("[ETAPA 9] Nenhum IdOrcamento encontrado. Pulando migração de promoter_tasks.")
            print("[ETAPA 9] Nenhum IdOrcamento encontrado. Pulando migração.")
            return
        
        # Limpar tabela
        if self.clear_data:
            print("\n[ETAPA 9] Limpando tabela promoter_tasks (TRUNCATE - flag --clear-data ativo)...")
            logger.info("[ETAPA 9] Flag --clear-data ativo: usando TRUNCATE")
            self.truncate_table('promoter_tasks', schema=schema_pdv)
        else:
            print("\n[ETAPA 9] Limpando tabela promoter_tasks...")
            logger.info("[ETAPA 9] Limpando tabela promoter_tasks")
            # Para promoter_tasks, sempre usar TRUNCATE quando não há filtros específicos
            # ou fazer DELETE baseado nos names que serão inseridos
            try:
                conn_pg_temp = DatabaseConnection.get_postgresql_destino_connection()
                cursor_pg_temp = conn_pg_temp.cursor()
                cursor_pg_temp.execute(f"TRUNCATE TABLE {schema_pdv}.promoter_tasks CASCADE")
                conn_pg_temp.commit()
                cursor_pg_temp.close()
                conn_pg_temp.close()
                logger.info(f"Tabela {schema_pdv}.promoter_tasks truncada com sucesso")
            except Exception as e:
                logger.warning(f"Não foi possível truncar tabela: {e}")
        
        # Construir query para coletar tarefas únicas da ViewOrcamentosLojas com NomeCliente
        conn_sql = None
        cursor_sql = None
        conn_pg = None
        cursor_pg = None
        
        try:
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            # Construir filtros de data se existirem
            where_conditions = []
            query_params = []
            
            # Filtro IdOrcamento
            if id_orcamento_filter_list:
                placeholders = ','.join(['?' for _ in id_orcamento_filter_list])
                where_conditions.append(f"v.IdOrcamento IN ({placeholders})")
                query_params.extend(id_orcamento_filter_list)
            
            # Filtros de data (mesma lógica dos outros steps)
            data_aviso_previo_to_use = self.data_aviso_previo_min
            data_inicio_operacao_to_use = self.data_inicio_operacao_max
            
            if data_aviso_previo_to_use is None and filter_data and 'filters_applied' in filter_data:
                json_filters = filter_data['filters_applied']
                data_aviso_previo_to_use = json_filters.get('data_aviso_previo_min')
            
            if data_inicio_operacao_to_use is None and filter_data and 'filters_applied' in filter_data:
                json_filters = filter_data['filters_applied']
                data_inicio_operacao_to_use = json_filters.get('data_inicio_operacao_max')
            
            if data_aviso_previo_to_use is not None:
                if isinstance(data_aviso_previo_to_use, str):
                    data_aviso_previo_str = data_aviso_previo_to_use
                else:
                    data_aviso_previo_str = data_aviso_previo_to_use.strftime('%Y-%m-%d')
                where_conditions.append("(CONVERT(DATE, v.DataAvisoPrevio) >= ? OR v.DataAvisoPrevio IS NULL)")
                query_params.append(data_aviso_previo_str)
            
            if data_inicio_operacao_to_use is not None:
                if isinstance(data_inicio_operacao_to_use, str):
                    data_inicio_str = data_inicio_operacao_to_use
                else:
                    data_inicio_str = data_inicio_operacao_to_use.strftime('%Y-%m-%d')
                where_conditions.append("CONVERT(DATE, v.DataInicioOperacao) <= ?")
                query_params.append(data_inicio_str)
            
            # Filtro IdOrcamento (se passado diretamente)
            if self.id_orcamento_filter:
                placeholders = ','.join(['?' for _ in self.id_orcamento_filter])
                where_conditions.append(f"v.IdOrcamento IN ({placeholders})")
                query_params.extend(self.id_orcamento_filter)
            
            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)
            
            # Query para coletar NomeTarefa e NomeCliente únicos
            query_tarefas = f"""
            SELECT DISTINCT
                v.NomeTarefa,
                v.NomeCliente
            FROM ViewOrcamentosLojas v
            INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
            {where_clause}
            AND v.NomeCliente IS NOT NULL
            AND LTRIM(RTRIM(v.NomeCliente)) != ''
            """
            
            if self.limit_rows > 0:
                query_tarefas = query_tarefas.replace("SELECT DISTINCT", f"SELECT DISTINCT TOP {self.limit_rows}")
            
            print(f"[ETAPA 9] Coletando tarefas e clientes únicos da ViewOrcamentosLojas...")
            logger.info(f"[ETAPA 9] Query: {query_tarefas}")
            logger.info(f"[ETAPA 9] Parâmetros: {len(query_params)} parâmetros")
            
            if query_params:
                cursor_sql.execute(query_tarefas, query_params)
            else:
                cursor_sql.execute(query_tarefas)
            
            tarefas_view = cursor_sql.fetchall()
            
            if not tarefas_view:
                logger.warning("[ETAPA 9] Nenhuma tarefa encontrada na ViewOrcamentosLojas.")
                print("[ETAPA 9] Nenhuma tarefa encontrada.")
                return
            
            print(f"[ETAPA 9] Encontradas {len(tarefas_view)} combinações únicas de tarefa+cliente")
            logger.info(f"[ETAPA 9] Encontradas {len(tarefas_view)} combinações únicas")
            
            # Conectar ao PostgreSQL
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            # Preparar dados para inserção
            records_to_insert = []
            now = datetime.now()
            seen_names = set()  # Para evitar duplicatas por name
            
            for row in tarefas_view:
                nome_tarefa_raw = row[0]
                nome_cliente_raw = row[1]
                
                # Usar função compartilhada para normalização (mesma lógica do step2)
                name, nome_tarefa_clean = normalize_promoter_task_name(nome_tarefa_raw, nome_cliente_raw)
                
                # Se name é None, pular (NomeCliente obrigatório)
                if not name:
                    continue
                
                # Evitar duplicatas
                if name in seen_names:
                    continue
                seen_names.add(name)
                
                # Mapear task_type usando função compartilhada (mesma lógica do step2)
                task_type = get_task_type_from_nome_tarefa(nome_tarefa_clean)
                
                # Preparar registro
                records_to_insert.append({
                    'name': name,
                    'task_type': task_type,
                    'is_active': True,  # Sempre true conforme especificado
                    'created_at': now,
                    'updated_at': now,
                    'nome_tarefa_original': nome_tarefa_raw
                })
            
            print(f"[ETAPA 9] Processando {len(records_to_insert)} promoter_tasks únicos para inserção...")
            logger.info(f"[ETAPA 9] Processando {len(records_to_insert)} promoter_tasks únicos")
            
            # Inserir em batch (verificando duplicatas antes, já que não há constraint UNIQUE)
            if records_to_insert:
                # Primeiro, verificar quais names já existem
                print("[ETAPA 9] Verificando names existentes...")
                existing_names = set()
                all_names = [record['name'] for record in records_to_insert]
                
                # Buscar em chunks para não sobrecarregar a query
                for i in range(0, len(all_names), CHUNK_SIZE):
                    chunk_names = all_names[i:i + CHUNK_SIZE]
                    placeholders = ','.join(['%s' for _ in chunk_names])
                    cursor_pg.execute(
                        f"SELECT name FROM {schema_pdv}.promoter_tasks WHERE name IN ({placeholders})",
                        chunk_names
                    )
                    existing_names.update([row[0] for row in cursor_pg.fetchall()])
                
                # Filtrar apenas os que não existem
                records_to_insert_new = [
                    record for record in records_to_insert 
                    if record['name'] not in existing_names
                ]
                
                print(f"[ETAPA 9] {len(records_to_insert_new)} novos promoter_tasks para inserir (de {len(records_to_insert)} únicos)")
                logger.info(f"[ETAPA 9] {len(records_to_insert_new)} novos promoter_tasks para inserir")
                
                if records_to_insert_new:
                    insert_query = f"""
                    INSERT INTO {schema_pdv}.promoter_tasks (
                        id, name, task_type, is_active, created_at, updated_at
                    ) VALUES %s
                    """
                    insert_template = "(gen_random_uuid(), %s, %s, %s, %s, %s)"
                    
                    batch_values = [
                        (
                            record['name'],
                            record['task_type'],
                            record['is_active'],
                            record['created_at'],
                            record['updated_at']
                        )
                        for record in records_to_insert_new
                    ]
                    
                    chunk_num = 0
                    inserted_count = 0
                    for i in range(0, len(batch_values), CHUNK_SIZE):
                        chunk = batch_values[i:i + CHUNK_SIZE]
                        chunk_num += 1
                        try:
                            execute_values(
                                cursor_pg,
                                insert_query,
                                chunk,
                                template=insert_template,
                                page_size=CHUNK_SIZE,
                                fetch=False
                            )
                            conn_pg.commit()
                            inserted_count += len(chunk)
                            print(f"[ETAPA 9] Chunk {chunk_num} processado: {len(chunk)} promoter_tasks inseridos")
                        except Exception as chunk_error:
                            logger.error(f"Erro no chunk {chunk_num}: {chunk_error}")
                            conn_pg.rollback()
                            # Tentar inserir um por um para identificar o problema
                            for record in records_to_insert_new[i:i + CHUNK_SIZE]:
                                try:
                                    cursor_pg.execute(
                                        f"INSERT INTO {schema_pdv}.promoter_tasks (id, name, task_type, is_active, created_at, updated_at) VALUES (gen_random_uuid(), %s, %s, %s, %s, %s)",
                                        (record['name'], record['task_type'], record['is_active'], record['created_at'], record['updated_at'])
                                    )
                                    inserted_count += 1
                                except Exception as e:
                                    logger.error(f"Erro ao inserir promoter_task '{record['name']}': {e}")
                                    self.stats['errors'].append(f"promoter_task '{record['name']}': {e}")
                            conn_pg.commit()
                else:
                    inserted_count = 0
                    print("[ETAPA 9] Todos os promoter_tasks já existem. Nenhum novo registro para inserir.")
                    logger.info("[ETAPA 9] Todos os promoter_tasks já existem")
                
                # Carregar mapeamento de promoter_tasks (name -> uuid) para uso no step2
                print("[ETAPA 9] Carregando mapeamento de promoter_tasks...")
                cursor_pg.execute(f"SELECT id, name FROM {schema_pdv}.promoter_tasks")
                for row in cursor_pg.fetchall():
                    self.promoter_task_map[row[1]] = str(row[0])  # name -> uuid (string)
                print(f"[ETAPA 9] {len(self.promoter_task_map)} promoter_tasks mapeados")
                logger.info(f"[ETAPA 9] {len(self.promoter_task_map)} promoter_tasks mapeados")
                
                self.stats['promoter_tasks'] = inserted_count
            
            print(f"\n[ETAPA 9] CONCLUIDA! Total de promoter_tasks migrados: {self.stats['promoter_tasks']}")
            logger.info(f"ETAPA 9 concluida: {self.stats['promoter_tasks']} registros")
            
        except Exception as e:
            logger.error(f"ERRO CRITICO na ETAPA 9: {e}")
            print(f"ERRO CRITICO: {e}")
            import traceback
            logger.error(traceback.format_exc())
            if conn_pg:
                conn_pg.rollback()
            raise
        finally:
            if cursor_sql:
                cursor_sql.close()
            if conn_sql:
                conn_sql.close()
            if cursor_pg:
                cursor_pg.close()
            if conn_pg:
                conn_pg.close()
    
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
        
        # Verificar se step9 foi executado (promoter_task_map deve estar populado)
        if not self.promoter_task_map:
            logger.warning("[ETAPA 2] promoter_task_map vazio. Executando step9_migrate_promoter_tasks primeiro...")
            print("[ETAPA 2] Executando step9_migrate_promoter_tasks primeiro...")
            self.step9_migrate_promoter_tasks()
            if not self.promoter_task_map:
                logger.error("[ETAPA 2] ERRO: Não foi possível criar promoter_task_map. Abortando step2.")
                print("[ETAPA 2] ERRO: Não foi possível criar promoter_task_map. Abortando.")
                raise Exception("promoter_task_map não disponível. step9 deve ser executado antes de step2.")
        
        # Garantir que existe uma tarefa padrão (para casos onde não encontra tarefa)
        self._ensure_default_promoter_task()
        
        # Verificar/criar coluna legacy_id (apenas em HML)
        include_legacy = False
        if self.should_include_legacy_id():
            try:
                conn_check = DatabaseConnection.get_postgresql_destino_connection()
                cursor_check = conn_check.cursor()
                cursor_check.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema}' 
                    AND table_name = 'contract_scenarios' 
                    AND column_name = 'legacy_id'
                """)
                if cursor_check.fetchone():
                    include_legacy = True
                    print("[ETAPA 2] Coluna legacy_id já existe na tabela")
                else:
                    print("[ETAPA 2] Criando coluna legacy_id...")
                    cursor_check.execute(f"ALTER TABLE {schema}.contract_scenarios ADD COLUMN legacy_id INTEGER")
                    conn_check.commit()
                    include_legacy = True
                    print("OK - Coluna legacy_id criada")
                cursor_check.close()
                conn_check.close()
            except Exception as e:
                logger.warning(f"Nao foi possivel criar/verificar coluna legacy_id: {e}")
                include_legacy = False
        
        # Carregar mapeamento de stores
        # Buscar do banco usando legacy_id (disponível tanto em HML quanto em PRD)
        print("[ETAPA 2] Carregando mapeamento de stores...")
        try:
            if self.should_include_legacy_id():
                # Buscar do banco usando legacy_id
                destino = DatabaseConnection.get_destino()
                schema_stores = 'gmcore' if destino == 'HML' else 'core'
                # ⚠️ CRÍTICO: Usar conexão PRD diretamente quando destino for PRD
                if destino == 'PRD':
                    conn_stores = DatabaseConnection.get_postgresql_prd_destino_connection()
                else:
                    conn_stores = DatabaseConnection.get_postgresql_hml_destino_connection()
                cursor_stores = conn_stores.cursor()
                
                # Verificar se a coluna legacy_id existe
                cursor_stores.execute(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_schema = %s 
                    AND table_name = 'stores' 
                    AND column_name = 'legacy_id'
                """, (schema_stores,))
                has_legacy_id = cursor_stores.fetchone()[0] > 0
                
                if has_legacy_id:
                    cursor_stores.execute(f"SELECT id, legacy_id FROM {schema_stores}.stores WHERE legacy_id IS NOT NULL")
                    for row in cursor_stores.fetchall():
                        if row[1] is not None:
                            self.store_id_map[row[1]] = row[0]
                    print(f"OK - {len(self.store_id_map)} stores carregados")
                    logger.info(f"Carregados {len(self.store_id_map)} stores para mapeamento")
                else:
                    logger.warning(f"Coluna legacy_id não existe em {schema_stores}.stores. Mapeamento de stores não será carregado.")
                    print(f"AVISO - Coluna legacy_id não existe em {schema_stores}.stores. Certifique-se de que stores foi migrado antes de contracts.")
                
                cursor_stores.close()
                conn_stores.close()
        except Exception as e:
            logger.warning(f"Erro ao carregar stores: {e}")
            print(f"AVISO - Nao foi possivel carregar stores: {e}")
        
        # Carregar filtros do step1 (se existir)
        filter_data = self.load_filter_json()
        id_orcamento_filter_list = []
        
        if filter_data and 'aggregated_ids' in filter_data:
            id_orcamento_filter_list = filter_data['aggregated_ids'].get('IdOrcamento', [])
            logger.info(f"[ETAPA 2] Carregados {len(id_orcamento_filter_list)} IdOrcamento do arquivo de filtros")
            print(f"[ETAPA 2] Carregados {len(id_orcamento_filter_list)} IdOrcamento do arquivo de filtros")
        else:
            logger.warning("[ETAPA 2] Arquivo de filtros não encontrado. Buscando todos os IdOrcamento de contracts...")
            # Se não há JSON, buscar todos os IdOrcamento de contracts
            try:
                conn_pg = DatabaseConnection.get_postgresql_destino_connection()
                cursor_pg = conn_pg.cursor()
                if include_legacy:
                    cursor_pg.execute(f"SELECT DISTINCT legacy_id FROM {schema}.contracts WHERE legacy_id IS NOT NULL")
                    id_orcamento_filter_list = [row[0] for row in cursor_pg.fetchall()]
                cursor_pg.close()
                conn_pg.close()
            except Exception as e:
                logger.error(f"Erro ao buscar IdOrcamento de contracts: {e}")
        
        # Limpar tabela (TRUNCATE ou DELETE baseado em filtros)
        if not id_orcamento_filter_list or self.clear_data:
            print("\n[ETAPA 2] Limpando tabela contract_scenarios...")
            self.clean_table('contract_scenarios')
        else:
            # Buscar IdOrcamentoLoja que serão deletados primeiro
            print("\n[ETAPA 2] Preparando limpeza de contract_scenarios...")
        
        # Buscar dados do SQL Server usando ViewOrcamentosLojas
        print("[ETAPA 2] Buscando dados do SQL Server...")
        sql_query = """
        SELECT 
            v.IdOrcamentoLoja,
            v.IdOrcamento,
            v.IdEstabelecimento,
            v.NomeTarefa,
            v.NomeCliente,
            v.Frequencia,
            v.Horas,
            v.ValorHora,
            v.DataInicioOperacao,
            v.StatusPedido,
            v.DataInclusaoOrcamentoLojas,
            v.DataAlteracaoOrcamentoLojas,
            v.IdTarefa,
            ol.Ativo
        FROM ViewOrcamentosLojas v
        LEFT JOIN OrcamentoLojas ol ON ol.Id = v.IdOrcamentoLoja
        """
        
        # Aplicar filtro de IdOrcamento se houver
        query_params = []
        if id_orcamento_filter_list:
            placeholders = ','.join(['?' for _ in id_orcamento_filter_list])
            sql_query += f" WHERE v.IdOrcamento IN ({placeholders})"
            query_params.extend(id_orcamento_filter_list)
        
        sql_query += " ORDER BY v.IdOrcamentoLoja"
        
        # Adicionar LIMIT se especificado
        if self.limit_rows > 0:
            sql_query = sql_query.replace("ORDER BY v.IdOrcamentoLoja", 
                f"ORDER BY v.IdOrcamentoLoja OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        
        # Executar query com parâmetros se houver
        if query_params:
            cursor_sql.execute(sql_query, query_params)
        else:
            cursor_sql.execute(sql_query)
        
        # Carregar TODOS os dados na memória de uma vez (otimizado)
        print("[ETAPA 2] Carregando dados na memória...")
        all_rows = cursor_sql.fetchall()
        cursor_sql.close()
        conn_sql.close()
        
        # Se há filtros e não é clear_data: fazer DELETE antes de inserir
        if id_orcamento_filter_list and not self.clear_data:
            # Buscar IdOrcamentoLoja que serão deletados
            id_orcamento_loja_to_delete = [row[0] for row in all_rows]  # IdOrcamentoLoja
            if id_orcamento_loja_to_delete:
                print("\n[ETAPA 2] Limpando registros filtrados da tabela contract_scenarios...")
                self.delete_table_with_filter('contract_scenarios', id_orcamento_loja_to_delete)
        
        print(f"[ETAPA 2] {len(all_rows)} registros carregados. Processando conversões em massa (vetorizado)...")
        
        # Processar conversões em massa usando DataFrame (vetorizado)
        try:
            df = pd.DataFrame.from_records(all_rows, columns=[
                'IdOrcamentoLoja', 'IdOrcamento', 'IdEstabelecimento', 'NomeTarefa', 'NomeCliente',
                'Frequencia', 'Horas', 'ValorHora', 'DataInicioOperacao', 'StatusPedido',
                'DataInclusaoOrcamentoLojas', 'DataAlteracaoOrcamentoLojas', 'IdTarefa', 'Ativo'
            ])
            
            # Log dos tipos de dados inferidos pelo pandas
            print(f"[ETAPA 2] Tipos de dados inferidos pelo pandas:")
            print(f"  IdTarefa: {df['IdTarefa'].dtype} (NULLs: {df['IdTarefa'].isna().sum()})")
            print(f"  Frequencia: {df['Frequencia'].dtype} (NULLs: {df['Frequencia'].isna().sum()})")
            print(f"  StatusPedido: {df['StatusPedido'].dtype} (NULLs: {df['StatusPedido'].isna().sum()})")
            print(f"  Ativo: {df['Ativo'].dtype} (NULLs: {df['Ativo'].isna().sum()})")
            logger.info(f"[ETAPA 2] Tipos inferidos - IdTarefa: {df['IdTarefa'].dtype}, Frequencia: {df['Frequencia'].dtype}, StatusPedido: {df['StatusPedido'].dtype}, Ativo: {df['Ativo'].dtype}")
        except Exception as e:
            logger.error(f"[ETAPA 2] ERRO ao criar DataFrame: {e}")
            print(f"[ETAPA 2] ERRO ao criar DataFrame: {e}")
            raise
        
        # Aplicar transformações vetorizadas
        df['legacy_id'] = df['IdOrcamentoLoja']
        
        # Mapear contract_id e store_id usando .map() (vetorizado)
        df['contract_id'] = df['IdOrcamento'].map(self.contract_id_map)
        df['store_id'] = df['IdEstabelecimento'].map(self.store_id_map)
        
        # Filtrar linhas onde contract_id ou store_id são None (coletar warnings em batch)
        mask_valid = df['contract_id'].notna() & df['store_id'].notna()
        missing_contracts = df[~mask_valid & df['contract_id'].isna()]['IdOrcamento'].unique()
        missing_stores = df[~mask_valid & df['store_id'].isna()]['IdEstabelecimento'].unique()
        
        if len(missing_contracts) > 0:
            logger.warning(f"Contract nao encontrado para {len(missing_contracts)} IdOrcamento(s): {missing_contracts[:10].tolist()}{'...' if len(missing_contracts) > 10 else ''}")
        if len(missing_stores) > 0:
            logger.warning(f"Store nao encontrado para {len(missing_stores)} IdEstabelecimento(s): {missing_stores[:10].tolist()}{'...' if len(missing_stores) > 10 else ''}")
        
        # Filtrar apenas linhas válidas
        df = df[mask_valid].copy()
        
        # Preparar promoter_task_id usando vetorização (otimizado - mesma lógica do step9)
        print("[ETAPA 2] Normalizando e mapeando promoter_task_id (vetorizado)...")
        logger.info("[ETAPA 2] Normalizando promoter_task names usando função compartilhada")
        
        # Normalização vetorizada usando pandas (mais rápido que apply)
        # 1. Normalizar NomeTarefa
        df['nome_tarefa_clean'] = df['NomeTarefa'].fillna('').astype(str).str.strip().str.upper()
        df.loc[df['nome_tarefa_clean'].isin(['NULL', 'NONE', '']), 'nome_tarefa_clean'] = None
        
        # 2. Normalizar NomeCliente
        df['nome_cliente_clean'] = df['NomeCliente'].fillna('').astype(str).str.strip()
        df.loc[df['nome_cliente_clean'].isin(['NULL', 'NONE', '']), 'nome_cliente_clean'] = None
        
        # 3. Construir name usando operações vetorizadas
        # Primeiro validar: se nome_cliente_clean é None, name deve ser None
        mask_valid_cliente = df['nome_cliente_clean'].notna()
        df['name'] = None
        
        # Apenas construir name para registros com cliente válido
        if mask_valid_cliente.sum() > 0:
            # Se nome_tarefa_clean não é None e não é vazio: "{nome_tarefa_clean} {nome_cliente_clean}"
            # Senão: "INDEFINIDA {nome_cliente_clean}"
            mask_has_tarefa = df['nome_tarefa_clean'].notna() & (df['nome_tarefa_clean'] != '') & mask_valid_cliente
            
            df.loc[mask_has_tarefa, 'name'] = (
                df.loc[mask_has_tarefa, 'nome_tarefa_clean'] + ' ' + 
                df.loc[mask_has_tarefa, 'nome_cliente_clean']
            )
            
            mask_indefinida = ~mask_has_tarefa & mask_valid_cliente
            df.loc[mask_indefinida, 'name'] = 'INDEFINIDA ' + df.loc[mask_indefinida, 'nome_cliente_clean']
            
            # 4. Remover espaços extras (vetorizado) apenas onde name não é None
            df.loc[df['name'].notna(), 'name'] = (
                df.loc[df['name'].notna(), 'name']
                .str.replace(r'\s+', ' ', regex=True)
                .str.strip()
            )
        
        # Preencher valores None com tarefa padrão
        DEFAULT_TASK_NAME = "standard_task"
        default_task_id = self.promoter_task_map.get(DEFAULT_TASK_NAME)
        
        if default_task_id:
            default_task_id = str(default_task_id)
        else:
            logger.error("[ETAPA 2] ERRO CRÍTICO: Tarefa padrão não disponível!")
            raise Exception("Tarefa padrão 'standard_task' não encontrada no promoter_task_map")
        
        # Mapear usando .map() (vetorizado - muito mais rápido que apply)
        df['promoter_task_id'] = df['name'].map(self.promoter_task_map)
        
        # Preencher valores None ou não encontrados com tarefa padrão (vetorizado)
        missing_mask = df['promoter_task_id'].isna() | df['name'].isna()
        missing_count = missing_mask.sum()
        
        if missing_count > 0:
            # Logar combinações não encontradas (apenas primeiras 10 para não sobrecarregar log)
            missing_names = df[missing_mask & df['name'].notna()]['name'].unique()[:10]
            for missing_name in missing_names:
                logger.warning(f"[ETAPA 2] Tarefa não encontrada para name='{missing_name}'. Usando tarefa padrão.")
            if len(df[missing_mask & df['name'].notna()]['name'].unique()) > 10:
                logger.warning(f"[ETAPA 2] ... e mais {len(df[missing_mask & df['name'].notna()]['name'].unique()) - 10} combinações não encontradas")
            
            # Preencher com tarefa padrão (vetorizado)
            df.loc[missing_mask, 'promoter_task_id'] = default_task_id
        
        # Converter para string
        df['promoter_task_id'] = df['promoter_task_id'].astype(str)
        
        # Remover colunas temporárias
        df = df.drop(columns=['name', 'nome_tarefa_clean', 'nome_cliente_clean'], errors='ignore')
        
        print(f"[ETAPA 2] Mapeamento de promoter_task_id concluído. {missing_count} registros usaram tarefa padrão.")
        logger.info(f"[ETAPA 2] Mapeamento concluído: {missing_count} registros usaram tarefa padrão de {len(df)} total")
        
        # Verificar se há valores None (erro crítico - não deveria acontecer)
        if df['promoter_task_id'].isna().any():
            missing_count = df['promoter_task_id'].isna().sum()
            logger.error(f"[ETAPA 2] ERRO CRÍTICO: {missing_count} registros sem promoter_task_id válido mesmo com tarefa padrão")
            print(f"[ETAPA 2] ERRO CRÍTICO: {missing_count} registros sem promoter_task_id válido")
            raise Exception(f"Não foi possível atribuir promoter_task_id para {missing_count} registros")
        
        # Mapear frequency de inteiro para varchar (conversão em lote vetorizada)
        frequency_map = {
            1: 'once_per_week',
            2: 'twice_per_week',
            3: 'three_times_per_week',
            4: 'four_times_per_week',
            5: 'five_times_per_week',
            6: 'six_times_per_week',
            7: 'seven_times_per_week',
            15: 'every_15_days',
            30: 'once_per_month'
        }
        # Converter para inteiro primeiro (caso venha como string) e depois mapear
        try:
            print("[ETAPA 2] Convertendo Frequencia...")
            # Converter para numérico primeiro, tratar NaN, depois converter para Int64 de forma segura
            frequencia_numeric = pd.to_numeric(df['Frequencia'], errors='coerce')
            print(f"[ETAPA 2] Frequencia após to_numeric: tipo={frequencia_numeric.dtype}, NULLs={frequencia_numeric.isna().sum()}")
            # Converter para Int64 de forma segura usando apply para tratar cada valor individualmente
            # Primeiro arredondar valores não inteiros, depois converter
            frequencia_int = frequencia_numeric.apply(lambda x: pd.NA if pd.isna(x) else int(round(float(x))) if pd.notna(x) else pd.NA)
            frequencia_int = frequencia_int.astype('Int64')
            print(f"[ETAPA 2] Frequencia após conversão para Int64: tipo={frequencia_int.dtype}, NULLs={frequencia_int.isna().sum()}")
            df['frequency'] = frequencia_int.map(frequency_map)
            # Preencher valores não mapeados com None ou valor padrão se necessário
            df['frequency'] = df['frequency'].fillna('')
            print("[ETAPA 2] Frequencia convertida com sucesso")
        except Exception as e:
            logger.error(f"[ETAPA 2] ERRO ao converter Frequencia: {e}")
            print(f"[ETAPA 2] ERRO ao converter Frequencia: {e}")
            print(f"[ETAPA 2] Tipo original: {df['Frequencia'].dtype}")
            print(f"[ETAPA 2] Valores únicos (primeiros 10): {df['Frequencia'].unique()[:10]}")
            raise
        df['hours'] = self.convert_hours_to_float_vectorized(df['Horas'])
        df['hour_value'] = df['ValorHora'].fillna(0.0)
        df['start_date'] = df['DataInicioOperacao'].fillna(df['DataInclusaoOrcamentoLojas'])
        df.loc[df['start_date'].isna(), 'start_date'] = datetime.now()
        
        # Status: usar StatusPedido se disponível, senão usar Ativo, depois converter para varchar
        # Converter status de inteiro para varchar: 11 → 'closed', 0 → 'inactive', qualquer outro → 'active' (conversão em lote vetorizada)
        try:
            print("[ETAPA 2] Convertendo Ativo...")
            # Converter Ativo para numérico primeiro (tratando NaN e valores não numéricos)
            ativo_numeric = pd.to_numeric(df['Ativo'], errors='coerce')
            print(f"[ETAPA 2] Ativo após to_numeric: tipo={ativo_numeric.dtype}, NULLs={ativo_numeric.isna().sum()}")
            ativo_numeric = ativo_numeric.fillna(0)
            # Converter para int usando apply para evitar problemas de conversão direta
            ativo_int = ativo_numeric.apply(lambda x: int(x) if pd.notna(x) else 0)
            print(f"[ETAPA 2] Ativo convertido com sucesso: tipo={ativo_int.dtype}")
        except Exception as e:
            logger.error(f"[ETAPA 2] ERRO ao converter Ativo: {e}")
            print(f"[ETAPA 2] ERRO ao converter Ativo: {e}")
            print(f"[ETAPA 2] Tipo original: {df['Ativo'].dtype}")
            print(f"[ETAPA 2] Valores únicos: {df['Ativo'].unique()}")
            raise
        
        try:
            print("[ETAPA 2] Convertendo StatusPedido...")
            status_pedido_numeric = pd.to_numeric(df['StatusPedido'], errors='coerce')
            print(f"[ETAPA 2] StatusPedido após to_numeric: tipo={status_pedido_numeric.dtype}, NULLs={status_pedido_numeric.isna().sum()}")
            # Usar fillna com ativo_int já convertido
            status_temp = status_pedido_numeric.fillna(ativo_int)
            status_temp = status_temp.fillna(0)
            # Converter para int usando apply para evitar problemas de conversão direta
            status_temp = status_temp.apply(lambda x: int(x) if pd.notna(x) else 0)
            print(f"[ETAPA 2] StatusPedido convertido com sucesso: tipo={status_temp.dtype}")
        except Exception as e:
            logger.error(f"[ETAPA 2] ERRO ao converter StatusPedido: {e}")
            print(f"[ETAPA 2] ERRO ao converter StatusPedido: {e}")
            print(f"[ETAPA 2] Tipo original: {df['StatusPedido'].dtype}")
            print(f"[ETAPA 2] Valores únicos: {df['StatusPedido'].unique()}")
            raise
        # Mapear em lote vetorizado: 1 para 'active', 0 para qualquer outro valor (int)
        df['status'] = pd.Series(1, index=df.index, dtype=int)  # 1 = active por padrão
        # Se status_temp == 11 (closed) OU status_temp == 0 (inactive) → status = 0
        df.loc[(status_temp == 11) | (status_temp == 0), 'status'] = 0
        
        df['created_at'] = df['DataInclusaoOrcamentoLojas'].fillna(datetime.now())
        df['updated_at'] = df['DataAlteracaoOrcamentoLojas'].fillna(df['created_at'])
        
        # Converter UUIDs para string
        df['contract_id'] = df['contract_id'].astype(str)
        df['store_id'] = df['store_id'].astype(str)
        df['promoter_task_id'] = df['promoter_task_id'].astype(str)
        
        # Converter DataFrame diretamente para lista de tuplas (otimizado)
        if include_legacy:
            processed_tuples = list(zip(
                df['contract_id'].tolist(),
                df['store_id'].tolist(),
                df['promoter_task_id'].tolist(),
                df['frequency'].tolist(),
                df['hours'].tolist(),
                df['hour_value'].tolist(),
                df['start_date'].tolist(),
                df['status'].tolist(),
                df['created_at'].tolist(),
                df['updated_at'].tolist(),
                df['legacy_id'].tolist()
            ))
        else:
            processed_tuples = list(zip(
                df['contract_id'].tolist(),
                df['store_id'].tolist(),
                df['promoter_task_id'].tolist(),
                df['frequency'].tolist(),
                df['hours'].tolist(),
                df['hour_value'].tolist(),
                df['start_date'].tolist(),
                df['status'].tolist(),
                df['created_at'].tolist(),
                df['updated_at'].tolist()
            ))
        legacy_ids_list = df['legacy_id'].tolist()
        
        print(f"[ETAPA 2] {len(processed_tuples)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL e inserir em bulk
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Query de insert usando gen_random_uuid() - formato para execute_values
        if include_legacy:
            insert_query = f"""
            INSERT INTO {schema}.contract_scenarios (
                id, contract_id, store_id, promoter_task_id, frequency, hours, hour_value,
                start_date, status, created_at, updated_at, legacy_id
            ) VALUES %s
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        else:
            insert_query = f"""
            INSERT INTO {schema}.contract_scenarios (
                id, contract_id, store_id, promoter_task_id, frequency, hours, hour_value,
                start_date, status, created_at, updated_at
            ) VALUES %s
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        
        chunk_num = 0
        total_processed = 0
        all_legacy_ids_inserted = []
        
        try:
            for i in range(0, len(processed_tuples), CHUNK_SIZE):
                chunk = processed_tuples[i:i + CHUNK_SIZE]
                chunk_num += 1
                
                if chunk:
                    try:
                        # Usar execute_values() para inserção otimizada em bulk
                        execute_values(
                            cursor_pg,
                            insert_query,
                            chunk,
                            template=insert_template,
                            page_size=CHUNK_SIZE,
                            fetch=False
                        )
                        
                        # Coletar legacy_ids para lookup depois (apenas se include_legacy)
                        if include_legacy:
                            chunk_legacy_ids = legacy_ids_list[i:i + CHUNK_SIZE]
                            all_legacy_ids_inserted.extend(chunk_legacy_ids)
                        
                        total_processed += len(chunk)
                        self.stats['contract_scenarios'] += len(chunk)
                        
                        conn_pg.commit()
                        print(f"[ETAPA 2] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                        logger.info(f"[ETAPA 2] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                        
                    except Exception as batch_error:
                        error_msg = f"Erro ao inserir batch de contract_scenarios: {batch_error}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
            
            # Buscar UUIDs gerados para mapeamento (uma única query após todas as inserções)
            print(f"[ETAPA 2] DEBUG: include_legacy={include_legacy}, all_legacy_ids_inserted={len(all_legacy_ids_inserted) if all_legacy_ids_inserted else 0}")
            if include_legacy and all_legacy_ids_inserted:
                print(f"[ETAPA 2] Buscando UUIDs gerados para {len(all_legacy_ids_inserted)} registros...")
                cursor_pg.execute(f"""
                    SELECT id, legacy_id 
                    FROM {schema}.contract_scenarios 
                    WHERE legacy_id = ANY(%s)
                """, (all_legacy_ids_inserted,))
                for uuid_row, leg_id in cursor_pg.fetchall():
                    self.scenario_id_map[leg_id] = uuid_row
                print(f"[ETAPA 2] {len(self.scenario_id_map)} UUIDs mapeados")
                logger.info(f"[ETAPA 2] {len(self.scenario_id_map)} UUIDs mapeados no scenario_id_map")
            elif not include_legacy and all_legacy_ids_inserted:
                # Sem legacy_id, usar ordem de created_at (menos confiável)
                print(f"[ETAPA 2] Buscando UUIDs gerados para mapeamento (sem legacy_id)...")
                cursor_pg.execute(f"""
                    SELECT id, created_at 
                    FROM {schema}.contract_scenarios 
                    ORDER BY created_at
                    LIMIT %s
                """, (len(all_legacy_ids_inserted),))
                uuid_rows = cursor_pg.fetchall()
                for idx, (uuid_row, created_at) in enumerate(uuid_rows):
                    if idx < len(all_legacy_ids_inserted):
                        leg_id = all_legacy_ids_inserted[idx]
                        self.scenario_id_map[leg_id] = uuid_row
                print(f"[ETAPA 2] {len(self.scenario_id_map)} UUIDs mapeados")
                logger.info(f"[ETAPA 2] {len(self.scenario_id_map)} UUIDs mapeados no scenario_id_map (sem legacy_id)")
            else:
                logger.warning(f"[ETAPA 2] Nao foi possivel mapear UUIDs: include_legacy={include_legacy}, all_legacy_ids_inserted={len(all_legacy_ids_inserted) if all_legacy_ids_inserted else 0}")
            
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\n[ETAPA 2] CONCLUIDA! Total de contract_scenarios migrados: {self.stats['contract_scenarios']}")
            logger.info(f"[ETAPA 2] CONCLUIDA! Total: {self.stats['contract_scenarios']}")
            
            # Validação
            self.validate_step2_contract_scenarios()
            
        except Exception as e:
            logger.error(f"Erro critico na ETAPA 2: {e}")
            if 'conn_pg' in locals():
                conn_pg.rollback()
                conn_pg.close()
            raise
    
    def validate_step3_contract_scenario_stores(self):
        """Validação e relatório de qualidade - ETAPA 3"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 3: CONTRACT_SCENARIO_STORES")
        print("-"*80)
        
        try:
            # Carregar filtros do step1 e step2
            filter_ids = self._get_filter_ids_for_validation()
            id_orcamento_list = filter_ids.get('IdOrcamento', [])
            
            # Contar origem aplicando os mesmos filtros do step1
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            if id_orcamento_list:
                # Aplicar filtro de IdOrcamento - contar TODOS os registros (não apenas DISTINCT)
                placeholders = ','.join(['?' for _ in id_orcamento_list])
                count_query = f"""
                SELECT COUNT(*)
                FROM ViewOrcamentosLojas v
                LEFT JOIN OrcamentoLojas ol ON ol.Id = v.IdOrcamentoLoja
                WHERE v.IdOrcamento IN ({placeholders})
                """
                cursor_sql.execute(count_query, id_orcamento_list)
            elif self.limit_rows > 0:
                cursor_sql.execute(f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT TOP {self.limit_rows} v.IdOrcamentoLoja
                        FROM ViewOrcamentosLojas v
                        LEFT JOIN OrcamentoLojas ol ON ol.Id = v.IdOrcamentoLoja
                        ORDER BY v.IdOrcamentoLoja
                    ) AS limited
                """)
            else:
                cursor_sql.execute("SELECT COUNT(*) FROM ViewOrcamentosLojas v LEFT JOIN OrcamentoLojas ol ON ol.Id = v.IdOrcamentoLoja")
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino aplicando os mesmos filtros
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            if id_orcamento_list:
                # Contar apenas os contract_scenario_stores relacionados aos contract_scenarios migrados
                cursor_pg.execute(f"""
                    SELECT COUNT(*) 
                    FROM {schema}.contract_scenario_stores css
                    INNER JOIN {schema}.contract_scenarios cs ON cs.id = css.scenario_id
                    INNER JOIN {schema}.contracts c ON c.id = cs.contract_id
                    WHERE c.legacy_id = ANY(%s)
                """, (id_orcamento_list,))
            else:
                cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contract_scenario_stores")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - ViewOrcamentosLojas):")
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
        
        # Carregar mapeamentos necessários (scenarios e stores)
        print("[ETAPA 3] Carregando mapeamentos de scenarios e stores...")
        try:
            schema_scenarios = schema
            # ⚠️ CRÍTICO: Usar conexão PRD diretamente quando destino for PRD
            if destino == 'PRD':
                conn_scenarios = DatabaseConnection.get_postgresql_prd_destino_connection()
            else:
                conn_scenarios = DatabaseConnection.get_postgresql_hml_destino_connection()
            cursor_scenarios = conn_scenarios.cursor()
            if self.should_include_legacy_id():
                cursor_scenarios.execute(f"SELECT id, legacy_id FROM {schema_scenarios}.contract_scenarios WHERE legacy_id IS NOT NULL")
                for row in cursor_scenarios.fetchall():
                    if row[1] is not None:
                        self.scenario_id_map[row[1]] = row[0]
            cursor_scenarios.close()
            conn_scenarios.close()
            print(f"OK - {len(self.scenario_id_map)} scenarios carregados")
            logger.info(f"Carregados {len(self.scenario_id_map)} scenarios para mapeamento")
        except Exception as e:
            logger.warning(f"Erro ao carregar scenarios: {e}")
            print(f"AVISO - Nao foi possivel carregar scenarios: {e}")
        
        try:
            if self.should_include_legacy_id():
                # Buscar do banco usando legacy_id
                destino = DatabaseConnection.get_destino()
                schema_stores = 'gmcore' if destino == 'HML' else 'core'
                # ⚠️ CRÍTICO: Usar conexão PRD diretamente quando destino for PRD
                if destino == 'PRD':
                    conn_stores = DatabaseConnection.get_postgresql_prd_destino_connection()
                else:
                    conn_stores = DatabaseConnection.get_postgresql_hml_destino_connection()
                cursor_stores = conn_stores.cursor()
                
                # Verificar se a coluna legacy_id existe
                cursor_stores.execute(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_schema = %s 
                    AND table_name = 'stores' 
                    AND column_name = 'legacy_id'
                """, (schema_stores,))
                has_legacy_id = cursor_stores.fetchone()[0] > 0
                
                if has_legacy_id:
                    cursor_stores.execute(f"SELECT id, legacy_id FROM {schema_stores}.stores WHERE legacy_id IS NOT NULL")
                    for row in cursor_stores.fetchall():
                        if row[1] is not None:
                            self.store_id_map[row[1]] = row[0]
                    print(f"OK - {len(self.store_id_map)} stores carregados")
                    logger.info(f"Carregados {len(self.store_id_map)} stores para mapeamento")
                else:
                    logger.warning(f"Coluna legacy_id não existe em {schema_stores}.stores. Mapeamento de stores não será carregado.")
                    print(f"AVISO - Coluna legacy_id não existe em {schema_stores}.stores. Certifique-se de que stores foi migrado antes de contracts.")
                
                cursor_stores.close()
                conn_stores.close()
        except Exception as e:
            logger.warning(f"Erro ao carregar stores: {e}")
            print(f"AVISO - Nao foi possivel carregar stores: {e}")
        
        # Verificar se precisa criar coluna legacy_id (apenas em HML)
        include_legacy = False
        if self.should_include_legacy_id():
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
                if cursor_check.fetchone():
                    include_legacy = True
                    print("[ETAPA 3] Coluna legacy_id já existe na tabela")
                else:
                    print("[ETAPA 3] Criando coluna legacy_id...")
                    cursor_check.execute(f"ALTER TABLE {schema}.contract_scenario_stores ADD COLUMN legacy_id INTEGER")
                    conn_check.commit()
                    include_legacy = True
                    print("OK - Coluna legacy_id criada")
                cursor_check.close()
                conn_check.close()
            except Exception as e:
                logger.warning(f"Nao foi possivel criar/verificar coluna legacy_id: {e}")
                include_legacy = False
        
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
        
        # Carregar TODOS os dados na memória de uma vez (otimizado)
        print("[ETAPA 3] Carregando dados na memória...")
        all_rows = cursor_sql.fetchall()
        cursor_sql.close()
        conn_sql.close()
        
        print(f"[ETAPA 3] {len(all_rows)} registros carregados. Processando conversões em massa (vetorizado)...")
        
        # Processar conversões em massa usando DataFrame (vetorizado)
        df = pd.DataFrame.from_records(all_rows, columns=[
            'IdOrcamentoLoja', 'IdEstabelecimento', 'DataInicioOperacao',
            'DataInclusaoOrcamentoLojas', 'DataAlteracaoOrcamentoLojas', 'Ativo', 'DataExclusao'
        ])
        
        # Aplicar transformações vetorizadas
        df['legacy_id'] = df['IdOrcamentoLoja']
        
        # Mapear scenario_id e store_id usando .map() (vetorizado)
        df['scenario_id'] = df['IdOrcamentoLoja'].map(self.scenario_id_map)
        df['store_id'] = df['IdEstabelecimento'].map(self.store_id_map)
        
        # Filtrar linhas onde scenario_id ou store_id são None (coletar warnings em batch)
        mask_valid = df['scenario_id'].notna() & df['store_id'].notna()
        missing_scenarios = df[~mask_valid & df['scenario_id'].isna()]['IdOrcamentoLoja'].unique()
        missing_stores = df[~mask_valid & df['store_id'].isna()]['IdEstabelecimento'].unique()
        
        if len(missing_scenarios) > 0:
            logger.warning(f"Scenario nao encontrado para {len(missing_scenarios)} IdOrcamentoLoja(s): {missing_scenarios[:10].tolist()}{'...' if len(missing_scenarios) > 10 else ''}")
        if len(missing_stores) > 0:
            logger.warning(f"Store nao encontrado para {len(missing_stores)} IdEstabelecimento(s): {missing_stores[:10].tolist()}{'...' if len(missing_stores) > 10 else ''}")
        
        # Filtrar apenas linhas válidas
        df = df[mask_valid].copy()
        
        # Preparar valores com transformações vetorizadas
        df['start_date'] = df['DataInicioOperacao'].fillna(df['DataInclusaoOrcamentoLojas'])
        df.loc[df['start_date'].isna(), 'start_date'] = datetime.now()
        df['status'] = df['Ativo'].fillna(False).astype(int)
        # Converter NaT para None explicitamente - usar mask para identificar NaT/NaN e substituir
        # NOTA: Em PRD a coluna é 'closed_at' (DATE), não 'removed_at' (TIMESTAMP)
        # Converter datetime para date apenas (remover hora) - tratar None/NaT corretamente
        df['closed_at'] = pd.to_datetime(df['DataExclusao'], errors='coerce')
        # Converter para date apenas onde não é NaT/None
        mask_not_null = df['closed_at'].notna()
        df.loc[mask_not_null, 'closed_at'] = df.loc[mask_not_null, 'closed_at'].dt.date
        df.loc[~mask_not_null, 'closed_at'] = None
        df['created_at'] = df['DataInclusaoOrcamentoLojas'].fillna(datetime.now())
        df['updated_at'] = df['DataAlteracaoOrcamentoLojas'].fillna(df['created_at'])
        
        # Converter UUIDs para string
        df['scenario_id'] = df['scenario_id'].astype(str)
        df['store_id'] = df['store_id'].astype(str)
        
        # Converter DataFrame diretamente para lista de tuplas (otimizado)
        # Converter NaT/NaN para None explicitamente antes de criar tuplas
        # Função helper para converter NaT/NaN para None (inclui objetos date)
        def convert_nat_to_none(val):
            if val is None:
                return None
            if pd.isna(val) or val is pd.NaT or str(val) == 'NaT':
                return None
            # Se for objeto date do Python, manter como está
            return val
        
        closed_at_list = [convert_nat_to_none(x) for x in df['closed_at']]
        
        if include_legacy:
            processed_tuples = list(zip(
                df['legacy_id'].tolist(),
                df['scenario_id'].tolist(),
                df['store_id'].tolist(),
                df['start_date'].tolist(),
                df['status'].tolist(),
                closed_at_list,
                df['created_at'].tolist(),
                df['updated_at'].tolist()
            ))
            legacy_ids_list = df['legacy_id'].tolist()
        else:
            processed_tuples = list(zip(
                df['scenario_id'].tolist(),
                df['store_id'].tolist(),
                df['start_date'].tolist(),
                df['status'].tolist(),
                closed_at_list,
                df['created_at'].tolist(),
                df['updated_at'].tolist()
            ))
            legacy_ids_list = df['legacy_id'].tolist()
        
        print(f"[ETAPA 3] {len(processed_tuples)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL e inserir em bulk
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Query de insert usando gen_random_uuid() - formato para execute_values
        # NOTA: Em PRD a coluna é 'closed_at' (DATE), não 'removed_at' (TIMESTAMP)
        if include_legacy:
            insert_query = f"""
            INSERT INTO {schema}.contract_scenario_stores (
                id, legacy_id, scenario_id, store_id, start_date,
                status, closed_at, created_at, updated_at
            ) VALUES %s
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s)"
        else:
            insert_query = f"""
            INSERT INTO {schema}.contract_scenario_stores (
                id, scenario_id, store_id, start_date,
                status, closed_at, created_at, updated_at
            ) VALUES %s
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s)"
        
        chunk_num = 0
        total_processed = 0
        all_legacy_ids_inserted = []
        
        try:
            for i in range(0, len(processed_tuples), CHUNK_SIZE):
                chunk = processed_tuples[i:i + CHUNK_SIZE]
                chunk_num += 1
                
                if chunk:
                    try:
                        # Usar execute_values() para inserção otimizada em bulk
                        execute_values(
                            cursor_pg,
                            insert_query,
                            chunk,
                            template=insert_template,
                            page_size=CHUNK_SIZE,
                            fetch=False
                        )
                        
                        # Coletar legacy_ids para lookup depois (se necessário)
                        chunk_legacy_ids = legacy_ids_list[i:i + CHUNK_SIZE]
                        all_legacy_ids_inserted.extend(chunk_legacy_ids)
                        
                        total_processed += len(chunk)
                        self.stats['contract_scenario_stores'] += len(chunk)
                        
                        conn_pg.commit()
                        print(f"[ETAPA 3] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                        logger.info(f"[ETAPA 3] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                        
                    except Exception as batch_error:
                        error_msg = f"Erro ao inserir batch de contract_scenario_stores: {batch_error}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
            
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\n[ETAPA 3] CONCLUIDA! Total de contract_scenario_stores migrados: {self.stats['contract_scenario_stores']}")
            logger.info(f"[ETAPA 3] CONCLUIDA! Total: {self.stats['contract_scenario_stores']}")
            
            # Validação
            self.validate_step3_contract_scenario_stores()
            
        except Exception as e:
            logger.error(f"Erro critico na ETAPA 3: {e}")
            if 'conn_pg' in locals():
                try:
                    conn_pg.rollback()
                    if 'cursor_pg' in locals():
                        cursor_pg.close()
                    conn_pg.close()
                except:
                    pass
            raise
    
    def validate_step4_contract_sellers(self):
        """Validação e relatório de qualidade - ETAPA 4"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 4: CONTRACT_SELLERS")
        print("-"*80)
        
        try:
            # Carregar filtros do step1
            filter_ids = self._get_filter_ids_for_validation()
            id_orcamento_list = filter_ids.get('IdOrcamento', [])
            
            # Contar origem: Orcamento (IdUsuarioVendedor) + FaturamentoOrcamentoComissao
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            # Contar Orcamentos com IdUsuarioVendedor
            if id_orcamento_list:
                placeholders = ','.join(['?' for _ in id_orcamento_list])
                cursor_sql.execute(f"""
                    SELECT COUNT(*) 
                    FROM Orcamento 
                    WHERE IdUsuarioVendedor IS NOT NULL
                    AND Id IN ({placeholders})
                """, id_orcamento_list)
            elif self.limit_rows > 0:
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
            if id_orcamento_list:
                placeholders = ','.join(['?' for _ in id_orcamento_list])
                cursor_sql.execute(f"""
                    SELECT COUNT(*) 
                    FROM FaturamentoOrcamentoComissao 
                    WHERE IdOrcamento IN ({placeholders})
                """, id_orcamento_list)
            elif self.limit_rows > 0:
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
            
            # Contar destino aplicando os mesmos filtros
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            if id_orcamento_list:
                # Contar apenas os contract_sellers relacionados aos contracts migrados
                cursor_pg.execute(f"""
                    SELECT COUNT(*) 
                    FROM {schema}.contract_sellers cs
                    INNER JOIN {schema}.contracts c ON c.id = cs.contract_id
                    WHERE c.legacy_id = ANY(%s)
                """, (id_orcamento_list,))
            else:
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
            # ⚠️ CRÍTICO: Usar conexão PRD diretamente quando destino for PRD
            if destino == 'PRD':
                conn_users = DatabaseConnection.get_postgresql_prd_destino_connection()
            else:
                conn_users = DatabaseConnection.get_postgresql_hml_destino_connection()
            cursor_users = conn_users.cursor()
            
            # Verificar se a coluna legacy_id existe
            cursor_users.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = '{schema_users}' 
                AND table_name = 'users' 
                AND column_name = 'legacy_id'
            """)
            has_legacy_id = cursor_users.fetchone() is not None
            
            if has_legacy_id:
                cursor_users.execute(f"SELECT id, legacy_id FROM {schema_users}.users WHERE legacy_id IS NOT NULL")
                for row in cursor_users.fetchall():
                    if row[1] is not None:
                        user_id_map[row[1]] = row[0]
                print(f"OK - {len(user_id_map)} users carregados")
                logger.info(f"Carregados {len(user_id_map)} users para mapeamento")
            else:
                print(f"AVISO - Tabela {schema_users}.users nao possui coluna legacy_id. Nao sera possivel mapear IdUsuarioVendedor.")
                logger.warning(f"Tabela {schema_users}.users nao possui coluna legacy_id. Mapeamento de users nao sera possivel.")
            
            cursor_users.close()
            conn_users.close()
        except Exception as e:
            logger.warning(f"Erro ao carregar users: {e}")
            print(f"AVISO - Nao foi possivel carregar users: {e}")
        
        # Truncate
        print("\n[ETAPA 4] Limpando tabela contract_sellers...")
        self.truncate_table('contract_sellers')
        
        # Carregar filtros do step1 (se existir)
        filter_data = self.load_filter_json()
        id_orcamento_filter_list = []
        
        if filter_data and 'aggregated_ids' in filter_data:
            id_orcamento_filter_list = filter_data['aggregated_ids'].get('IdOrcamento', [])
            logger.info(f"[ETAPA 4] Carregados {len(id_orcamento_filter_list)} IdOrcamento do arquivo de filtros")
            print(f"[ETAPA 4] Carregados {len(id_orcamento_filter_list)} IdOrcamento do arquivo de filtros")
        elif len(self.id_orcamento_filter) > 0:
            id_orcamento_filter_list = self.id_orcamento_filter
            logger.info(f"[ETAPA 4] Usando {len(id_orcamento_filter_list)} IdOrcamento dos filtros aplicados")
            print(f"[ETAPA 4] Usando {len(id_orcamento_filter_list)} IdOrcamento dos filtros aplicados")
        
        # PARTE 1: Migrar sellers de Orcamento (IdUsuarioVendedor)
        print("[ETAPA 4] Migrando sellers de Orcamento...")
        sql_query_orcamento = """
        SELECT 
            Id,
            IdUsuarioVendedor
        FROM Orcamento
        WHERE IdUsuarioVendedor IS NOT NULL
        """
        
        query_params_orcamento = []
        if id_orcamento_filter_list:
            placeholders = ','.join(['?' for _ in id_orcamento_filter_list])
            sql_query_orcamento += f" AND Id IN ({placeholders})"
            query_params_orcamento.extend(id_orcamento_filter_list)
        
        sql_query_orcamento += " ORDER BY Id"
        
        if self.limit_rows > 0:
            sql_query_orcamento = sql_query_orcamento.replace("ORDER BY Id", 
                f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        if query_params_orcamento:
            cursor_sql.execute(sql_query_orcamento, query_params_orcamento)
        else:
            cursor_sql.execute(sql_query_orcamento)
        
        # Carregar TODOS os dados na memória de uma vez (otimizado)
        print("[ETAPA 4] Carregando dados de Orcamento na memória...")
        all_rows_orcamento = cursor_sql.fetchall()
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
        """
        
        query_params_comissao = []
        if id_orcamento_filter_list:
            placeholders = ','.join(['?' for _ in id_orcamento_filter_list])
            sql_query_comissao += f" WHERE foc.IdOrcamento IN ({placeholders})"
            query_params_comissao.extend(id_orcamento_filter_list)
        
        sql_query_comissao += " ORDER BY foc.Id"
        
        if self.limit_rows > 0:
            sql_query_comissao = sql_query_comissao.replace("ORDER BY foc.Id", 
                f"ORDER BY foc.Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        if query_params_comissao:
            cursor_sql.execute(sql_query_comissao, query_params_comissao)
        else:
            cursor_sql.execute(sql_query_comissao)
        
        # Carregar TODOS os dados na memória de uma vez (otimizado)
        print("[ETAPA 4] Carregando dados de Comissao na memória...")
        all_rows_comissao = cursor_sql.fetchall()
        cursor_sql.close()
        conn_sql.close()
        
        print(f"[ETAPA 4] {len(all_rows_orcamento)} registros de Orcamento e {len(all_rows_comissao)} registros de Comissao carregados. Processando conversões...")
        
        # Processar tudo em memória e preparar batch_values
        batch_values = []
        missing_contracts_orcamento = []  # Coletar IdOrcamento não encontrados
        
        # Processar Orcamento
        for row in all_rows_orcamento:
            try:
                legado_id_orcamento = row[0]  # Id
                legado_id_usuario = row[1]  # IdUsuarioVendedor
                
                # Mapear contract_id
                contract_uuid = self.contract_id_map.get(legado_id_orcamento)
                if not contract_uuid:
                    missing_contracts_orcamento.append(legado_id_orcamento)
                    continue
                
                # Mapear user_id
                user_uuid = user_id_map.get(legado_id_usuario)
                if not user_uuid:
                    error_msg = f"User nao encontrado: IdUsuarioVendedor={legado_id_usuario} para Orcamento={legado_id_orcamento}"
                    logger.warning(error_msg)
                    self.stats['errors'].append(error_msg)
                    continue
                
                batch_values.append((
                    str(contract_uuid),
                    str(user_uuid),
                    'hunter',  # seller_type para Orcamento
                    datetime.now(),
                    datetime.now()
                ))
                
            except Exception as e:
                error_msg = f"Erro ao preparar contract_seller de Orcamento Id={row[0]}: {e}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)
                continue
        
        # Processar Comissao
        missing_contracts_comissao = []  # Coletar IdOrcamento não encontrados
        for row in all_rows_comissao:
            try:
                legado_id_orcamento = row[0]  # IdOrcamento
                legado_id_usuario = row[1]  # IdUsuarioVendedor
                
                # Mapear contract_id
                contract_uuid = self.contract_id_map.get(legado_id_orcamento)
                if not contract_uuid:
                    missing_contracts_comissao.append(legado_id_orcamento)
                    continue
                
                # Mapear user_id
                user_uuid = user_id_map.get(legado_id_usuario)
                if not user_uuid:
                    error_msg = f"User nao encontrado: IdUsuarioVendedor={legado_id_usuario} para Comissao Orcamento={legado_id_orcamento}"
                    logger.warning(error_msg)
                    self.stats['errors'].append(error_msg)
                    continue
                
                batch_values.append((
                    str(contract_uuid),
                    str(user_uuid),
                    'commission',  # seller_type para Comissao
                    row[2] if row[2] else datetime.now(),  # DataInclusao
                    row[3] if row[3] else datetime.now()  # DataAlteracao
                ))
                
            except Exception as e:
                error_msg = f"Erro ao preparar contract_seller de Comissao IdOrcamento={row[0]}: {e}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)
                continue
        
        # Log de contracts não encontrados (agrupado)
        all_missing_contracts = list(set(missing_contracts_orcamento + missing_contracts_comissao))
        if all_missing_contracts:
            logger.warning(f"Contracts nao encontrados: IdOrcamento: {sorted(all_missing_contracts)}")
            self.stats['errors'].extend([f"Contract nao encontrado: IdOrcamento={id_orc}" for id_orc in all_missing_contracts])
        
        print(f"[ETAPA 4] {len(batch_values)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL e inserir em bulk
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Query de insert usando gen_random_uuid() - formato para execute_values
        insert_query = f"""
        INSERT INTO {schema}.contract_sellers (
            id, contract_id, user_id, seller_type, created_at, updated_at, status
        ) VALUES %s
        """
        insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, 'active')"
        
        chunk_num = 0
        total_processed = 0
        
        try:
            for i in range(0, len(batch_values), CHUNK_SIZE):
                chunk = batch_values[i:i + CHUNK_SIZE]
                chunk_num += 1
                
                if chunk:
                    try:
                        # Usar execute_values() para inserção otimizada em bulk
                        execute_values(
                            cursor_pg,
                            insert_query,
                            chunk,
                            template=insert_template,
                            page_size=CHUNK_SIZE,
                            fetch=False
                        )
                        
                        total_processed += len(chunk)
                        self.stats['contract_sellers'] += len(chunk)
                        
                        conn_pg.commit()
                        print(f"[ETAPA 4] Chunk {chunk_num} processado: {total_processed}/{len(batch_values)} registros inseridos")
                        logger.info(f"[ETAPA 4] Chunk {chunk_num} processado: {total_processed}/{len(batch_values)} registros inseridos")
                        
                    except Exception as batch_error:
                        error_msg = f"Erro ao inserir batch de contract_sellers: {batch_error}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
            
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\n[ETAPA 4] CONCLUIDA! Total de contract_sellers migrados: {self.stats['contract_sellers']}")
            logger.info(f"[ETAPA 4] CONCLUIDA! Total: {self.stats['contract_sellers']}")
            
            # Validação
            self.validate_step4_contract_sellers()
            
        except Exception as e:
            logger.error(f"Erro critico na ETAPA 4: {e}")
            if 'conn_pg' in locals():
                try:
                    conn_pg.rollback()
                    if 'cursor_pg' in locals():
                        cursor_pg.close()
                    conn_pg.close()
                except:
                    pass
            raise
    
    def validate_step5_contract_team_members(self):
        """Validação e relatório de qualidade - ETAPA 5"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 5: CONTRACT_TEAM_MEMBERS")
        print("-"*80)
        
        try:
            # Carregar filtros do step1
            filter_ids = self._get_filter_ids_for_validation()
            id_orcamento_list = filter_ids.get('IdOrcamento', [])
            
            # Contar origem: Orcamentos com IdUsuarioVendedor
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            if id_orcamento_list:
                placeholders = ','.join(['?' for _ in id_orcamento_list])
                cursor_sql.execute(f"""
                    SELECT COUNT(DISTINCT v.IdOrcamento) 
                    FROM ViewOrcamentosLojas v
                    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                    WHERE o.IdUsuarioVendedor IS NOT NULL
                    AND v.IdOrcamento IN ({placeholders})
                """, id_orcamento_list)
            elif self.limit_rows > 0:
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
            
            # Contar destino aplicando os mesmos filtros
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            if id_orcamento_list:
                # Contar apenas os contract_team_members relacionados aos contracts migrados
                cursor_pg.execute(f"""
                    SELECT COUNT(*) 
                    FROM {schema}.contract_team_members ctm
                    INNER JOIN {schema}.contracts c ON c.id = ctm.contract_id
                    WHERE c.legacy_id = ANY(%s)
                """, (id_orcamento_list,))
            else:
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
            # ⚠️ CRÍTICO: Usar conexão PRD diretamente quando destino for PRD
            if destino == 'PRD':
                conn_users = DatabaseConnection.get_postgresql_prd_destino_connection()
            else:
                conn_users = DatabaseConnection.get_postgresql_hml_destino_connection()
            cursor_users = conn_users.cursor()
            
            # Verificar se a coluna legacy_id existe
            cursor_users.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = '{schema_users}' 
                AND table_name = 'users' 
                AND column_name = 'legacy_id'
            """)
            has_legacy_id = cursor_users.fetchone() is not None
            
            if has_legacy_id:
                cursor_users.execute(f"SELECT id, legacy_id FROM {schema_users}.users WHERE legacy_id IS NOT NULL")
                for row in cursor_users.fetchall():
                    if row[1] is not None:
                        user_id_map[row[1]] = row[0]
                print(f"OK - {len(user_id_map)} users carregados")
                logger.info(f"Carregados {len(user_id_map)} users para mapeamento")
            else:
                print(f"AVISO - Tabela {schema_users}.users nao possui coluna legacy_id. Nao sera possivel mapear IdUsuarioVendedor.")
                logger.warning(f"Tabela {schema_users}.users nao possui coluna legacy_id. Mapeamento de users nao sera possivel.")
            
            cursor_users.close()
            conn_users.close()
        except Exception as e:
            logger.warning(f"Erro ao carregar users: {e}")
            print(f"AVISO - Nao foi possivel carregar users: {e}")
        
        # Carregar filtros do contracts (se existir)
        filter_data = self.load_filter_json()
        id_orcamento_filter_list = []
        
        if filter_data and 'aggregated_ids' in filter_data:
            id_orcamento_filter_list = filter_data['aggregated_ids'].get('IdOrcamento', [])
            logger.info(f"[ETAPA 5] Carregados {len(id_orcamento_filter_list)} IdOrcamento do arquivo de filtros")
            print(f"[ETAPA 5] Carregados {len(id_orcamento_filter_list)} IdOrcamento do arquivo de filtros")
        elif self.id_orcamento_filter:
            id_orcamento_filter_list = self.id_orcamento_filter
            logger.info(f"[ETAPA 5] Usando {len(id_orcamento_filter_list)} IdOrcamento dos filtros aplicados")
            print(f"[ETAPA 5] Usando {len(id_orcamento_filter_list)} IdOrcamento dos filtros aplicados")
        
        # Limpar tabela (TRUNCATE ou DELETE baseado em filtros)
        if not id_orcamento_filter_list:
            # Se não há filtros, usar TRUNCATE
            print("\n[ETAPA 5] Limpando tabela contract_team_members (sem filtros)...")
            self.truncate_table('contract_team_members')
        else:
            # Com filtros: usar DELETE
            print("\n[ETAPA 5] Limpando registros filtrados da tabela contract_team_members...")
            # Buscar contract_ids dos IdOrcamento filtrados para deletar
            contract_ids_to_delete = []
            for id_orc in id_orcamento_filter_list:
                contract_uuid = self.contract_id_map.get(id_orc)
                if contract_uuid:
                    contract_ids_to_delete.append(str(contract_uuid))
            
            if contract_ids_to_delete:
                schema = get_schema_atual()
                conn_pg = DatabaseConnection.get_postgresql_destino_connection()
                cursor_pg = conn_pg.cursor()
                placeholders = ','.join(['%s' for _ in contract_ids_to_delete])
                cursor_pg.execute(f"DELETE FROM {schema}.contract_team_members WHERE contract_id IN ({placeholders})", contract_ids_to_delete)
                deleted_count = cursor_pg.rowcount
                conn_pg.commit()
                logger.info(f"Tabela {schema}.contract_team_members: {deleted_count} registros deletados")
                print(f"OK - Tabela {schema}.contract_team_members: {deleted_count} registros deletados")
                cursor_pg.close()
                conn_pg.close()
        
        # Buscar dados do SQL Server (apenas seller - IdUsuarioVendedor)
        print("[ETAPA 5] Buscando dados do SQL Server...")
        query_params = []
        
        if id_orcamento_filter_list:
            # Aplicar filtro de IdOrcamento
            placeholders = ','.join(['?' for _ in id_orcamento_filter_list])
            sql_query = f"""
            SELECT DISTINCT
                v.IdOrcamento,
                o.IdUsuarioVendedor,
                v.DataInclusaoOrcamento,
                v.DataAlteracaoOrcamento
            FROM ViewOrcamentosLojas v
            INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
            WHERE o.IdUsuarioVendedor IS NOT NULL
              AND v.IdOrcamento IN ({placeholders})
            ORDER BY v.IdOrcamento
            """
            query_params.extend(id_orcamento_filter_list)
        else:
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
        if query_params:
            cursor_sql.execute(sql_query, query_params)
        else:
            cursor_sql.execute(sql_query)
        
        # Carregar TODOS os dados na memória de uma vez (otimizado)
        print("[ETAPA 5] Carregando dados na memória...")
        all_rows = cursor_sql.fetchall()
        cursor_sql.close()
        conn_sql.close()
        
        print(f"[ETAPA 5] {len(all_rows)} registros carregados. Processando conversões...")
        
        # Processar tudo em memória e preparar batch_values
        batch_values = []
        missing_contracts = []  # Coletar IdOrcamento não encontrados
        
        for row in all_rows:
            try:
                legado_id_orcamento = row[0]  # IdOrcamento
                legado_id_usuario = row[1]  # IdUsuarioVendedor
                
                # Mapear contract_id
                contract_uuid = self.contract_id_map.get(legado_id_orcamento)
                if not contract_uuid:
                    missing_contracts.append(legado_id_orcamento)
                    continue
                
                # Mapear user_id
                user_uuid = user_id_map.get(legado_id_usuario)
                if not user_uuid:
                    error_msg = f"User nao encontrado: IdUsuarioVendedor={legado_id_usuario} para Orcamento={legado_id_orcamento}"
                    logger.warning(error_msg)
                    self.stats['errors'].append(error_msg)
                    continue
                
                batch_values.append((
                    str(contract_uuid),
                    str(user_uuid),
                    'analyst',  # position sempre "analyst" na primeira carga
                    row[2] if row[2] else datetime.now(),  # DataInclusaoOrcamento
                    row[3] if row[3] else datetime.now()  # DataAlteracaoOrcamento
                ))
                
            except Exception as e:
                error_msg = f"Erro ao preparar contract_team_member IdOrcamento={row[0]}: {e}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)
                continue
        
        # Log de contracts não encontrados (agrupado)
        if missing_contracts:
            unique_missing = sorted(list(set(missing_contracts)))
            logger.warning(f"Contracts nao encontrados: IdOrcamento: {unique_missing}")
            self.stats['errors'].extend([f"Contract nao encontrado: IdOrcamento={id_orc}" for id_orc in unique_missing])
        
        print(f"[ETAPA 5] {len(batch_values)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL e inserir em bulk
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Query de insert usando gen_random_uuid() - formato para execute_values
        insert_query = f"""
        INSERT INTO {schema}.contract_team_members (
            id, contract_id, user_id, position, created_at, updated_at
        ) VALUES %s
        """
        insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s)"
        
        chunk_num = 0
        total_processed = 0
        
        try:
            for i in range(0, len(batch_values), CHUNK_SIZE):
                chunk = batch_values[i:i + CHUNK_SIZE]
                chunk_num += 1
                
                if chunk:
                    try:
                        # Usar execute_values() para inserção otimizada em bulk
                        execute_values(
                            cursor_pg,
                            insert_query,
                            chunk,
                            template=insert_template,
                            page_size=CHUNK_SIZE,
                            fetch=False
                        )
                        
                        total_processed += len(chunk)
                        self.stats['contract_team_members'] += len(chunk)
                        
                        conn_pg.commit()
                        print(f"[ETAPA 5] Chunk {chunk_num} processado: {total_processed}/{len(batch_values)} registros inseridos")
                        logger.info(f"[ETAPA 5] Chunk {chunk_num} processado: {total_processed}/{len(batch_values)} registros inseridos")
                        
                    except Exception as batch_error:
                        error_msg = f"Erro ao inserir batch de contract_team_members: {batch_error}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
            
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\n[ETAPA 5] CONCLUIDA! Total de contract_team_members migrados: {self.stats['contract_team_members']}")
            logger.info(f"[ETAPA 5] CONCLUIDA! Total: {self.stats['contract_team_members']}")
            
            # Validação
            self.validate_step5_contract_team_members()
            
        except Exception as e:
            logger.error(f"Erro critico na ETAPA 5: {e}")
            if 'conn_pg' in locals():
                try:
                    conn_pg.rollback()
                    if 'cursor_pg' in locals():
                        cursor_pg.close()
                    conn_pg.close()
                except:
                    pass
            raise


    def validate_step6_contract_contacts(self):
        """Validação e relatório de qualidade - ETAPA 6"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 6: CONTRACT_CONTACTS")
        print("-"*80)
        
        try:
            # Carregar filtros do step1
            filter_ids = self._get_filter_ids_for_validation()
            id_orcamento_list = filter_ids.get('IdOrcamento', [])
            
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            if id_orcamento_list:
                placeholders = ','.join(['?' for _ in id_orcamento_list])
                cursor_sql.execute(f"""
                    SELECT COUNT(DISTINCT IdOrcamento) 
                    FROM ViewOrcamentosLojas 
                    WHERE NomeCliente IS NOT NULL
                    AND IdOrcamento IN ({placeholders})
                """, id_orcamento_list)
            elif self.limit_rows > 0:
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
            
            if id_orcamento_list:
                placeholders = ','.join(['?' for _ in id_orcamento_list])
                cursor_sql.execute(f"""
                    SELECT COUNT(DISTINCT IdOrcamento) 
                    FROM ViewOrcamentosLojas 
                    WHERE NomeCliente IS NOT NULL 
                    AND NomeSistemaCliente IS NOT NULL 
                    AND NomeSistemaCliente != NomeCliente
                    AND IdOrcamento IN ({placeholders})
                """, id_orcamento_list)
            elif self.limit_rows > 0:
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
            
            # Contar destino aplicando os mesmos filtros
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            if id_orcamento_list:
                # Contar apenas os contract_contacts relacionados aos contracts migrados
                cursor_pg.execute(f"""
                    SELECT COUNT(*) 
                    FROM {schema}.contract_contacts cc
                    INNER JOIN {schema}.contracts c ON c.id = cc.contract_id
                    WHERE c.legacy_id = ANY(%s)
                """, (id_orcamento_list,))
            else:
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
        
        # Carregar filtros do contracts (se existir)
        filter_data = self.load_filter_json()
        id_orcamento_filter_list = []
        
        if filter_data and 'aggregated_ids' in filter_data:
            id_orcamento_filter_list = filter_data['aggregated_ids'].get('IdOrcamento', [])
            logger.info(f"[ETAPA 6] Carregados {len(id_orcamento_filter_list)} IdOrcamento do arquivo de filtros")
            print(f"[ETAPA 6] Carregados {len(id_orcamento_filter_list)} IdOrcamento do arquivo de filtros")
        elif self.id_orcamento_filter:
            id_orcamento_filter_list = self.id_orcamento_filter
            logger.info(f"[ETAPA 6] Usando {len(id_orcamento_filter_list)} IdOrcamento dos filtros aplicados")
            print(f"[ETAPA 6] Usando {len(id_orcamento_filter_list)} IdOrcamento dos filtros aplicados")
        
        # Limpar tabela (TRUNCATE ou DELETE baseado em filtros)
        if not id_orcamento_filter_list:
            print("\n[ETAPA 6] Limpando tabela contract_contacts (sem filtros)...")
            self.truncate_table('contract_contacts')
        else:
            print("\n[ETAPA 6] Limpando registros filtrados da tabela contract_contacts...")
            # Buscar contract_ids dos IdOrcamento filtrados para deletar
            contract_ids_to_delete = []
            for id_orc in id_orcamento_filter_list:
                contract_uuid = self.contract_id_map.get(id_orc)
                if contract_uuid:
                    contract_ids_to_delete.append(str(contract_uuid))
            
            if contract_ids_to_delete:
                schema = get_schema_atual()
                conn_pg = DatabaseConnection.get_postgresql_destino_connection()
                cursor_pg = conn_pg.cursor()
                placeholders = ','.join(['%s' for _ in contract_ids_to_delete])
                cursor_pg.execute(f"DELETE FROM {schema}.contract_contacts WHERE contract_id IN ({placeholders})", contract_ids_to_delete)
                deleted_count = cursor_pg.rowcount
                conn_pg.commit()
                logger.info(f"Tabela {schema}.contract_contacts: {deleted_count} registros deletados")
                print(f"OK - Tabela {schema}.contract_contacts: {deleted_count} registros deletados")
                cursor_pg.close()
                conn_pg.close()
        
        print("[ETAPA 6] Buscando dados do SQL Server...")
        query_params = []
        
        if id_orcamento_filter_list:
            # Aplicar filtro de IdOrcamento
            placeholders = ','.join(['?' for _ in id_orcamento_filter_list])
            sql_query = f"""
            SELECT DISTINCT
                v.IdOrcamento,
                v.NomeCliente,
                v.NomeSistemaCliente,
                v.DataInclusaoOrcamento,
                v.DataAlteracaoOrcamento
            FROM ViewOrcamentosLojas v
            WHERE v.NomeCliente IS NOT NULL
              AND v.IdOrcamento IN ({placeholders})
            ORDER BY v.IdOrcamento
            """
            query_params.extend(id_orcamento_filter_list)
        else:
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
        if query_params:
            cursor_sql.execute(sql_query, query_params)
        else:
            cursor_sql.execute(sql_query)
        
        # Carregar TODOS os dados na memória de uma vez (otimizado)
        print("[ETAPA 6] Carregando dados na memória...")
        all_rows = cursor_sql.fetchall()
        cursor_sql.close()
        conn_sql.close()
        
        print(f"[ETAPA 6] {len(all_rows)} registros carregados. Processando conversões...")
        
        # Processar tudo em memória e preparar batch_values (pode gerar múltiplos contatos por linha)
        batch_values = []
        missing_contracts = []  # Coletar IdOrcamento não encontrados
        
        for row in all_rows:
            try:
                legado_id_orcamento = row[0]
                nome_cliente = self.clean_string(row[1])
                nome_sistema = self.clean_string(row[2])
                
                contract_uuid = self.contract_id_map.get(legado_id_orcamento)
                if not contract_uuid:
                    missing_contracts.append(legado_id_orcamento)
                    continue
                
                if not nome_cliente:
                    continue
                
                created_at = row[3] if row[3] else datetime.now()
                updated_at = row[4] if row[4] else datetime.now()
                
                # Primeiro contato: NomeCliente
                batch_values.append((
                    str(contract_uuid),
                    nome_cliente,
                    created_at,
                    updated_at
                ))
                
                # Segundo contato: NomeSistemaCliente (se diferente)
                if nome_sistema and nome_sistema != nome_cliente:
                    batch_values.append((
                        str(contract_uuid),
                        nome_sistema,
                        created_at,
                        updated_at
                    ))
                
            except Exception as e:
                error_msg = f"Erro ao preparar contract_contact IdOrcamento={row[0]}: {e}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)
                continue
        
        # Log de contracts não encontrados (agrupado)
        if missing_contracts:
            unique_missing = sorted(list(set(missing_contracts)))
            logger.warning(f"Contracts nao encontrados: IdOrcamento: {unique_missing}")
            self.stats['errors'].extend([f"Contract nao encontrado: IdOrcamento={id_orc}" for id_orc in unique_missing])
        
        print(f"[ETAPA 6] {len(batch_values)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL e inserir em bulk
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Query de insert usando gen_random_uuid() - formato para execute_values
        insert_query = f"""
        INSERT INTO {schema}.contract_contacts (
            id, contract_id, name, created_at, updated_at
        ) VALUES %s
        """
        insert_template = f"(gen_random_uuid(), %s, %s, %s, %s)"
        
        chunk_num = 0
        total_processed = 0
        
        try:
            for i in range(0, len(batch_values), CHUNK_SIZE):
                chunk = batch_values[i:i + CHUNK_SIZE]
                chunk_num += 1
                
                if chunk:
                    try:
                        # Usar execute_values() para inserção otimizada em bulk
                        execute_values(
                            cursor_pg,
                            insert_query,
                            chunk,
                            template=insert_template,
                            page_size=CHUNK_SIZE,
                            fetch=False
                        )
                        
                        total_processed += len(chunk)
                        self.stats['contract_contacts'] += len(chunk)
                        
                        conn_pg.commit()
                        print(f"[ETAPA 6] Chunk {chunk_num} processado: {total_processed}/{len(batch_values)} registros inseridos")
                        logger.info(f"[ETAPA 6] Chunk {chunk_num} processado: {total_processed}/{len(batch_values)} registros inseridos")
                        
                    except Exception as batch_error:
                        error_msg = f"Erro ao inserir batch de contract_contacts: {batch_error}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
            
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\n[ETAPA 6] CONCLUIDA! Total de contract_contacts migrados: {self.stats['contract_contacts']}")
            logger.info(f"[ETAPA 6] CONCLUIDA! Total: {self.stats['contract_contacts']}")
            
            self.validate_step6_contract_contacts()
            
        except Exception as e:
            logger.error(f"Erro critico na ETAPA 6: {e}")
            if 'conn_pg' in locals():
                try:
                    conn_pg.rollback()
                    if 'cursor_pg' in locals():
                        cursor_pg.close()
                    conn_pg.close()
                except:
                    pass
            raise
    
    def validate_step7_contract_partners(self):
        """Validação e relatório de qualidade - ETAPA 7"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 7: CONTRACT_PARTNERS")
        print("-"*80)
        
        try:
            # Carregar filtros do step1
            filter_ids = self._get_filter_ids_for_validation()
            id_orcamento_list = filter_ids.get('IdOrcamento', [])
            
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            if id_orcamento_list:
                placeholders = ','.join(['?' for _ in id_orcamento_list])
                # ⚠️ CORRIGIDO: Contar todas as linhas (não DISTINCT IdOrcamento)
                # Cada IdClienteLoja é um registro diferente em contract_partners
                cursor_sql.execute(f"""
                    SELECT COUNT(*) 
                    FROM ViewOrcamentosLojas 
                    WHERE IdClienteLoja IS NOT NULL 
                    AND NomeClienteLoja IS NOT NULL
                    AND IdOrcamento IN ({placeholders})
                """, id_orcamento_list)
            elif self.limit_rows > 0:
                # ⚠️ CORRIGIDO: Contar todas as linhas (não DISTINCT IdOrcamento)
                cursor_sql.execute(f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT TOP {self.limit_rows} IdOrcamento, IdClienteLoja
                        FROM ViewOrcamentosLojas 
                        WHERE IdClienteLoja IS NOT NULL 
                        AND NomeClienteLoja IS NOT NULL
                        ORDER BY IdOrcamento
                    ) AS limited
                """)
            else:
                # ⚠️ CORRIGIDO: Contar todas as linhas (não DISTINCT IdOrcamento)
                cursor_sql.execute("SELECT COUNT(*) FROM ViewOrcamentosLojas WHERE IdClienteLoja IS NOT NULL AND NomeClienteLoja IS NOT NULL")
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino aplicando os mesmos filtros
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            # ⚠️ CRÍTICO: Usar conexão PRD diretamente quando destino for PRD
            if destino_nome == 'PRD':
                conn_pg = DatabaseConnection.get_postgresql_prd_destino_connection()
            else:
                conn_pg = DatabaseConnection.get_postgresql_hml_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            if id_orcamento_list:
                # Contar apenas os contract_partners relacionados aos contracts migrados
                cursor_pg.execute(f"""
                    SELECT COUNT(*) 
                    FROM {schema}.contract_partners cp
                    INNER JOIN {schema}.contracts c ON c.id = cp.contract_id
                    WHERE c.legacy_id = ANY(%s)
                """, (id_orcamento_list,))
            else:
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
        
        # NOTA: Tabela persons não existe em PRD. Campo person_id será sempre preenchido com UUID do Sys Admin.
        
        # Carregar filtros do contracts (se existir)
        filter_data = self.load_filter_json()
        id_orcamento_filter_list = []
        
        if filter_data and 'aggregated_ids' in filter_data:
            id_orcamento_filter_list = filter_data['aggregated_ids'].get('IdOrcamento', [])
            logger.info(f"[ETAPA 7] Carregados {len(id_orcamento_filter_list)} IdOrcamento do arquivo de filtros")
            print(f"[ETAPA 7] Carregados {len(id_orcamento_filter_list)} IdOrcamento do arquivo de filtros")
        elif self.id_orcamento_filter:
            id_orcamento_filter_list = self.id_orcamento_filter
            logger.info(f"[ETAPA 7] Usando {len(id_orcamento_filter_list)} IdOrcamento dos filtros aplicados")
            print(f"[ETAPA 7] Usando {len(id_orcamento_filter_list)} IdOrcamento dos filtros aplicados")
        
        # Limpar tabela (TRUNCATE ou DELETE baseado em filtros)
        if not id_orcamento_filter_list:
            print("\n[ETAPA 7] Limpando tabela contract_partners (sem filtros)...")
            self.truncate_table('contract_partners')
        else:
            print("\n[ETAPA 7] Limpando registros filtrados da tabela contract_partners...")
            # Buscar contract_ids dos IdOrcamento filtrados para deletar
            contract_ids_to_delete = []
            for id_orc in id_orcamento_filter_list:
                contract_uuid = self.contract_id_map.get(id_orc)
                if contract_uuid:
                    contract_ids_to_delete.append(str(contract_uuid))
            
            if contract_ids_to_delete:
                schema = get_schema_atual()
                conn_pg = DatabaseConnection.get_postgresql_destino_connection()
                cursor_pg = conn_pg.cursor()
                placeholders = ','.join(['%s' for _ in contract_ids_to_delete])
                cursor_pg.execute(f"DELETE FROM {schema}.contract_partners WHERE contract_id IN ({placeholders})", contract_ids_to_delete)
                deleted_count = cursor_pg.rowcount
                conn_pg.commit()
                logger.info(f"Tabela {schema}.contract_partners: {deleted_count} registros deletados")
                print(f"OK - Tabela {schema}.contract_partners: {deleted_count} registros deletados")
                cursor_pg.close()
                conn_pg.close()
        
        print("[ETAPA 7] Buscando dados do SQL Server...")
        query_params = []
        
        if id_orcamento_filter_list:
            # Aplicar filtro de IdOrcamento
            placeholders = ','.join(['?' for _ in id_orcamento_filter_list])
            sql_query = f"""
            SELECT DISTINCT
                v.IdOrcamento,
                v.IdClienteLoja,
                v.NomeClienteLoja,
                v.DataInclusaoOrcamento,
                v.DataAlteracaoOrcamento
            FROM ViewOrcamentosLojas v
            WHERE v.IdClienteLoja IS NOT NULL AND v.NomeClienteLoja IS NOT NULL
              AND v.IdOrcamento IN ({placeholders})
            ORDER BY v.IdOrcamento
            """
            query_params.extend(id_orcamento_filter_list)
        else:
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
        if query_params:
            cursor_sql.execute(sql_query, query_params)
        else:
            cursor_sql.execute(sql_query)
        
        # Carregar TODOS os dados na memória de uma vez (otimizado)
        print("[ETAPA 7] Carregando dados na memória...")
        all_rows = cursor_sql.fetchall()
        cursor_sql.close()
        conn_sql.close()
        
        print(f"[ETAPA 7] {len(all_rows)} registros carregados. Processando conversões...")
        
        # Processar tudo em memória e preparar batch_values
        batch_values = []
        missing_contracts = []  # Coletar IdOrcamento não encontrados
        
        for row in all_rows:
            try:
                legado_id_orcamento = row[0]
                legado_id_cliente_loja = row[1]
                
                contract_uuid = self.contract_id_map.get(legado_id_orcamento)
                if not contract_uuid:
                    missing_contracts.append(legado_id_orcamento)
                    continue
                
                # person_id sempre usa UUID do Sys Admin (tabela persons não existe em PRD)
                SYS_ADMIN_USER_UUID = 'b1d3a1a3-580b-4db4-92e1-0b7cb66ffe9f'
                person_uuid = SYS_ADMIN_USER_UUID
                
                batch_values.append((
                    str(contract_uuid),
                    str(person_uuid),
                    None,  # position
                    None,  # phone
                    None,  # email
                    row[3] if row[3] else datetime.now(),  # created_at
                    row[4] if row[4] else datetime.now()  # updated_at
                ))
                
            except Exception as e:
                error_msg = f"Erro ao preparar contract_partner IdOrcamento={row[0]}: {e}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)
                continue
        
        # Log de contracts não encontrados (agrupado)
        if missing_contracts:
            unique_missing = sorted(list(set(missing_contracts)))
            logger.warning(f"Contracts nao encontrados: IdOrcamento: {unique_missing}")
            self.stats['errors'].extend([f"Contract nao encontrado: IdOrcamento={id_orc}" for id_orc in unique_missing])
        
        print(f"[ETAPA 7] {len(batch_values)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL e inserir em bulk
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Query de insert usando gen_random_uuid() - formato para execute_values
        insert_query = f"""
        INSERT INTO {schema}.contract_partners (
            id, contract_id, person_id, position, phone, email, created_at, updated_at
        ) VALUES %s
        """
        insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s)"
        
        chunk_num = 0
        total_processed = 0
        
        try:
            for i in range(0, len(batch_values), CHUNK_SIZE):
                chunk = batch_values[i:i + CHUNK_SIZE]
                chunk_num += 1
                
                if chunk:
                    try:
                        # Usar execute_values() para inserção otimizada em bulk
                        execute_values(
                            cursor_pg,
                            insert_query,
                            chunk,
                            template=insert_template,
                            page_size=CHUNK_SIZE,
                            fetch=False
                        )
                        
                        total_processed += len(chunk)
                        self.stats['contract_partners'] += len(chunk)
                        
                        conn_pg.commit()
                        print(f"[ETAPA 7] Chunk {chunk_num} processado: {total_processed}/{len(batch_values)} registros inseridos")
                        logger.info(f"[ETAPA 7] Chunk {chunk_num} processado: {total_processed}/{len(batch_values)} registros inseridos")
                        
                    except Exception as batch_error:
                        error_msg = f"Erro ao inserir batch de contract_partners: {batch_error}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
            
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\n[ETAPA 7] CONCLUIDA! Total de contract_partners migrados: {self.stats['contract_partners']}")
            logger.info(f"[ETAPA 7] CONCLUIDA! Total: {self.stats['contract_partners']}")
            
            self.validate_step7_contract_partners()
            
        except Exception as e:
            logger.error(f"Erro critico na ETAPA 7: {e}")
            if 'conn_pg' in locals():
                try:
                    conn_pg.rollback()
                    if 'cursor_pg' in locals():
                        cursor_pg.close()
                    conn_pg.close()
                except:
                    pass
            raise
    
    def validate_step8_contract_additional_charges(self):
        """Validação e relatório de qualidade - ETAPA 8"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 8: CONTRACT_ADDITIONAL_CHARGES")
        print("-"*80)
        
        try:
            # Carregar filtros do step1
            filter_ids = self._get_filter_ids_for_validation()
            id_orcamento_list = filter_ids.get('IdOrcamento', [])
            
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            if id_orcamento_list:
                placeholders = ','.join(['?' for _ in id_orcamento_list])
                cursor_sql.execute(f"""
                    SELECT 
                        SUM(CASE WHEN (ValorEPI > 0 OR CobrarEPI = 1) THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN ValorTradeMarketing > 0 THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN ValorOutros > 0 THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN Juros > 0 THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN Desconto > 0 THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN Multa > 0 THEN 1 ELSE 0 END)
                    FROM Orcamento
                    WHERE Id IN ({placeholders})
                """, id_orcamento_list)
            elif self.limit_rows > 0:
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
            
            # Contar destino aplicando os mesmos filtros
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            if id_orcamento_list:
                # Contar apenas os contract_additional_charges relacionados aos contracts migrados
                cursor_pg.execute(f"""
                    SELECT COUNT(*) 
                    FROM {schema}.contract_additional_charges cac
                    INNER JOIN {schema}.contracts c ON c.id = cac.contract_id
                    WHERE c.legacy_id = ANY(%s)
                """, (id_orcamento_list,))
            else:
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
        
        # Carregar filtros do contracts (se existir)
        filter_data = self.load_filter_json()
        id_orcamento_filter_list = []
        
        if filter_data and 'aggregated_ids' in filter_data:
            id_orcamento_filter_list = filter_data['aggregated_ids'].get('IdOrcamento', [])
            logger.info(f"[ETAPA 8] Carregados {len(id_orcamento_filter_list)} IdOrcamento do arquivo de filtros")
            print(f"[ETAPA 8] Carregados {len(id_orcamento_filter_list)} IdOrcamento do arquivo de filtros")
        elif self.id_orcamento_filter:
            id_orcamento_filter_list = self.id_orcamento_filter
            logger.info(f"[ETAPA 8] Usando {len(id_orcamento_filter_list)} IdOrcamento dos filtros aplicados")
            print(f"[ETAPA 8] Usando {len(id_orcamento_filter_list)} IdOrcamento dos filtros aplicados")
        
        # Limpar tabela (TRUNCATE ou DELETE baseado em filtros)
        if not id_orcamento_filter_list:
            print("\n[ETAPA 8] Limpando tabela contract_additional_charges (sem filtros)...")
            self.truncate_table('contract_additional_charges')
        else:
            print("\n[ETAPA 8] Limpando registros filtrados da tabela contract_additional_charges...")
            # Buscar contract_ids dos IdOrcamento filtrados para deletar
            contract_ids_to_delete = []
            for id_orc in id_orcamento_filter_list:
                contract_uuid = self.contract_id_map.get(id_orc)
                if contract_uuid:
                    contract_ids_to_delete.append(str(contract_uuid))
            
            if contract_ids_to_delete:
                schema = get_schema_atual()
                conn_pg = DatabaseConnection.get_postgresql_destino_connection()
                cursor_pg = conn_pg.cursor()
                placeholders = ','.join(['%s' for _ in contract_ids_to_delete])
                cursor_pg.execute(f"DELETE FROM {schema}.contract_additional_charges WHERE contract_id IN ({placeholders})", contract_ids_to_delete)
                deleted_count = cursor_pg.rowcount
                conn_pg.commit()
                logger.info(f"Tabela {schema}.contract_additional_charges: {deleted_count} registros deletados")
                print(f"OK - Tabela {schema}.contract_additional_charges: {deleted_count} registros deletados")
                cursor_pg.close()
                conn_pg.close()
        
        print("[ETAPA 8] Buscando dados do SQL Server...")
        query_params = []
        
        if id_orcamento_filter_list:
            # Aplicar filtro de IdOrcamento
            placeholders = ','.join(['?' for _ in id_orcamento_filter_list])
            sql_query = f"""
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
            WHERE Id IN ({placeholders})
              AND ((ValorEPI > 0 OR CobrarEPI = 1)
               OR ValorTradeMarketing > 0
               OR ValorOutros > 0
               OR Juros > 0
               OR Desconto > 0
               OR Multa > 0)
            ORDER BY Id
            """
            query_params.extend(id_orcamento_filter_list)
        else:
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
        if query_params:
            cursor_sql.execute(sql_query, query_params)
        else:
            cursor_sql.execute(sql_query)
        
        # Carregar TODOS os dados na memória de uma vez (otimizado)
        print("[ETAPA 8] Carregando dados na memória...")
        all_rows = cursor_sql.fetchall()
        cursor_sql.close()
        conn_sql.close()
        
        print(f"[ETAPA 8] {len(all_rows)} registros carregados. Processando conversões...")
        
        # Processar tudo em memória e preparar batch_values (pode gerar múltiplos charges por linha)
        batch_values = []
        missing_contracts = []  # Coletar IdOrcamento não encontrados
        
        for row in all_rows:
            try:
                legado_id_orcamento = row[0]
                
                contract_uuid = self.contract_id_map.get(legado_id_orcamento)
                if not contract_uuid:
                    missing_contracts.append(legado_id_orcamento)
                    continue
                
                created_at_base = row[9] if row[9] else datetime.now()
                updated_at_base = row[10] if row[10] else datetime.now()
                
                # REGISTRO 1: EPI
                if (row[1] and row[1] > 0) or (row[4] is True):
                    # billing_model: se tem InicioCobrancaEPI, é recurring (cobrança recorrente), senão one_time
                    # # Anterior
                    # billing_model_epi = 'recurring' if row[5] else 'one_time'

                    #Novo = enum
                    billing_model_epi = 'monthly'

                    batch_values.append((
                        str(contract_uuid),
                        row[1] if row[1] else 0.0,
                        # anterior
                        # 'epi',
                        'other',
                        billing_model_epi,
                        row[5] if row[5] else created_at_base,  # InicioCobrancaEPI ou DataInclusao
                        updated_at_base
                    ))
                
                # REGISTRO 2: Trade Marketing
                if row[2] and row[2] > 0:
                    batch_values.append((
                        str(contract_uuid),
                        row[2],
                        # anterior
                        # 'trade_marketing',
                        'other',
                        # anterior
                        # 'recurring',
                        'monthly',
                        created_at_base,
                        updated_at_base
                    ))
                
                # REGISTRO 3: Outros
                if row[3] and row[3] > 0:
                    batch_values.append((
                        str(contract_uuid),
                        row[3],
                        # anterior
                        # 'others',
                        'other',
                        # anterior
                        # 'one_time',
                        'monthly',
                        created_at_base,
                        updated_at_base
                    ))
                
                # REGISTRO 4: Juros
                if row[6] and row[6] > 0:
                    batch_values.append((
                        str(contract_uuid),
                        row[6],
                        # anterior
                        # 'interest',
                        'other',
                        # anterior
                        # 'recurring',
                        'monthly',
                        created_at_base,
                        updated_at_base
                    ))
                
                # REGISTRO 5: Desconto
                if row[7] and row[7] > 0:
                    batch_values.append((
                        str(contract_uuid),
                        row[7],
                        # anterior
                        # 'discount',
                        'other',
                        # anterior
                        # 'one_time',
                        'monthly',
                        created_at_base,
                        updated_at_base
                    ))
                
                # REGISTRO 6: Multa
                if row[8] and row[8] > 0:
                    batch_values.append((
                        str(contract_uuid),
                        row[8],
                        # anterior
                        # 'fine',
                        'other',
                        # anterior
                        # 'one_time',
                        'monthly',
                        created_at_base,
                        updated_at_base
                    ))
                
            except Exception as e:
                error_msg = f"Erro ao preparar contract_additional_charge IdOrcamento={row[0]}: {e}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)
                continue
        
        # Log de contracts não encontrados (agrupado)
        if missing_contracts:
            unique_missing = sorted(list(set(missing_contracts)))
            logger.warning(f"Contracts nao encontrados: IdOrcamento: {unique_missing}")
            self.stats['errors'].extend([f"Contract nao encontrado: IdOrcamento={id_orc}" for id_orc in unique_missing])
        
        print(f"[ETAPA 8] {len(batch_values)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL e inserir em bulk
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Query de insert usando gen_random_uuid() - formato para execute_values
        insert_query = f"""
        INSERT INTO {schema}.contract_additional_charges (
            id, contract_id, amount, charge_type, billing_model, created_at, updated_at
        ) VALUES %s
        """
        insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s)"
        
        chunk_num = 0
        total_processed = 0
        
        try:
            for i in range(0, len(batch_values), CHUNK_SIZE):
                chunk = batch_values[i:i + CHUNK_SIZE]
                chunk_num += 1
                
                if chunk:
                    try:
                        # Usar execute_values() para inserção otimizada em bulk
                        execute_values(
                            cursor_pg,
                            insert_query,
                            chunk,
                            template=insert_template,
                            page_size=CHUNK_SIZE,
                            fetch=False
                        )
                        
                        total_processed += len(chunk)
                        self.stats['contract_additional_charges'] += len(chunk)
                        
                        conn_pg.commit()
                        print(f"[ETAPA 8] Chunk {chunk_num} processado: {total_processed}/{len(batch_values)} registros inseridos")
                        logger.info(f"[ETAPA 8] Chunk {chunk_num} processado: {total_processed}/{len(batch_values)} registros inseridos")
                        
                    except Exception as batch_error:
                        error_msg = f"Erro ao inserir batch de contract_additional_charges: {batch_error}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
            
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\n[ETAPA 8] CONCLUIDA! Total de contract_additional_charges migrados: {self.stats['contract_additional_charges']}")
            logger.info(f"[ETAPA 8] CONCLUIDA! Total: {self.stats['contract_additional_charges']}")
            
            self.validate_step8_contract_additional_charges()
            
        except Exception as e:
            logger.error(f"Erro critico na ETAPA 8: {e}")
            if 'conn_pg' in locals():
                try:
                    conn_pg.rollback()
                    if 'cursor_pg' in locals():
                        cursor_pg.close()
                    conn_pg.close()
                except:
                    pass
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
            self.step9_migrate_promoter_tasks()  # Executar antes do step2
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
            print(f"  Promoter Tasks: {self.stats['promoter_tasks']}")
            print(f"  Contract Scenarios: {self.stats['contract_scenarios']}")
            print(f"  Contract Scenario Stores: {self.stats['contract_scenario_stores']}")
            print(f"  Contract Sellers: {self.stats['contract_sellers']}")
            print(f"  Contract Team Members: {self.stats['contract_team_members']}")
            print(f"  Contract Contacts: {self.stats['contract_contacts']}")
            print(f"  Contract Partners: {self.stats['contract_partners']}")
            print(f"  Contract Additional Charges: {self.stats['contract_additional_charges']}")
            print(f"  Promoter Tasks: {self.stats['promoter_tasks']}")
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
            logger.info(f"Promoter Tasks: {self.stats['promoter_tasks']}")
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
