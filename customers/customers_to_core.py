"""
Script de migração de dados: SQL Server PRD -> PostgreSQL Destino
Migra dados das tabelas: customers, customer_segments, addresses, contacts
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
# HML: gmcore
# PRD: core
SCHEMA_HML = 'gmcore'
SCHEMA_PRD = 'core'

# Schema atual será determinado automaticamente baseado no destino configurado
def get_schema_atual():
    """Retorna o schema atual baseado no destino configurado"""
    destino = DatabaseConnection.get_destino()
    if destino == 'PRD':
        return SCHEMA_PRD
    else:
        return SCHEMA_HML

# Configurar destino padrão (pode ser alterado via orchestrator)
# DatabaseConnection.set_destino('HML')  # ou 'PRD'

# Configurar logging
# Criar logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Remover handlers existentes para evitar duplicacao
if logger.handlers:
    logger.handlers.clear()

# Handler para arquivo (modo 'a' para append - log é truncado apenas no orchestrator)
# Arquivo na raiz do projeto
try:
    log_file_path = os.path.join(os.path.dirname(__file__), '..', 'log_execution.txt')
    log_file_path = os.path.abspath(log_file_path)  # Converter para caminho absoluto
    file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
except Exception as e:
    # Se houver erro, apenas criar handler de console
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


class CustomersMigration:
    """Classe para executar a migração de dados"""
    
    def __init__(self, limit_rows=0, id_orcamento_filter=None, 
                 data_aviso_previo_min=None, data_inicio_operacao_max=None, clear_data=False):
        self.stats = {
            'customers': 0,
            'customer_segments': 0,
            'addresses': 0,
            'contacts': 0,
            'errors': []
        }
        self.customer_id_map = {}  # Map: legado_id -> uuid
        self.segment_id_map = {}   # Map: legado_id -> uuid
        self.limit_rows = limit_rows  # 0 = todos, > 0 = limitar quantidade
        self.id_orcamento_filter = id_orcamento_filter  # Lista de IdOrcamento para filtrar
        self.data_aviso_previo_min = data_aviso_previo_min  # Data mínima para DataAvisoPrevio
        self.data_inicio_operacao_max = data_inicio_operacao_max  # Data máxima para DataInicioOperacao
        self.clear_data = clear_data  # Se True, força TRUNCATE mesmo com filtros aplicados
        self.json_updated_this_run = False  # Flag para evitar múltiplas atualizações do JSON na mesma execução
        
        # Caminho do arquivo JSON de filtros do contracts
        contracts_dir = os.path.join(os.path.dirname(__file__), '..', 'contracts')
        self.filter_json_path = os.path.join(contracts_dir, 'contracts_filter_main.json')
    
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
    
    def clean_cpf_cnpj(self, value: Optional[str]) -> Optional[str]:
        """Remove formatação de CPF/CNPJ"""
        if not value:
            return None
        import re
        cleaned = re.sub(r'[^\d]', '', str(value))
        if not cleaned or cleaned == '0' * len(cleaned):
            return None
        return cleaned
    
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
    
    def clean_cpf_cnpj_vectorized(self, series: pd.Series) -> pd.Series:
        """Remove formatação de CPF/CNPJ de forma vetorizada"""
        import re
        cleaned = series.astype(str)
        cleaned = cleaned.replace(['nan', 'None', 'NULL', 'NONE'], '')
        cleaned = cleaned.str.replace(r'[^\d]', '', regex=True)
        cleaned = cleaned.apply(lambda x: None if x and x == '0' * len(x) else x)
        cleaned = cleaned.replace('', None)
        return cleaned
    
    def convert_status(self, ativo: Optional[bool]) -> str:
        """Converte status booleano para string"""
        if ativo is True:
            return 'active'
        return 'inactive'
    
    def truncate_table(self, table_name: str, schema: str = None):
        """Faz TRUNCATE em uma tabela (apenas para tabelas não polimórficas)"""
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
    
    def delete_polymorphic_table(self, table_name: str, entity_type: str, type_column: str, schema: str = None):
        """
        Faz DELETE em tabela polimórfica filtrando por tipo de entidade
        Exemplo: delete_polymorphic_table('contacts', 'customers', 'contactable_type')
        """
        if schema is None:
            schema = get_schema_atual()
        
        conn = None
        try:
            conn = DatabaseConnection.get_postgresql_destino_connection()
            cursor = conn.cursor()
            
            query = f"DELETE FROM {schema}.{table_name} WHERE {type_column} = %s"
            cursor.execute(query, (entity_type,))
            deleted_count = cursor.rowcount
            conn.commit()
            
            logger.info(f"Tabela {schema}.{table_name}: {deleted_count} registros deletados (tipo: {entity_type})")
            print(f"OK - Tabela {schema}.{table_name}: {deleted_count} registros deletados (tipo: {entity_type})")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Erro ao deletar registros da tabela {schema}.{table_name} (tipo: {entity_type}): {e}")
            if conn:
                conn.rollback()
                conn.close()
            raise
    
    def load_filter_json(self) -> Optional[Dict]:
        """
        Carrega arquivo JSON com filtros e IDs agregados do contracts
        
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
                logger.info(f"Arquivo de filtros não encontrado: {self.filter_json_path} (buscando diretamente da ViewOrcamentosLojas)")
                return None
        except Exception as e:
            logger.error(f"Erro ao carregar arquivo de filtros: {e}")
            return None
    
    def save_filter_json_from_view(self, id_orcamento_list=None):
        """
        Atualiza o JSON de filtros do contracts com IDs coletados da ViewOrcamentosLojas
        Chamado quando customers busca diretamente da ViewOrcamentosLojas (com ou sem LIMIT)
        Aplica os mesmos filtros do JSON existente (DataAvisoPrevio, DataInicioOperacao)
        Usa queries separadas para cada tipo de ID para garantir resultados corretos
        
        Args:
            id_orcamento_list: Lista opcional de IdOrcamento para filtrar (se None, usa self.id_orcamento_filter)
        """
        try:
            print("[CUSTOMERS] Coletando IDs únicos da ViewOrcamentosLojas para atualizar JSON...")
            
            # Usar id_orcamento_list fornecido ou self.id_orcamento_filter
            id_orcamento_to_use = id_orcamento_list if id_orcamento_list is not None else self.id_orcamento_filter
            
            # Carregar JSON existente para pegar filtros de data (se não foram fornecidos no objeto)
            existing_data = self.load_filter_json()
            data_aviso_previo_min = self.data_aviso_previo_min
            data_inicio_operacao_max = self.data_inicio_operacao_max
            
            # Se não foram fornecidos no objeto, tentar pegar do JSON existente
            if not data_aviso_previo_min and existing_data and 'filters_applied' in existing_data:
                filters = existing_data['filters_applied']
                data_aviso_previo_min = filters.get('data_aviso_previo_min')
            
            if not data_inicio_operacao_max and existing_data and 'filters_applied' in existing_data:
                filters = existing_data['filters_applied']
                data_inicio_operacao_max = filters.get('data_inicio_operacao_max')
            
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            # Construir WHERE clause comum
            where_conditions = []
            query_params = []
            
            # Filtro IdOrcamento (usar id_orcamento_to_use)
            if id_orcamento_to_use:
                placeholders = ','.join(['?' for _ in id_orcamento_to_use])
                where_conditions.append(f"v.IdOrcamento IN ({placeholders})")
                query_params.extend(id_orcamento_to_use)
            
            # Filtro DataAvisoPrevio (data mínima)
            if data_aviso_previo_min:
                where_conditions.append("(CONVERT(DATE, v.DataAvisoPrevio) >= ? OR v.DataAvisoPrevio IS NULL)")
                query_params.append(data_aviso_previo_min)
            
            # Filtro DataInicioOperacao (data máxima)
            if data_inicio_operacao_max:
                where_conditions.append("CONVERT(DATE, v.DataInicioOperacao) <= ?")
                query_params.append(data_inicio_operacao_max)
            
            where_clause = ""
            if where_conditions:
                where_clause = " WHERE " + " AND ".join(where_conditions)
            
            # ⚠️ LÓGICA SIMPLIFICADA: Query direta sem subquery (conforme validação)
            # Função auxiliar para substituir parâmetros nas queries (para debug no SSMS)
            def format_query_for_ssms(query, params):
                """Substitui placeholders (?) pelos valores reais para execução no SSMS"""
                if not params:
                    return query
                formatted = query
                param_idx = 0
                while '?' in formatted and param_idx < len(params):
                    param = params[param_idx]
                    if isinstance(param, str):
                        escaped_param = param.replace("'", "''")
                        formatted = formatted.replace('?', f"'{escaped_param}'", 1)
                    elif isinstance(param, (int, float)):
                        formatted = formatted.replace('?', str(param), 1)
                    elif param is None:
                        formatted = formatted.replace('?', 'NULL', 1)
                    else:
                        formatted = formatted.replace('?', str(param), 1)
                    param_idx += 1
                return formatted
            
            # Executar query separada para cada tipo de ID usando estrutura simplificada
            aggregated_ids = {}
            
            # IdOrcamento
            if self.limit_rows > 0:
                query_id_orcamento = f"""
                SELECT DISTINCT TOP {self.limit_rows}
                    v.IdOrcamento
                FROM ViewOrcamentosLojas v
                INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                {where_clause} AND v.IdOrcamento IS NOT NULL
                """
            else:
                query_id_orcamento = f"""
                SELECT DISTINCT
                    v.IdOrcamento
                FROM ViewOrcamentosLojas v
                INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                {where_clause} AND v.IdOrcamento IS NOT NULL
                """
            if query_params:
                cursor_sql.execute(query_id_orcamento, query_params)
            else:
                cursor_sql.execute(query_id_orcamento)
            aggregated_ids['IdOrcamento'] = [row[0] for row in cursor_sql.fetchall()]
            
            # IdCliente
            if self.limit_rows > 0:
                query_id_cliente = f"""
                SELECT DISTINCT TOP {self.limit_rows}
                    v.IdCliente
                FROM ViewOrcamentosLojas v
                INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                {where_clause} AND v.IdCliente IS NOT NULL
                """
            else:
                query_id_cliente = f"""
                SELECT DISTINCT
                    v.IdCliente
                FROM ViewOrcamentosLojas v
                INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                {where_clause} AND v.IdCliente IS NOT NULL
                """
            if query_params:
                cursor_sql.execute(query_id_cliente, query_params)
            else:
                cursor_sql.execute(query_id_cliente)
            aggregated_ids['IdCliente'] = [row[0] for row in cursor_sql.fetchall()]
            
            # IdEstabelecimento
            if self.limit_rows > 0:
                query_id_estabelecimento = f"""
                SELECT DISTINCT TOP {self.limit_rows}
                    v.IdEstabelecimento
                FROM ViewOrcamentosLojas v
                INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                {where_clause} AND v.IdEstabelecimento IS NOT NULL
                """
            else:
                query_id_estabelecimento = f"""
                SELECT DISTINCT
                    v.IdEstabelecimento
                FROM ViewOrcamentosLojas v
                INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                {where_clause} AND v.IdEstabelecimento IS NOT NULL
                """
            if query_params:
                cursor_sql.execute(query_id_estabelecimento, query_params)
            else:
                cursor_sql.execute(query_id_estabelecimento)
            aggregated_ids['IdEstabelecimento'] = [row[0] for row in cursor_sql.fetchall()]
            
            # IdBandeira
            if self.limit_rows > 0:
                query_id_bandeira = f"""
                SELECT DISTINCT TOP {self.limit_rows}
                    v.IdBandeira
                FROM ViewOrcamentosLojas v
                INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                {where_clause} AND v.IdBandeira IS NOT NULL
                """
            else:
                query_id_bandeira = f"""
                SELECT DISTINCT
                    v.IdBandeira
                FROM ViewOrcamentosLojas v
                INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                {where_clause} AND v.IdBandeira IS NOT NULL
                """
            if query_params:
                cursor_sql.execute(query_id_bandeira, query_params)
            else:
                cursor_sql.execute(query_id_bandeira)
            aggregated_ids['IdBandeira'] = [row[0] for row in cursor_sql.fetchall()]
            
            # IdRede
            if self.limit_rows > 0:
                query_id_rede = f"""
                SELECT DISTINCT TOP {self.limit_rows}
                    v.IdRede
                FROM ViewOrcamentosLojas v
                INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                {where_clause} AND v.IdRede IS NOT NULL
                """
            else:
                query_id_rede = f"""
                SELECT DISTINCT
                    v.IdRede
                FROM ViewOrcamentosLojas v
                INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                {where_clause} AND v.IdRede IS NOT NULL
                """
            if query_params:
                cursor_sql.execute(query_id_rede, query_params)
            else:
                cursor_sql.execute(query_id_rede)
            aggregated_ids['IdRede'] = [row[0] for row in cursor_sql.fetchall()]
            
            # Log das queries para debug
            logger.info("="*80)
            logger.info("[CUSTOMERS] DEBUG - QUERIES PARA COLETA DE IDs (ESTRUTURA SIMPLIFICADA)")
            logger.info("="*80)
            logger.info(f"[CUSTOMERS] Parâmetros da query: {query_params}")
            logger.info("="*80)
            
            # Log da query formatada para SSMS (exemplo com IdCliente)
            logger.info("="*80)
            logger.info("[CUSTOMERS] QUERY PRONTA PARA COPIAR E COLAR NO SSMS (Exemplo: IdCliente)")
            logger.info("="*80)
            if query_params:
                logger.info("\n-- Query para IdCliente:")
                logger.info(format_query_for_ssms(query_id_cliente, query_params))
            else:
                logger.info("\n-- Query para IdCliente:")
                logger.info(query_id_cliente)
            logger.info("="*80)
            
            # Log dos resultados
            for id_type, id_list in aggregated_ids.items():
                preview = id_list[:10] if id_list else []
                logger.info(f"[CUSTOMERS] Resultado {id_type}: {len(id_list)} registros - {preview}")
            
            cursor_sql.close()
            conn_sql.close()
            
            print(f"[CUSTOMERS] IDs coletados: {len(aggregated_ids['IdOrcamento'])} contratos, "
                  f"{len(aggregated_ids['IdCliente'])} clientes, "
                  f"{len(aggregated_ids['IdEstabelecimento'])} estabelecimentos, "
                  f"{len(aggregated_ids['IdBandeira'])} bandeiras, "
                  f"{len(aggregated_ids['IdRede'])} redes")
            
            # Preparar estrutura do JSON (preservar filtros existentes)
            # Se há filtro de IdOrcamento aplicado, usar o mesmo valor em aggregated_ids (evitar duplicação)
            id_orcamento_for_filters = id_orcamento_to_use if id_orcamento_to_use else (existing_data.get('filters_applied', {}).get('id_orcamento', []) if existing_data else [])
            id_orcamento_for_aggregated = sorted(id_orcamento_for_filters) if id_orcamento_for_filters else sorted(aggregated_ids['IdOrcamento'])
            
            filter_data = {
                'filters_applied': {
                    'id_orcamento': id_orcamento_for_filters if id_orcamento_for_filters else [],
                    'data_aviso_previo_min': data_aviso_previo_min,
                    'data_inicio_operacao_max': data_inicio_operacao_max,
                    'limit_rows': self.limit_rows,
                    'clear_data': existing_data.get('filters_applied', {}).get('clear_data', False) if existing_data else False
                },
                'aggregated_ids': {
                    # Se há filtro de IdOrcamento, usar o mesmo valor (evitar duplicação)
                    'IdOrcamento': id_orcamento_for_aggregated,
                    'IdCliente': sorted([x for x in aggregated_ids['IdCliente'] if x is not None]),
                    'IdEstabelecimento': sorted([x for x in aggregated_ids['IdEstabelecimento'] if x is not None]),
                    'IdBandeira': sorted([x for x in aggregated_ids['IdBandeira'] if x is not None]),
                    'IdRede': sorted([x for x in aggregated_ids['IdRede'] if x is not None])
                },
                'execution_info': {
                    'timestamp': datetime.now().isoformat(),
                    'source': 'customers',
                    'total_customers_migrated': self.stats.get('customers', 0)
                }
            }
            
            # Salvar JSON
            with open(self.filter_json_path, 'w', encoding='utf-8') as f:
                json.dump(filter_data, f, indent=2, ensure_ascii=False)
            
            print(f"[CUSTOMERS] JSON atualizado: {len(aggregated_ids['IdOrcamento'])} contratos, "
                  f"{len(aggregated_ids['IdCliente'])} clientes, "
                  f"{len(aggregated_ids['IdEstabelecimento'])} estabelecimentos, "
                  f"{len(aggregated_ids['IdBandeira'])} bandeiras, "
                  f"{len(aggregated_ids['IdRede'])} redes")
            logger.info(f"[CUSTOMERS] JSON atualizado: {self.filter_json_path}")
            logger.info(f"[CUSTOMERS] IDs coletados - Contratos: {len(aggregated_ids['IdOrcamento'])}, "
                       f"Clientes: {len(aggregated_ids['IdCliente'])}, "
                       f"Estabelecimentos: {len(aggregated_ids['IdEstabelecimento'])}, "
                       f"Bandeiras: {len(aggregated_ids['IdBandeira'])}, "
                       f"Redes: {len(aggregated_ids['IdRede'])}")
            
        except Exception as e:
            logger.error(f"Erro ao atualizar arquivo de filtros: {e}")
            print(f"AVISO - Erro ao atualizar arquivo de filtros: {e}")
    
    def validate_step1_customer_segments(self):
        """Validação e relatório de qualidade - ETAPA 1"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 1: CUSTOMER_SEGMENTS")
        print("-"*80)
        
        try:
            # Contar origem (aplicando limite se especificado)
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            if self.limit_rows > 0:
                cursor_sql.execute(f"SELECT COUNT(*) FROM (SELECT TOP {self.limit_rows} Id FROM SegmentoProduto ORDER BY Id) AS limited")
            else:
                cursor_sql.execute("SELECT COUNT(*) FROM SegmentoProduto")
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.customer_segments")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - SegmentoProduto):")
            print(f"  Total de registros: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.customer_segments):")
            print(f"  Total de registros: {destino_count}")
            
            diferenca = origem_count - destino_count
            
            if diferenca == 0:
                print(f"\nOK - Todos os registros foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 1: OK - Origem: {origem_count}, Destino: {destino_count}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} registros")
                print(f"  {abs(diferenca)} registros {'faltando' if diferenca > 0 else 'extras'} no destino")
                logger.warning(f"VALIDACAO ETAPA 1: Diferenca - Origem: {origem_count}, Destino: {destino_count}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 1: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step1_migrate_customer_segments(self):
        """ETAPA 1: Migrar customer_segments"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 1: MIGRANDO CUSTOMER_SEGMENTS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 1: Migrando customer_segments")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        # Truncate
        print("\n[ETAPA 1] Limpando tabela customer_segments...")
        self.truncate_table('customer_segments')
        
        # Verificar se precisa criar coluna legacy_id em HML
        include_legacy = False
        if self.should_include_legacy_id():
            try:
                # Verificar se a coluna legacy_id existe na tabela
                conn_check = DatabaseConnection.get_postgresql_destino_connection()
                cursor_check = conn_check.cursor()
                cursor_check.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema}' 
                    AND table_name = 'customer_segments' 
                    AND column_name = 'legacy_id'
                """)
                if cursor_check.fetchone():
                    include_legacy = True
                    print("[ETAPA 1] Coluna legacy_id já existe na tabela")
                else:
                    # Criar coluna legacy_id se não existir
                    print("[ETAPA 1] Criando coluna legacy_id...")
                    cursor_check.execute(f"ALTER TABLE {schema}.customer_segments ADD COLUMN legacy_id INTEGER")
                    conn_check.commit()
                    include_legacy = True
                    print("OK - Coluna legacy_id criada")
                cursor_check.close()
                conn_check.close()
            except Exception as e:
                logger.warning(f"Nao foi possivel criar/verificar coluna legacy_id: {e}")
                include_legacy = False
        
        # Buscar dados do SQL Server
        print("[ETAPA 1] Buscando dados do SQL Server...")
        sql_query = """
        SELECT 
            Id,
            Nome,
            Ativo,
            DataInclusao,
            DataAlteracao
        FROM SegmentoProduto
        ORDER BY Id
        """
        
        # Adicionar LIMIT se especificado
        if self.limit_rows > 0:
            sql_query = sql_query.replace("ORDER BY Id", f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute(sql_query)
        
        # Carregar TODOS os dados na memória de uma vez (otimizado)
        print("[ETAPA 1] Carregando dados na memória...")
        all_rows = cursor_sql.fetchall()
        cursor_sql.close()
        conn_sql.close()
        
        print(f"[ETAPA 1] {len(all_rows)} registros carregados. Processando conversões em massa (vetorizado)...")
        
        # Processar conversões em massa usando DataFrame (vetorizado)
        schema = get_schema_atual()
        df = pd.DataFrame.from_records(all_rows, columns=['Id', 'Nome', 'Ativo', 'DataInclusao', 'DataAlteracao'])
        
        # Aplicar transformações vetorizadas
        df['legacy_id'] = df['Id']
        df['nome'] = df['Nome'].astype(str).str.strip()
        df['nome'] = df['nome'].replace(['', 'null', 'none', 'None', 'NULL', 'NONE'], None)
        df['nome'] = df['nome'].str[:255] if len(df) > 0 else df['nome']
        df['ativo'] = df['Ativo'].fillna(False).astype(bool)
        df['data_inclusao'] = df['DataInclusao']
        df['data_alteracao'] = df['DataAlteracao'].fillna(df['DataInclusao'])
        
        # Remover linhas com erros (nome None após limpeza)
        df = df[df['nome'].notna()]
        
        print(f"[ETAPA 1] {len(df)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Preparar query e dados baseado em include_legacy
        if include_legacy:
            # Query com legacy_id
            insert_query = f"""
            INSERT INTO {schema}.customer_segments (
                id, name, is_active, created_at, updated_at, legacy_id
            ) VALUES %s
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s)"
            # Converter DataFrame para lista de tuplas com legacy_id
            processed_tuples = list(zip(
                df['nome'].tolist(),
                df['ativo'].tolist(),
                df['data_inclusao'].tolist(),
                df['data_alteracao'].tolist(),
                df['legacy_id'].tolist()
            ))
        else:
            # Query sem legacy_id
            insert_query = f"""
            INSERT INTO {schema}.customer_segments (
                id, name, is_active, created_at, updated_at
            ) VALUES %s
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s)"
            # Converter DataFrame para lista de tuplas sem legacy_id
            processed_tuples = list(zip(
                df['nome'].tolist(),
                df['ativo'].tolist(),
                df['data_inclusao'].tolist(),
                df['data_alteracao'].tolist()
            ))
        
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
                        
                        # Coletar legacy_ids para lookup depois (se include_legacy)
                        if include_legacy:
                            chunk_legacy_ids = [row[-1] for row in chunk]  # último elemento é legacy_id
                            all_legacy_ids_inserted.extend(chunk_legacy_ids)
                        
                        total_processed += len(chunk)
                        self.stats['customer_segments'] += len(chunk)
                        
                    except Exception as batch_error:
                        error_msg = f"Erro ao inserir batch de customer_segments: {batch_error}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
                
                # Commit do chunk
                try:
                    conn_pg.commit()
                    print(f"[ETAPA 1] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                    logger.info(f"[ETAPA 1] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            # Buscar UUIDs gerados para mapeamento (uma única query após todas as inserções)
            if include_legacy and all_legacy_ids_inserted:
                print(f"[ETAPA 1] Buscando UUIDs gerados para {len(all_legacy_ids_inserted)} registros...")
                cursor_pg.execute(f"""
                    SELECT id, legacy_id 
                    FROM {schema}.customer_segments 
                    WHERE legacy_id = ANY(%s)
                """, (all_legacy_ids_inserted,))
                for uuid_row, leg_id in cursor_pg.fetchall():
                    self.segment_id_map[leg_id] = uuid_row
                print(f"[ETAPA 1] {len(self.segment_id_map)} UUIDs mapeados")
            elif not include_legacy:
                # Se não há legacy_id, mapear por ordem de inserção (menos confiável, mas necessário)
                print(f"[ETAPA 1] Buscando UUIDs gerados para mapeamento...")
                cursor_pg.execute(f"""
                    SELECT id, created_at 
                    FROM {schema}.customer_segments 
                    ORDER BY created_at
                    LIMIT %s
                """, (len(processed_tuples),))
                legacy_ids_list = df['legacy_id'].tolist()
                for idx, (uuid_row, _) in enumerate(cursor_pg.fetchall()):
                    if idx < len(legacy_ids_list):
                        self.segment_id_map[legacy_ids_list[idx]] = uuid_row
                print(f"[ETAPA 1] {len(self.segment_id_map)} UUIDs mapeados")
            
            print(f"\n[ETAPA 1] CONCLUIDA! Total de customer_segments migrados: {self.stats['customer_segments']}")
            logger.info(f"ETAPA 1 concluida: {self.stats['customer_segments']} registros")
            
            # Validação e relatório de qualidade
            self.validate_step1_customer_segments()
            
            # Fechar conexões
            cursor_pg.close()
            conn_pg.close()
            
        except Exception as e:
            logger.error(f"Erro na ETAPA 1: {e}")
            if 'conn_pg' in locals():
                try:
                    conn_pg.rollback()
                    if 'cursor_pg' in locals():
                        cursor_pg.close()
                    conn_pg.close()
                except:
                    pass
            raise
    
    def validate_step2_customers(self):
        """Validação e relatório de qualidade - ETAPA 2"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 2: CUSTOMERS")
        print("-"*80)
        
        try:
            # Carregar filtros do contracts ou buscar da ViewOrcamentosLojas
            filter_data = self.load_filter_json()
            id_cliente_list = []
            
            if filter_data and 'aggregated_ids' in filter_data:
                id_cliente_list = filter_data['aggregated_ids'].get('IdCliente', [])
            
            # Contar origem aplicando os mesmos filtros
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            if id_cliente_list:
                # Aplicar filtro de IdCliente do JSON
                placeholders = ','.join(['?' for _ in id_cliente_list])
                cursor_sql.execute(f"SELECT COUNT(*) FROM Cliente WHERE Id IN ({placeholders})", id_cliente_list)
            else:
                # Buscar diretamente da ViewOrcamentosLojas (mesmo em full load)
                cursor_sql.execute("SELECT COUNT(DISTINCT IdCliente) FROM ViewOrcamentosLojas WHERE IdCliente IS NOT NULL")
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino aplicando os mesmos filtros
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            if id_cliente_list:
                # Contar apenas os customers migrados nesta execução
                cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.customers WHERE legacy_id = ANY(%s)", (id_cliente_list,))
            else:
                # Contar todos (fallback)
                cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.customers")
            destino_count = cursor_pg.fetchone()[0]
            
            # Verificar legacy_id
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.customers WHERE legacy_id IS NOT NULL")
            com_legacy_id = cursor_pg.fetchone()[0]
            
            # Verificar cnpj preenchido
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.customers WHERE cnpj IS NOT NULL")
            com_cnpj = cursor_pg.fetchone()[0]
            
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - Cliente):")
            print(f"  Total de registros: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.customers):")
            print(f"  Total de registros: {destino_count}")
            print(f"  Com legacy_id: {com_legacy_id}")
            print(f"  Com cnpj: {com_cnpj}")
            
            diferenca = origem_count - destino_count
            
            if diferenca == 0:
                print(f"\nOK - Todos os registros foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 2: OK - Origem: {origem_count}, Destino: {destino_count}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} registros")
                print(f"  {abs(diferenca)} registros {'faltando' if diferenca > 0 else 'extras'} no destino")
                logger.warning(f"VALIDACAO ETAPA 2: Diferenca - Origem: {origem_count}, Destino: {destino_count}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 2: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step2_migrate_customers(self):
        """ETAPA 2: Migrar customers"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 2: MIGRANDO CUSTOMERS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 2: Migrando customers")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        # Carregar filtros do contracts step1 (se existir)
        # ⚠️ IMPORTANTE: Verificar se os filtros de data correspondem aos filtros passados na linha de comando
        filter_data = None
        json_filters_match = False
        
        if self.limit_rows == 0:
            filter_data = self.load_filter_json()
            
            # Verificar se os filtros de data no JSON correspondem aos filtros passados na linha de comando
            if filter_data and 'filters_applied' in filter_data:
                json_filters = filter_data['filters_applied']
                json_data_aviso = json_filters.get('data_aviso_previo_min')
                json_data_inicio = json_filters.get('data_inicio_operacao_max')
                
                # Converter filtros para string para comparação
                cmd_data_aviso = self.data_aviso_previo_min
                if cmd_data_aviso and isinstance(cmd_data_aviso, str):
                    cmd_data_aviso = cmd_data_aviso
                elif cmd_data_aviso:
                    cmd_data_aviso = cmd_data_aviso.strftime('%Y-%m-%d')
                
                cmd_data_inicio = self.data_inicio_operacao_max
                if cmd_data_inicio and isinstance(cmd_data_inicio, str):
                    cmd_data_inicio = cmd_data_inicio
                elif cmd_data_inicio:
                    cmd_data_inicio = cmd_data_inicio.strftime('%Y-%m-%d')
                
                # Comparar filtros
                json_filters_match = (
                    (json_data_aviso == cmd_data_aviso or (json_data_aviso is None and cmd_data_aviso is None)) and
                    (json_data_inicio == cmd_data_inicio or (json_data_inicio is None and cmd_data_inicio is None))
                )
                
                if not json_filters_match:
                    logger.info(f"[ETAPA 2] Filtros de data no JSON não correspondem aos filtros passados. JSON: data_aviso={json_data_aviso}, data_inicio={json_data_inicio}. CMD: data_aviso={cmd_data_aviso}, data_inicio={cmd_data_inicio}")
                    print(f"[ETAPA 2] Filtros de data no JSON não correspondem. Buscando da ViewOrcamentosLojas...")
                    # ⚠️ IMPORTANTE: Manter filter_data para usar os filtros do JSON quando CMD não especificou filtros
                    # Não definir filter_data = None aqui, pois precisamos dos filtros do JSON
        
        id_cliente_filter_list = []
        
        # ⚠️ IMPORTANTE: Se os filtros não correspondem mas CMD não especificou filtros, usar os IdCliente do JSON
        # Isso garante consistência quando o JSON tem filtros mas o CMD não especificou nenhum
        if filter_data and 'aggregated_ids' in filter_data:
            if json_filters_match:
                # Filtros correspondem: usar IdCliente do JSON diretamente
                id_cliente_filter_list = filter_data['aggregated_ids'].get('IdCliente', [])
                logger.info(f"[ETAPA 2] Carregados {len(id_cliente_filter_list)} IdCliente do arquivo de filtros do contracts")
                print(f"[ETAPA 2] Carregados {len(id_cliente_filter_list)} IdCliente do arquivo de filtros do contracts")
            elif self.data_aviso_previo_min is None and self.data_inicio_operacao_max is None:
                # Filtros não correspondem mas CMD não especificou filtros: usar IdCliente do JSON mesmo assim
                id_cliente_filter_list = filter_data['aggregated_ids'].get('IdCliente', [])
                logger.info(f"[ETAPA 2] Filtros não correspondem mas CMD não especificou filtros. Usando {len(id_cliente_filter_list)} IdCliente do JSON.")
                print(f"[ETAPA 2] Filtros não correspondem mas CMD não especificou filtros. Usando {len(id_cliente_filter_list)} IdCliente do JSON.")
            else:
                # Filtros não correspondem e CMD especificou filtros diferentes: buscar da ViewOrcamentosLojas
                logger.info(f"[ETAPA 2] Filtros não correspondem e CMD especificou filtros diferentes. Buscando da ViewOrcamentosLojas...")
                print(f"[ETAPA 2] Filtros não correspondem e CMD especificou filtros diferentes. Buscando da ViewOrcamentosLojas...")
        
        if not id_cliente_filter_list:
            # Se não há JSON, filtros não correspondem, ou há LIMIT, buscar diretamente da ViewOrcamentosLojas
            if self.limit_rows > 0:
                print(f"[ETAPA 2] LIMIT {self.limit_rows} especificado. Buscando IdCliente da ViewOrcamentosLojas com LIMIT...")
                logger.info(f"[ETAPA 2] LIMIT {self.limit_rows} especificado. Ignorando JSON e buscando da ViewOrcamentosLojas")
            else:
                print("[ETAPA 2] Buscando IdCliente da ViewOrcamentosLojas com filtros aplicados...")
                logger.info("[ETAPA 2] Buscando IdCliente da ViewOrcamentosLojas com filtros aplicados...")
            
            conn_sql_view = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql_view = conn_sql_view.cursor()
            
            # Construir query com filtros de data aplicados
            # ⚠️ IMPORTANTE: Se os filtros do CMD são None mas o JSON tem filtros, usar os filtros do JSON
            where_conditions = []
            query_params = []
            
            # Determinar quais filtros de data usar
            # Prioridade: CMD > JSON > None
            data_aviso_previo_to_use = self.data_aviso_previo_min
            data_inicio_operacao_to_use = self.data_inicio_operacao_max
            
            # Debug: verificar se filter_data existe
            logger.info(f"[ETAPA 2] DEBUG - filter_data existe: {filter_data is not None}")
            if filter_data:
                logger.info(f"[ETAPA 2] DEBUG - filter_data tem 'filters_applied': {'filters_applied' in filter_data}")
                if 'filters_applied' in filter_data:
                    logger.info(f"[ETAPA 2] DEBUG - filters_applied: {filter_data['filters_applied']}")
            
            # Se CMD não especificou filtros mas JSON tem, usar os do JSON
            if data_aviso_previo_to_use is None:
                if filter_data and 'filters_applied' in filter_data:
                    json_filters = filter_data['filters_applied']
                    json_data_aviso = json_filters.get('data_aviso_previo_min')
                    if json_data_aviso:
                        data_aviso_previo_to_use = json_data_aviso
                        logger.info(f"[ETAPA 2] Usando filtro de data do JSON (data_aviso_previo_min={json_data_aviso}) já que CMD não especificou")
                else:
                    logger.info(f"[ETAPA 2] DEBUG - Não foi possível usar filtro do JSON para data_aviso_previo (filter_data={filter_data is not None}, tem filters_applied={filter_data and 'filters_applied' in filter_data if filter_data else False})")
            
            if data_inicio_operacao_to_use is None:
                if filter_data and 'filters_applied' in filter_data:
                    json_filters = filter_data['filters_applied']
                    json_data_inicio = json_filters.get('data_inicio_operacao_max')
                    if json_data_inicio:
                        data_inicio_operacao_to_use = json_data_inicio
                        logger.info(f"[ETAPA 2] Usando filtro de data do JSON (data_inicio_operacao_max={json_data_inicio}) já que CMD não especificou")
                else:
                    logger.info(f"[ETAPA 2] DEBUG - Não foi possível usar filtro do JSON para data_inicio_operacao (filter_data={filter_data is not None}, tem filters_applied={filter_data and 'filters_applied' in filter_data if filter_data else False})")
            
            logger.info(f"[ETAPA 2] DEBUG - Filtros finais a usar: data_aviso_previo={data_aviso_previo_to_use}, data_inicio_operacao={data_inicio_operacao_to_use}")
            
            # Filtro IdOrcamento
            if self.id_orcamento_filter:
                placeholders = ','.join(['?' for _ in self.id_orcamento_filter])
                where_conditions.append(f"v.IdOrcamento IN ({placeholders})")
                query_params.extend(self.id_orcamento_filter)
            
            # Filtro DataAvisoPrevio (data mínima)
            if data_aviso_previo_to_use is not None:
                if isinstance(data_aviso_previo_to_use, str):
                    data_aviso_previo_str = data_aviso_previo_to_use
                else:
                    data_aviso_previo_str = data_aviso_previo_to_use.strftime('%Y-%m-%d')
                where_conditions.append("(CONVERT(DATE, v.DataAvisoPrevio) >= ? OR v.DataAvisoPrevio IS NULL)")
                query_params.append(data_aviso_previo_str)
            
            # Filtro DataInicioOperacao (data máxima)
            if data_inicio_operacao_to_use is not None:
                if isinstance(data_inicio_operacao_to_use, str):
                    data_inicio_str = data_inicio_operacao_to_use
                else:
                    data_inicio_str = data_inicio_operacao_to_use.strftime('%Y-%m-%d')
                where_conditions.append("CONVERT(DATE, v.DataInicioOperacao) <= ?")
                query_params.append(data_inicio_str)
            
            # Construir query completa com INNER JOIN Orcamento e filtros
            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)
            # Não adicionar filtro de IdCliente IS NOT NULL aqui, pois vamos filtrar NULLs depois no DataFrame
            
            # ⚠️ LÓGICA SIMPLIFICADA: Query direta sem subquery (conforme validação)
            if self.limit_rows > 0:
                query_id_cliente = f"""
                SELECT DISTINCT TOP {self.limit_rows}
                    v.IdCliente
                FROM ViewOrcamentosLojas v
                INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                {where_clause}
                """
            else:
                query_id_cliente = f"""
                SELECT DISTINCT
                    v.IdCliente
                FROM ViewOrcamentosLojas v
                INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                {where_clause}
                """
            
            # Log da query completa para debug
            logger.info(f"[ETAPA 2] Query completa para buscar IdCliente da ViewOrcamentosLojas: {query_id_cliente}")
            logger.info(f"[ETAPA 2] Parâmetros da query: {query_params}")
            
            # Executar query
            if query_params:
                cursor_sql_view.execute(query_id_cliente, query_params)
            else:
                cursor_sql_view.execute(query_id_cliente)
            
            # Carregar resultados (já são únicos devido ao DISTINCT na query externa)
            id_cliente_filter_list = [row[0] for row in cursor_sql_view.fetchall()]
            cursor_sql_view.close()
            conn_sql_view.close()
            
            print(f"[ETAPA 2] Carregados {len(id_cliente_filter_list)} IdCliente únicos da ViewOrcamentosLojas")
            logger.info(f"[ETAPA 2] Carregados {len(id_cliente_filter_list)} IdCliente únicos da ViewOrcamentosLojas")
            
            # Atualizar JSON com IDs coletados da ViewOrcamentosLojas (apenas uma vez por execução)
            if not self.json_updated_this_run:
                self.save_filter_json_from_view(id_orcamento_list=self.id_orcamento_filter)
                self.json_updated_this_run = True
        
        # Limpar tabela
        # ⚠️ IMPORTANTE: Se clear_data=True, sempre usar TRUNCATE (mesmo com filtros)
        if self.clear_data:
            print("\n[ETAPA 2] Limpando tabela customers (TRUNCATE - flag --clear-data ativo)...")
            logger.info("[ETAPA 2] Flag --clear-data ativo: usando TRUNCATE mesmo com filtros")
            self.truncate_table('customers')
        elif id_cliente_filter_list:
            # Com filtros e SEM clear_data: usar DELETE
            print("\n[ETAPA 2] Limpando registros filtrados da tabela customers...")
            self.delete_table_with_filter('customers', id_cliente_filter_list)
        else:
            # Sem filtros e SEM clear_data: usar TRUNCATE (caso raro)
            print("\n[ETAPA 2] Limpando tabela customers...")
            self.truncate_table('customers')
        
        # Buscar dados do SQL Server com filtro de IdCliente
        print("[ETAPA 2] Buscando dados do SQL Server...")
        sql_query = """
        SELECT 
            Id,
            Codigo,
            TipoPessoa,
            CpfCnpj,
            InscricaoEstadual,
            InscricaoMunicipal,
            RazaoSocial,
            NomeFantasia,
            Ativo,
            DataInclusao,
            DataAlteracao,
            DataAtivacao
        FROM Cliente
        """
        
        # Aplicar filtro de IdCliente se houver
        query_params = []
        if id_cliente_filter_list:
            # ⚠️ IMPORTANTE: SQL Server tem limite de ~2100 parâmetros
            # Dividir em chunks de 2000 para evitar erro "COUNT field incorrect"
            MAX_PARAMS_PER_QUERY = 2000
            all_rows = []
            
            # Dividir lista em chunks se necessário
            if len(id_cliente_filter_list) > MAX_PARAMS_PER_QUERY:
                chunks = [id_cliente_filter_list[i:i + MAX_PARAMS_PER_QUERY] 
                         for i in range(0, len(id_cliente_filter_list), MAX_PARAMS_PER_QUERY)]
                
                print(f"[ETAPA 2] Processando {len(id_cliente_filter_list)} IdCliente em {len(chunks)} chunks de até {MAX_PARAMS_PER_QUERY} registros cada...")
                logger.info(f"[ETAPA 2] Processando {len(id_cliente_filter_list)} IdCliente em {len(chunks)} chunks")
                
                conn_sql = DatabaseConnection.get_sql_server_prd_connection()
                cursor_sql = conn_sql.cursor()
                
                for chunk_idx, chunk in enumerate(chunks, 1):
                    placeholders = ','.join(['?' for _ in chunk])
                    chunk_query = f"""
                    SELECT 
                        Id,
                        Codigo,
                        TipoPessoa,
                        CpfCnpj,
                        InscricaoEstadual,
                        InscricaoMunicipal,
                        RazaoSocial,
                        NomeFantasia,
                        Ativo,
                        DataInclusao,
                        DataAlteracao,
                        DataAtivacao
                    FROM Cliente
                    WHERE Id IN ({placeholders})
                    ORDER BY Id
                    """
                    cursor_sql.execute(chunk_query, chunk)
                    chunk_rows = cursor_sql.fetchall()
                    all_rows.extend(chunk_rows)
                    print(f"[ETAPA 2] Chunk {chunk_idx}/{len(chunks)}: {len(chunk_rows)} registros carregados")
                    logger.info(f"[ETAPA 2] Chunk {chunk_idx}/{len(chunks)}: {len(chunk_rows)} registros carregados (query: {chunk_query[:100]}...)")
                
                cursor_sql.close()
                conn_sql.close()
                
                print(f"[ETAPA 2] Total: {len(all_rows)} registros carregados de {len(id_cliente_filter_list)} IdCliente")
                logger.info(f"[ETAPA 2] Total: {len(all_rows)} registros carregados de {len(id_cliente_filter_list)} IdCliente únicos")
                
                # Verificar se há duplicatas (mesmo Id aparecendo múltiplas vezes)
                if len(all_rows) > len(id_cliente_filter_list):
                    unique_ids = set(row[0] for row in all_rows)
                    print(f"[ETAPA 2] AVISO: {len(all_rows)} registros carregados, mas apenas {len(unique_ids)} IdCliente únicos. Possíveis duplicatas na tabela Cliente!")
                    logger.warning(f"[ETAPA 2] AVISO: {len(all_rows)} registros carregados, mas apenas {len(unique_ids)} IdCliente únicos. Possíveis duplicatas na tabela Cliente!")
            else:
                # Menos de 2000 parâmetros, executar query normal
                placeholders = ','.join(['?' for _ in id_cliente_filter_list])
                sql_query += f" WHERE Id IN ({placeholders})"
                query_params.extend(id_cliente_filter_list)
                sql_query += " ORDER BY Id"
                
                # Adicionar LIMIT se especificado
                if self.limit_rows > 0:
                    sql_query = sql_query.replace("ORDER BY Id", f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
                
                logger.info(f"[ETAPA 2] Query completa: {sql_query[:200]}...")
                logger.info(f"[ETAPA 2] Parâmetros: {len(query_params)} IdCliente (primeiros 10: {query_params[:10]})")
                
                conn_sql = DatabaseConnection.get_sql_server_prd_connection()
                cursor_sql = conn_sql.cursor()
                cursor_sql.execute(sql_query, query_params)
                all_rows = cursor_sql.fetchall()
                cursor_sql.close()
                conn_sql.close()
                
                print(f"[ETAPA 2] {len(all_rows)} registros carregados. Processando conversões em massa (vetorizado)...")
                logger.info(f"[ETAPA 2] {len(all_rows)} registros carregados de {len(id_cliente_filter_list)} IdCliente únicos")
                
                # Verificar se há duplicatas
                if len(all_rows) > len(id_cliente_filter_list):
                    unique_ids = set(row[0] for row in all_rows)
                    print(f"[ETAPA 2] AVISO: {len(all_rows)} registros carregados, mas apenas {len(unique_ids)} IdCliente únicos. Possíveis duplicatas na tabela Cliente!")
                    logger.warning(f"[ETAPA 2] AVISO: {len(all_rows)} registros carregados, mas apenas {len(unique_ids)} IdCliente únicos. Possíveis duplicatas na tabela Cliente!")
        else:
            sql_query += " ORDER BY Id"
            
            # Adicionar LIMIT se especificado
            if self.limit_rows > 0:
                sql_query = sql_query.replace("ORDER BY Id", f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
            
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            cursor_sql.execute(sql_query)
            all_rows = cursor_sql.fetchall()
            cursor_sql.close()
            conn_sql.close()
            
            print(f"[ETAPA 2] {len(all_rows)} registros carregados. Processando conversões em massa (vetorizado)...")
        
        # Processar conversões em massa usando DataFrame (vetorizado)
        schema = get_schema_atual()
        df = pd.DataFrame.from_records(all_rows, columns=['Id', 'Codigo', 'TipoPessoa', 'CpfCnpj', 'InscricaoEstadual', 'InscricaoMunicipal', 'RazaoSocial', 'NomeFantasia', 'Ativo', 'DataInclusao', 'DataAlteracao', 'DataAtivacao'])
        
        # Aplicar transformações vetorizadas
        df['legacy_id'] = df['Id']
        
        # Limpar CPF/CNPJ (vetorizado)
        df['cpf_cnpj'] = self.clean_cpf_cnpj_vectorized(df['CpfCnpj'])
        df['cpf_cnpj'] = df['cpf_cnpj'].fillna('00000000000000')  # Valor padrão
        
        # CNPJ status
        df['cnpj_status'] = df['cpf_cnpj'].apply(lambda x: 'valid' if x and x != '00000000000000' else 'invalid')
        
        # Limpar strings (vetorizado)
        df['state_registration'] = self.clean_string_vectorized(df['InscricaoEstadual'], max_length=20)
        df['municipal_registration'] = self.clean_string_vectorized(df['InscricaoMunicipal'], max_length=20)
        df['legal_name'] = self.clean_string_vectorized(df['RazaoSocial'], max_length=255)
        df['trade_name'] = self.clean_string_vectorized(df['NomeFantasia'], max_length=255)
        
        # Status (vetorizado)
        df['status'] = df['Ativo'].apply(lambda x: 'active' if x is True else 'inactive')
        
        # Datas
        df['data_inclusao'] = df['DataInclusao']
        df['data_alteracao'] = df['DataAlteracao'].fillna(df['DataInclusao'])
        
        # Remover linhas com erros (legal_name None após limpeza)
        df = df[df['legal_name'].notna()]
        
        # Converter DataFrame diretamente para lista de tuplas (otimizado)
        processed_tuples = list(zip(
            df['legacy_id'].tolist(),
            df['cpf_cnpj'].tolist(),
            df['cnpj_status'].tolist(),
            df['state_registration'].tolist(),
            df['municipal_registration'].tolist(),
            df['legal_name'].tolist(),
            df['trade_name'].tolist(),
            df['status'].tolist(),
            df['data_inclusao'].tolist(),
            df['data_alteracao'].tolist()
        ))
        legacy_ids_list = df['legacy_id'].tolist()
        
        print(f"[ETAPA 2] {len(processed_tuples)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Query de insert usando gen_random_uuid() - formato para execute_values
        insert_query = f"""
        INSERT INTO {schema}.customers (
            id, legacy_id, cnpj, cnpj_status, state_registration,
            municipal_registration, legal_name, trade_name, status,
            created_at, updated_at
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
                print(f"[ETAPA 2] Processando chunk {chunk_num} ({len(chunk)} registros)...")
                
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
                        
                        # Coletar legacy_ids para lookup depois
                        chunk_legacy_ids = [row[0] for row in chunk]  # primeiro elemento é legacy_id
                        all_legacy_ids_inserted.extend(chunk_legacy_ids)
                        
                        total_processed += len(chunk)
                        self.stats['customers'] += len(chunk)
                        
                    except Exception as batch_error:
                        error_msg = f"Erro ao inserir batch de customers: {batch_error}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
                
                # Commit do chunk
                try:
                    conn_pg.commit()
                    print(f"[ETAPA 2] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                    logger.info(f"[ETAPA 2] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            # Buscar UUIDs gerados para mapeamento (uma única query após todas as inserções)
            if all_legacy_ids_inserted:
                print(f"[ETAPA 2] Buscando UUIDs gerados para {len(all_legacy_ids_inserted)} registros...")
                cursor_pg.execute(f"""
                    SELECT id, legacy_id 
                    FROM {schema}.customers 
                    WHERE legacy_id = ANY(%s)
                """, (all_legacy_ids_inserted,))
                for uuid_row, leg_id in cursor_pg.fetchall():
                    self.customer_id_map[leg_id] = uuid_row
                print(f"[ETAPA 2] {len(self.customer_id_map)} UUIDs mapeados")
            
            print(f"\n[ETAPA 2] CONCLUIDA! Total de customers migrados: {self.stats['customers']}")
            logger.info(f"ETAPA 2 concluida: {self.stats['customers']} registros")
            
            # Validação e relatório de qualidade
            self.validate_step2_customers()
            
            # Fechar conexões
            cursor_pg.close()
            conn_pg.close()
            
        except Exception as e:
            logger.error(f"Erro na ETAPA 2: {e}")
            if 'conn_pg' in locals():
                try:
                    conn_pg.rollback()
                    if 'cursor_pg' in locals():
                        cursor_pg.close()
                    conn_pg.close()
                except:
                    pass
            raise
    
    def validate_step3_addresses(self):
        """Validação e relatório de qualidade - ETAPA 3"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 3: ADDRESSES")
        print("-"*80)
        
        try:
            # Contar origem - endereços principais (aplicando mesmo filtro e limite da migração)
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            # Se houver limite, usar subquery com TOP para aplicar o limite
            if self.limit_rows > 0:
                query_main = f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT TOP {self.limit_rows} Id, Endereco, EnderecoCobranca
                        FROM Cliente
                        WHERE (Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != '')
                           OR (EnderecoCobranca IS NOT NULL AND LTRIM(RTRIM(EnderecoCobranca)) != '')
                        ORDER BY Id
                    ) AS limited
                    WHERE Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != ''
                """
                query_billing = f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT TOP {self.limit_rows} Id, Endereco, EnderecoCobranca
                        FROM Cliente
                        WHERE (Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != '')
                           OR (EnderecoCobranca IS NOT NULL AND LTRIM(RTRIM(EnderecoCobranca)) != '')
                        ORDER BY Id
                    ) AS limited
                    WHERE EnderecoCobranca IS NOT NULL AND LTRIM(RTRIM(EnderecoCobranca)) != ''
                """
            else:
                # Sem limite, query direta sem subquery
                query_main = """
                    SELECT COUNT(*) 
                    FROM Cliente
                    WHERE Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != ''
                """
                query_billing = """
                    SELECT COUNT(*) 
                    FROM Cliente
                    WHERE EnderecoCobranca IS NOT NULL AND LTRIM(RTRIM(EnderecoCobranca)) != ''
                """
            
            cursor_sql.execute(query_main)
            origem_main = cursor_sql.fetchone()[0]
            
            cursor_sql.execute(query_billing)
            origem_billing = cursor_sql.fetchone()[0]
            
            origem_total = origem_main + origem_billing
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.addresses")
            destino_total = cursor_pg.fetchone()[0]
            
            # Contar por tipo
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.addresses WHERE type = 'main'")
            destino_main = cursor_pg.fetchone()[0]
            
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.addresses WHERE type = 'billing'")
            destino_billing = cursor_pg.fetchone()[0]
            
            # Verificar addressable_id preenchido
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.addresses WHERE addressable_id IS NOT NULL")
            com_addressable_id = cursor_pg.fetchone()[0]
            
            # Verificar addressable_type
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.addresses WHERE addressable_type = 'customers'")
            com_addressable_type = cursor_pg.fetchone()[0]
            
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - Cliente):")
            print(f"  Enderecos principais: {origem_main}")
            print(f"  Enderecos de cobranca: {origem_billing}")
            print(f"  Total esperado: {origem_total}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.addresses):")
            print(f"  Enderecos principais (type='main'): {destino_main}")
            print(f"  Enderecos de cobranca (type='billing'): {destino_billing}")
            print(f"  Total inserido: {destino_total}")
            print(f"  Com addressable_id: {com_addressable_id}")
            print(f"  Com addressable_type='customers': {com_addressable_type}")
            
            diferenca = origem_total - destino_total
            
            if diferenca == 0:
                print(f"\nOK - Todos os enderecos foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 3: OK - Origem: {origem_total}, Destino: {destino_total}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} enderecos")
                print(f"  {abs(diferenca)} enderecos {'faltando' if diferenca > 0 else 'extras'} no destino")
                logger.warning(f"VALIDACAO ETAPA 3: Diferenca - Origem: {origem_total}, Destino: {destino_total}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 3: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step3_migrate_addresses(self):
        """ETAPA 3: Migrar addresses"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 3: MIGRANDO ADDRESSES")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 3: Migrando addresses")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        try:
            # Delete apenas addresses de customers (tabela polimórfica)
            # ⚠️ IMPORTANTE: delete_polymorphic_table sempre deleta TODOS os registros do tipo especificado
            # Quando clear_data=True, o comportamento é o mesmo (deleta todos de 'customers')
            if self.clear_data:
                print("\n[ETAPA 3] Limpando addresses de customers (flag --clear-data ativo - todos os registros)...")
                logger.info("[ETAPA 3] Flag --clear-data ativo: deletando TODOS os addresses de customers")
            else:
                print("\n[ETAPA 3] Limpando addresses de customers...")
            self.delete_polymorphic_table('addresses', 'customers', 'addressable_type')
            
            # Buscar dados do SQL Server
            print("[ETAPA 3] Buscando dados do SQL Server...")
            sql_query = """
        SELECT 
            Id,
            Endereco,
            Numero,
            Complemento,
            Bairro,
            CEP,
            Cidade,
            CodigoMunicipio,
            UF,
            EnderecoCobranca,
            NumeroCobranca,
            ComplementoCobranca,
            BairroCobranca,
            CepCobranca,
            CidadeCobranca,
            CodigoMunicipioCobranca,
            UFCobranca,
            Latitude,
            Longitude,
            DataInclusao,
            DataAlteracao
            FROM Cliente
            WHERE (Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != '')
               OR (EnderecoCobranca IS NOT NULL AND LTRIM(RTRIM(EnderecoCobranca)) != '')
            ORDER BY Id
            """
            
            # Adicionar LIMIT se especificado
            if self.limit_rows > 0:
                sql_query = sql_query.replace("ORDER BY Id", f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
            
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            cursor_sql.execute(sql_query)
            
            # Carregar TODOS os dados na memória de uma vez (otimizado)
            print("[ETAPA 3] Carregando dados na memória...")
            all_rows = cursor_sql.fetchall()
            cursor_sql.close()
            conn_sql.close()
            
            print(f"[ETAPA 3] {len(all_rows)} registros carregados. Processando conversões...")
            
            # Processar conversões em massa
            schema = get_schema_atual()
            import re
            
            # Preparar lista de valores para insert em batch (processar tudo em memória)
            batch_values = []
            chunk_errors = 0
            
            for row in all_rows:
                legado_id = row[0]
                customer_id = self.customer_id_map.get(legado_id)
                
                if not customer_id:
                    continue
                
                # Endereço Principal
                if row[1] and str(row[1]).strip():  # Endereco
                    try:
                        cep = re.sub(r'[^\d]', '', str(row[5])) if row[5] else None
                        if not cep or cep == '0' * len(cep):
                            cep = '00000000'  # Valor padrão (campo é NOT NULL)
                        
                        # Converter CodigoMunicipio para integer se possível
                        municipal_code = 0  # Valor padrão (campo é NOT NULL)
                        if row[7]:
                            try:
                                codigo_str = re.sub(r'[^\d]', '', str(row[7]))
                                if codigo_str:
                                    municipal_code = int(codigo_str[:10])  # Limitar a 10 dígitos
                            except:
                                pass
                        
                        # Converter latitude/longitude se possível
                        lat = None
                        lon = None
                        if row[17]:  # Latitude
                            try:
                                lat = float(str(row[17]).replace(',', '.'))
                            except:
                                pass
                        if row[18]:  # Longitude
                            try:
                                lon = float(str(row[18]).replace(',', '.'))
                            except:
                                pass
                        
                        # Zone e Region são obrigatórios - usar neighborhood como zone e string vazia como region
                        zone_value = self.clean_string(row[4], 100) or ''  # neighborhood como zone (garantir que nunca seja None)
                        region_value = ''  # region vazio (não temos na origem)
                        
                        # Number é obrigatório - usar 'S/N' se não houver número
                        number_value = self.clean_string(row[2], 20) if row[2] and str(row[2]).strip() else 'S/N'
                        
                        # City é obrigatório - usar string vazia se não houver cidade
                        city_value = self.clean_string(row[6], 100) if row[6] and str(row[6]).strip() else ''
                        
                        # State é obrigatório - usar string vazia se não houver UF
                        state_value = self.clean_string(row[8], 2) if row[8] and str(row[8]).strip() else ''
                        
                        # Street é obrigatório - usar string vazia se não houver endereço
                        street_value = self.clean_string(row[1], 500) or ''
                        
                        # Neighborhood é obrigatório - usar string vazia se não houver bairro
                        neighborhood_value = self.clean_string(row[4], 100) or ''
                        
                        batch_values.append((
                            legado_id,  # legacy_id
                            str(customer_id),  # addressable_id (UUID do customer)
                            'customers',  # addressable_type
                            'main',  # type
                            cep,  # postal_code (obrigatório, usar '00000000' se vazio)
                            street_value,  # street (obrigatório, usar '' se vazio)
                            number_value,  # number (obrigatório, usar 'S/N' se vazio)
                            self.clean_string(row[3], 200),  # address_line_2 (Complemento - pode ser NULL)
                            neighborhood_value,  # neighborhood (obrigatório, usar '' se vazio)
                            city_value,  # city (obrigatório, usar '' se vazio)
                            state_value,  # state (obrigatório, usar '' se vazio)
                            municipal_code,  # municipal_code (obrigatório, usar 0 se vazio)
                            lat,  # latitude
                            lon,  # longitude
                            zone_value,  # zone (obrigatório)
                            region_value,  # region (obrigatório)
                            row[19],  # created_at
                            row[20] if row[20] else row[19]  # updated_at
                        ))
                        
                        self.stats['addresses'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao preparar endereco principal cliente Id={legado_id}: {e}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        continue
                
                # Endereço de Cobrança
                if row[9] and str(row[9]).strip():  # EnderecoCobranca
                    try:
                        cep = re.sub(r'[^\d]', '', str(row[13])) if row[13] else None
                        if not cep or cep == '0' * len(cep):
                            cep = '00000000'  # Valor padrão (campo é NOT NULL)
                        
                        # Converter CodigoMunicipioCobranca para integer se possível
                        municipal_code = 0  # Valor padrão (campo é NOT NULL)
                        if row[15]:
                            try:
                                codigo_str = re.sub(r'[^\d]', '', str(row[15]))
                                if codigo_str:
                                    municipal_code = int(codigo_str[:10])
                            except:
                                pass
                        
                        # Zone e Region são obrigatórios - usar neighborhood como zone e string vazia como region
                        zone_value = self.clean_string(row[12], 100) or ''  # BairroCobranca como zone (garantir que nunca seja None)
                        region_value = ''  # region vazio (não temos na origem)
                        
                        # Number é obrigatório - usar 'S/N' se não houver número
                        number_value = self.clean_string(row[10], 20) if row[10] and str(row[10]).strip() else 'S/N'
                        
                        # City é obrigatório - usar string vazia se não houver cidade
                        city_value = self.clean_string(row[14], 100) if row[14] and str(row[14]).strip() else ''
                        
                        # State é obrigatório - usar string vazia se não houver UF
                        state_value = self.clean_string(row[16], 2) if row[16] and str(row[16]).strip() else ''
                        
                        # Street é obrigatório - usar string vazia se não houver endereço
                        street_value = self.clean_string(row[9], 500) or ''
                        
                        # Neighborhood é obrigatório - usar string vazia se não houver bairro
                        neighborhood_value = self.clean_string(row[12], 100) or ''
                        
                        batch_values.append((
                            legado_id,
                            str(customer_id),
                            'customers',
                            'billing',  # type = billing
                            cep,  # postal_code (obrigatório, usar '00000000' se vazio)
                            street_value,  # street (obrigatório, usar '' se vazio)
                            number_value,  # number (obrigatório, usar 'S/N' se vazio)
                            self.clean_string(row[11], 200),  # ComplementoCobranca (pode ser NULL)
                            neighborhood_value,  # neighborhood (obrigatório, usar '' se vazio)
                            city_value,  # city (obrigatório, usar '' se vazio)
                            state_value,  # state (obrigatório, usar '' se vazio)
                            municipal_code,  # municipal_code (obrigatório, usar 0 se vazio)
                            None,  # latitude (não temos para endereço de cobrança)
                            None,  # longitude (não temos para endereço de cobrança)
                            zone_value,  # zone (obrigatório)
                            region_value,  # region (obrigatório)
                            row[19],  # created_at
                            row[20] if row[20] else row[19]  # updated_at
                        ))
                        
                        self.stats['addresses'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao preparar endereco cobranca cliente Id={legado_id}: {e}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        continue
            
            # Executar insert em batch (otimizado com execute_values)
            if batch_values:
                try:
                    insert_query = f"""
                    INSERT INTO {schema}.addresses (
                        id, legacy_id, addressable_id, addressable_type, type,
                        postal_code, street, number, address_line_2, neighborhood,
                        city, state, municipal_code, latitude, longitude, zone, region,
                        created_at, updated_at
                    ) VALUES %s
                    """
                    insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    
                    # Processar em chunks para inserção
                    conn_pg = DatabaseConnection.get_postgresql_destino_connection()
                    cursor_pg = conn_pg.cursor()
                    
                    chunk_num = 0
                    for i in range(0, len(batch_values), CHUNK_SIZE):
                        chunk = batch_values[i:i + CHUNK_SIZE]
                        chunk_num += 1
                        execute_values(
                            cursor_pg,
                            insert_query,
                            chunk,
                            template=insert_template,
                            page_size=CHUNK_SIZE,
                            fetch=False
                        )
                        conn_pg.commit()
                        self.stats['addresses'] += len(chunk)
                        print(f"[ETAPA 3] Chunk {chunk_num} inserido: {len(chunk)} enderecos")
                    
                    cursor_pg.close()
                    conn_pg.close()
                except Exception as batch_error:
                    error_msg = f"Erro ao inserir batch de addresses: {batch_error}"
                    logger.error(error_msg)
                    print(f"ERRO - {error_msg}")
                    self.stats['errors'].append(error_msg)
                    chunk_errors += len(batch_values)
                    try:
                        if 'conn_pg' in locals():
                            conn_pg.rollback()
                            if 'cursor_pg' in locals():
                                cursor_pg.close()
                            conn_pg.close()
                    except Exception as rollback_error:
                        logger.error(f"Erro ao fazer rollback: {rollback_error}")
            
            print(f"\n[ETAPA 3] CONCLUIDA! Total de addresses migrados: {self.stats['addresses']}")
            logger.info(f"ETAPA 3 concluida: {self.stats['addresses']} registros")
            
            # Validação e relatório de qualidade
            self.validate_step3_addresses()
            
        except Exception as e:
            logger.error(f"Erro na ETAPA 3: {e}")
            if 'conn_pg' in locals():
                try:
                    conn_pg.rollback()
                    if 'cursor_pg' in locals():
                        cursor_pg.close()
                    conn_pg.close()
                except:
                    pass
            raise
    
    def validate_step4_contacts(self):
        """Validação e relatório de qualidade - ETAPA 4"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 4: CONTACTS")
        print("-"*80)
        
        try:
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            # Se houver limite, usar subquery com TOP para aplicar o limite
            if self.limit_rows > 0:
                query_email = f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT TOP {self.limit_rows} Id, Email, Telefone, Celular
                        FROM Cliente
                        WHERE (Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
                           OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
                           OR (Celular IS NOT NULL AND LTRIM(RTRIM(Celular)) != '')
                        ORDER BY Id
                    ) AS limited
                    WHERE Email IS NOT NULL AND LTRIM(RTRIM(Email)) != ''
                """
                query_phone = f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT TOP {self.limit_rows} Id, Email, Telefone, Celular
                        FROM Cliente
                        WHERE (Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
                           OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
                           OR (Celular IS NOT NULL AND LTRIM(RTRIM(Celular)) != '')
                        ORDER BY Id
                    ) AS limited
                    WHERE Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != ''
                """
                query_cellphone = f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT TOP {self.limit_rows} Id, Email, Telefone, Celular
                        FROM Cliente
                        WHERE (Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
                           OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
                           OR (Celular IS NOT NULL AND LTRIM(RTRIM(Celular)) != '')
                        ORDER BY Id
                    ) AS limited
                    WHERE Celular IS NOT NULL AND LTRIM(RTRIM(Celular)) != ''
                """
            else:
                # Sem limite, query direta sem subquery
                query_email = """
                    SELECT COUNT(*) 
                    FROM Cliente
                    WHERE Email IS NOT NULL AND LTRIM(RTRIM(Email)) != ''
                """
                query_phone = """
                    SELECT COUNT(*) 
                    FROM Cliente
                    WHERE Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != ''
                """
                query_cellphone = """
                    SELECT COUNT(*) 
                    FROM Cliente
                    WHERE Celular IS NOT NULL AND LTRIM(RTRIM(Celular)) != ''
                """
            
            cursor_sql.execute(query_email)
            origem_email = cursor_sql.fetchone()[0]
            
            cursor_sql.execute(query_phone)
            origem_phone = cursor_sql.fetchone()[0]
            
            cursor_sql.execute(query_cellphone)
            origem_cellphone = cursor_sql.fetchone()[0]
            
            origem_total = origem_email + origem_phone + origem_cellphone
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contacts WHERE contactable_type = 'customers'")
            destino_total = cursor_pg.fetchone()[0]
            
            # Contar por tipo
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contacts WHERE contactable_type = 'customers' AND type = 'email'")
            destino_email = cursor_pg.fetchone()[0]
            
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contacts WHERE contactable_type = 'customers' AND type = 'phone'")
            destino_phone = cursor_pg.fetchone()[0]
            
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contacts WHERE contactable_type = 'customers' AND type = 'cellphone'")
            destino_cellphone = cursor_pg.fetchone()[0]
            
            # Verificar contactable_id preenchido
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contacts WHERE contactable_type = 'customers' AND contactable_id IS NOT NULL")
            com_contactable_id = cursor_pg.fetchone()[0]
            
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - Cliente):")
            print(f"  Emails: {origem_email}")
            print(f"  Telefones: {origem_phone}")
            print(f"  Celulares: {origem_cellphone}")
            print(f"  Total esperado: {origem_total}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.contacts):")
            print(f"  Emails (type='email'): {destino_email}")
            print(f"  Telefones (type='phone'): {destino_phone}")
            print(f"  Celulares (type='cellphone'): {destino_cellphone}")
            print(f"  Total inserido: {destino_total}")
            print(f"  Com contactable_id: {com_contactable_id}")
            
            diferenca = origem_total - destino_total
            
            if diferenca == 0:
                print(f"\nOK - Todos os contacts foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 4: OK - Origem: {origem_total}, Destino: {destino_total}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} contacts")
                print(f"  {abs(diferenca)} contacts {'faltando' if diferenca > 0 else 'extras'} no destino")
                logger.warning(f"VALIDACAO ETAPA 4: Diferenca - Origem: {origem_total}, Destino: {destino_total}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 4: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step4_migrate_contacts(self):
        """ETAPA 4: Migrar contacts"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 4: MIGRANDO CONTACTS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 4: Migrando contacts")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        try:
            # Delete apenas contacts de customers (tabela polimórfica)
            # ⚠️ IMPORTANTE: delete_polymorphic_table sempre deleta TODOS os registros do tipo especificado
            # Quando clear_data=True, o comportamento é o mesmo (deleta todos de 'customers')
            if self.clear_data:
                print("\n[ETAPA 4] Limpando contacts de customers (flag --clear-data ativo - todos os registros)...")
                logger.info("[ETAPA 4] Flag --clear-data ativo: deletando TODOS os contacts de customers")
            else:
                print("\n[ETAPA 4] Limpando contacts de customers...")
            self.delete_polymorphic_table('contacts', 'customers', 'contactable_type')
            
            # Buscar dados do SQL Server
            print("[ETAPA 4] Buscando dados do SQL Server...")
            sql_query = """
        SELECT 
            Id,
            Email,
            Telefone,
            Celular,
            DataInclusao,
            DataAlteracao
        FROM Cliente
        WHERE (Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
           OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
               OR (Celular IS NOT NULL AND LTRIM(RTRIM(Celular)) != '')
            ORDER BY Id
            """
            
            # Adicionar LIMIT se especificado
            if self.limit_rows > 0:
                sql_query = sql_query.replace("ORDER BY Id", f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
            
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            cursor_sql.execute(sql_query)
            
            # Carregar TODOS os dados na memória de uma vez (otimizado)
            print("[ETAPA 4] Carregando dados na memória...")
            all_rows = cursor_sql.fetchall()
            cursor_sql.close()
            conn_sql.close()
            
            print(f"[ETAPA 4] {len(all_rows)} registros carregados. Processando conversões...")
            
            # Preparar lista de valores para insert em batch (processar tudo em memória)
            batch_values = []
            chunk_errors = 0
            import re
            
            for row in all_rows:
                legado_id = row[0]
                customer_id = self.customer_id_map.get(legado_id)
                
                if not customer_id:
                    continue
                
                data_inclusao = row[4]
                data_alteracao = row[5] if row[5] else row[4]
                
                # REGISTRO 1 - Email
                if row[1] and str(row[1]).strip():
                    try:
                        email_value = str(row[1]).strip().lower()
                        batch_values.append((
                            str(customer_id),  # contactable_id
                            'customers',  # contactable_type
                            'email',  # type
                            email_value,  # value (em minusculo)
                            data_inclusao,  # created_at
                            data_alteracao  # updated_at
                        ))
                        self.stats['contacts'] += 1
                    except Exception as e:
                        error_msg = f"Erro ao preparar email para Cliente Id={legado_id}: {e}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                
                # REGISTRO 2 - Telefone
                if row[2] and str(row[2]).strip():
                    try:
                        telefone_value = re.sub(r'[^\d]', '', str(row[2]))
                        if telefone_value:
                            batch_values.append((
                                str(customer_id),  # contactable_id
                                'customers',  # contactable_type
                                'phone',  # type
                                telefone_value,  # value (apenas numeros)
                                data_inclusao,  # created_at
                                data_alteracao  # updated_at
                            ))
                            self.stats['contacts'] += 1
                    except Exception as e:
                        error_msg = f"Erro ao preparar telefone para Cliente Id={legado_id}: {e}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                
                # REGISTRO 3 - Celular
                if row[3] and str(row[3]).strip():
                    try:
                        celular_value = re.sub(r'[^\d]', '', str(row[3]))
                        if celular_value:
                            batch_values.append((
                                str(customer_id),  # contactable_id
                                'customers',  # contactable_type
                                'cellphone',  # type
                                celular_value,  # value (apenas numeros)
                                data_inclusao,  # created_at
                                data_alteracao  # updated_at
                            ))
                            self.stats['contacts'] += 1
                    except Exception as e:
                        error_msg = f"Erro ao preparar celular para Cliente Id={legado_id}: {e}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
            
            # Executar insert em batch (otimizado com execute_values)
            if batch_values:
                try:
                    insert_query = f"""
                    INSERT INTO {schema}.contacts (
                        id, contactable_id, contactable_type, type, value,
                        created_at, updated_at
                    ) VALUES %s
                    """
                    insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s)"
                    
                    # Processar em chunks para inserção
                    conn_pg = DatabaseConnection.get_postgresql_destino_connection()
                    cursor_pg = conn_pg.cursor()
                    
                    chunk_num = 0
                    for i in range(0, len(batch_values), CHUNK_SIZE):
                        chunk = batch_values[i:i + CHUNK_SIZE]
                        chunk_num += 1
                        execute_values(
                            cursor_pg,
                            insert_query,
                            chunk,
                            template=insert_template,
                            page_size=CHUNK_SIZE,
                            fetch=False
                        )
                        conn_pg.commit()
                        self.stats['contacts'] += len(chunk)
                        print(f"[ETAPA 4] Chunk {chunk_num} inserido: {len(chunk)} contacts")
                    
                    cursor_pg.close()
                    conn_pg.close()
                except Exception as batch_error:
                    error_msg = f"Erro ao inserir batch de contacts: {batch_error}"
                    logger.error(error_msg)
                    print(f"ERRO - {error_msg}")
                    self.stats['errors'].append(error_msg)
                    chunk_errors += len(batch_values)
                    try:
                        if 'conn_pg' in locals():
                            conn_pg.rollback()
                            if 'cursor_pg' in locals():
                                cursor_pg.close()
                            conn_pg.close()
                    except Exception as rollback_error:
                        logger.error(f"Erro ao fazer rollback: {rollback_error}")
            
            print(f"\n[ETAPA 4] CONCLUIDA! Total de contacts migrados: {self.stats['contacts']}")
            logger.info(f"ETAPA 4 concluida: {self.stats['contacts']} registros")
            
            # Validação e relatório de qualidade
            self.validate_step4_contacts()
            
        except Exception as e:
            logger.error(f"Erro na ETAPA 4: {e}")
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
        # Sempre ler o destino dinamicamente (não cachear)
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        
        print("\n" + "="*80)
        print(f"INICIANDO MIGRACAO: SQL Server PRD -> PostgreSQL {destino} ({schema})")
        print("="*80)
        print(f"Data/Hora: {datetime.now()}")
        print("="*80)
        
        # Log de configuracoes no inicio
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
            # ETAPA 1: Customer Segments (primeiro, pois pode ser referenciado)
            self.step1_migrate_customer_segments()
            
            # ETAPA 2: Customers (segundo, pois addresses referencia customers)
            self.step2_migrate_customers()
            
            # ETAPA 3: Addresses (terceiro, referencia customers)
            self.step3_migrate_addresses()
            
            # ETAPA 4: Contacts (quarto, referencia customers)
            self.step4_migrate_contacts()
            
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
            print(f"  Customer Segments: {self.stats['customer_segments']}")
            print(f"  Customers: {self.stats['customers']}")
            print(f"  Addresses: {self.stats['addresses']}")
            print(f"  Contacts: {self.stats['contacts']}")
            print(f"  Erros: {len(self.stats['errors'])}")
            
            logger.info(f"Duracao: {duration}")
            logger.info(f"Customer Segments: {self.stats['customer_segments']}")
            logger.info(f"Customers: {self.stats['customers']}")
            logger.info(f"Addresses: {self.stats['addresses']}")
            logger.info(f"Contacts: {self.stats['contacts']}")
            logger.info(f"Erros: {len(self.stats['errors'])}")
            
            if self.stats['errors']:
                print(f"\nAVISO - Total de erros: {len(self.stats['errors'])}")
                print("Verifique o arquivo customers_to_core_log.txt para detalhes")
                logger.warning(f"Total de erros: {len(self.stats['errors'])}")
            else:
                print("\nOK - Nenhum erro encontrado!")
                logger.info("Nenhum erro encontrado!")
            
        except Exception as e:
            logger.error("="*80)
            logger.error("ERRO CRITICO NA MIGRACAO!")
            logger.error("="*80)
            logger.error(f"Erro: {str(e)}")
            print(f"\nERRO CRITICO: {e}")
            raise


if __name__ == "__main__":
    import sys
    
    # Verificar se foi passado limite via argumento
    limit_rows = 0
    if '--limit' in sys.argv:
        idx = sys.argv.index('--limit')
        if idx + 1 < len(sys.argv):
            try:
                limit_rows = int(sys.argv[idx + 1])
            except ValueError:
                print("AVISO: Valor invalido para --limit, usando 0 (todos os dados)")
    
    migration = CustomersMigration(limit_rows=limit_rows)
    
    # Verificar se deve executar apenas uma etapa específica
    if len(sys.argv) > 1 and sys.argv[1] == "--step3":
        print("\n" + "="*80)
        print("EXECUTANDO APENAS ETAPA 3: MIGRACAO DE ADDRESSES")
        print("="*80)
        print(f"Data/Hora: {datetime.now()}")
        print("="*80)
        logger.info("="*80)
        logger.info("EXECUTANDO APENAS ETAPA 3: MIGRACAO DE ADDRESSES")
        logger.info(f"Data/Hora: {datetime.now()}")
        logger.info("="*80)
        
        start_time = datetime.now()
        
        try:
            # Carregar mapeamento de customer IDs (necessário para addresses)
            print("\n[ETAPA 3] Carregando mapeamento de customers...")
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.customers")
            for row in cursor_pg.fetchall():
                migration.customer_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            print(f"[ETAPA 3] {len(migration.customer_id_map)} customers carregados")
            
            # Executar apenas a etapa 3
            migration.step3_migrate_addresses()
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            print("\n" + "="*80)
            print("ETAPA 3 CONCLUIDA!")
            print("="*80)
            logger.info("="*80)
            logger.info("ETAPA 3 CONCLUIDA!")
            logger.info("="*80)
            
            print(f"\nDuracao: {duration}")
            print(f"\nESTATISTICAS ETAPA 3:")
            print(f"  Addresses: {migration.stats['addresses']}")
            print(f"  Erros: {len(migration.stats['errors'])}")
            
            logger.info(f"Duracao: {duration}")
            logger.info(f"Addresses: {migration.stats['addresses']}")
            logger.info(f"Erros: {len(migration.stats['errors'])}")
            
            if migration.stats['errors']:
                print(f"\nAVISO - Total de erros: {len(migration.stats['errors'])}")
                print("Verifique o arquivo customers_to_core_log.txt para detalhes")
                logger.warning(f"Total de erros: {len(migration.stats['errors'])}")
            else:
                print("\nOK - Nenhum erro encontrado!")
                logger.info("Nenhum erro encontrado!")
                
        except Exception as e:
            logger.error("="*80)
            logger.error("ERRO CRITICO NA ETAPA 3!")
            logger.error("="*80)
            logger.error(f"Erro: {str(e)}")
            print(f"\nERRO CRITICO: {e}")
            raise
    elif len(sys.argv) > 1 and sys.argv[1] == "--step4":
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("EXECUTANDO APENAS ETAPA 4: MIGRACAO DE CONTACTS")
        print("="*80)
        print(f"Data/Hora: {datetime.now()}")
        print("="*80)
        logger.info("="*80)
        logger.info("EXECUTANDO APENAS ETAPA 4: MIGRACAO DE CONTACTS")
        logger.info(f"Data/Hora: {datetime.now()}")
        logger.info(f"Ambiente Destino: {destino}")
        logger.info(f"Schema Destino: {schema}")
        logger.info(f"Limite de Linhas: {'TODOS' if limit_rows == 0 else limit_rows}")
        logger.info(f"Tamanho do Chunk: {CHUNK_SIZE}")
        logger.info("="*80)
        
        start_time = datetime.now()
        
        try:
            # Carregar mapeamento de customer IDs (necessário para contacts)
            print("\n[ETAPA 4] Carregando mapeamento de customers...")
            schema = get_schema_atual()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.customers")
            for row in cursor_pg.fetchall():
                migration.customer_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            print(f"[ETAPA 4] {len(migration.customer_id_map)} customers carregados")
            
            # Executar apenas a etapa 4
            migration.step4_migrate_contacts()
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            print("\n" + "="*80)
            print("ETAPA 4 CONCLUIDA!")
            print("="*80)
            logger.info("="*80)
            logger.info("ETAPA 4 CONCLUIDA!")
            logger.info("="*80)
            
            print(f"\nDuracao: {duration}")
            print(f"\nESTATISTICAS ETAPA 4:")
            print(f"  Contacts: {migration.stats['contacts']}")
            print(f"  Erros: {len(migration.stats['errors'])}")
            
            logger.info(f"Duracao: {duration}")
            logger.info(f"Contacts: {migration.stats['contacts']}")
            logger.info(f"Erros: {len(migration.stats['errors'])}")
            
            if migration.stats['errors']:
                print(f"\nAVISO - Total de erros: {len(migration.stats['errors'])}")
                print("Verifique o arquivo customers_to_core_log.txt para detalhes")
                logger.warning(f"Total de erros: {len(migration.stats['errors'])}")
            else:
                print("\nOK - Nenhum erro encontrado!")
                logger.info("Nenhum erro encontrado!")
                
        except Exception as e:
            logger.error("="*80)
            logger.error("ERRO CRITICO NA ETAPA 4!")
            logger.error("="*80)
            logger.error(f"Erro: {str(e)}")
            print(f"\nERRO CRITICO: {e}")
            raise
    else:
        # Executar migração completa
        migration.run()

