"""
Script de migração de dados: SQL Server PRD -> PostgreSQL Destino
Migra dados das tabelas: store_segments, retail_chains, store_brands, stores, store_cnpjs, addresses, contacts

SCHEMAS CONFIGURADOS:
- HML: gmcore
- PRD: core
"""

import sys
import os
import uuid
import logging
import re
import json
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from psycopg2.extras import execute_values

# Adicionar diretório utils ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
# ⚠️ CRÍTICO: Importar usando o mesmo caminho do orchestrator para garantir mesma referência
from utils.database_connection import DatabaseConnection
from utils.fetch_canal_estabelecimento_ids import fetch_distinct_id_canal_from_estabelecimento_ids
from utils.fetch_segmento_produto_ids import resolve_id_segmento_produto_for_json
from utils.municipio_lookup import load_municipio_lookup, municipal_code_from_origem

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

# Configurar logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Remover handlers existentes para evitar duplicacao
if logger.handlers:
    logger.handlers.clear()

# Handler para arquivo (modo 'a' para append - log é truncado apenas no orchestrator)
# Arquivo na raiz do projeto
try:
    log_file_path = os.path.join(os.path.dirname(__file__), '..', 'log_execution.txt')
    log_file_path = os.path.abspath(log_file_path)
    file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
except Exception as e:
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


class StoresMigration:
    """Classe para executar a migração de dados de stores"""
    
    def __init__(self, limit_rows=0, id_orcamento_filter=None, 
                 data_aviso_previo_min=None, data_inicio_operacao_max=None, 
                 status_pedido_filter=None, clear_data=False):
        self.stats = {
            'store_segments': 0,
            'retail_chains': 0,
            'store_brands': 0,
            'stores': 0,
            'store_cnpjs': 0,
            'addresses': 0,
            'contacts': 0,
            'errors': []
        }
        self.store_segment_id_map = {}  # Map: legado_id -> uuid
        self.retail_chain_id_map = {}   # Map: legado_id -> uuid
        self.store_brand_id_map = {}    # Map: legado_id -> uuid
        self.store_id_map = {}          # Map: legado_id -> uuid
        self.limit_rows = limit_rows     # 0 = todos, > 0 = limitar quantidade
        self.id_orcamento_filter = id_orcamento_filter  # Lista de IdOrcamento para filtrar
        self.data_aviso_previo_min = data_aviso_previo_min  # Data mínima para DataAvisoPrevio
        self.data_inicio_operacao_max = data_inicio_operacao_max  # Data máxima para DataInicioOperacao
        self.status_pedido_filter = status_pedido_filter if status_pedido_filter else []  # Lista de StatusPedido para filtrar
        self.clear_data = clear_data  # Se True, força TRUNCATE mesmo com filtros aplicados
        self.json_updated_this_run = False  # Flag para evitar múltiplas atualizações do JSON na mesma execução
        
        # Caminho do arquivo JSON de filtros do contracts
        contracts_dir = os.path.join(os.path.dirname(__file__), '..', 'contracts')
        self.filter_json_path = os.path.join(contracts_dir, 'contracts_filter_main.json')
    
    def should_include_legacy_id(self):
        """Retorna True se deve incluir legacy_id (HML e PRD)"""
        # legacy_id existe tanto em HML quanto em PRD
        return True
    
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
    
    def _get_filter_ids_for_validation(self):
        """
        Retorna os IDs filtrados para validação (do JSON ou ViewOrcamentosLojas)
        Retorna: dict com IdEstabelecimento, IdBandeira, IdRede, etc.
        """
        filter_data = self.load_filter_json()
        if filter_data and 'aggregated_ids' in filter_data:
            return filter_data['aggregated_ids']
        return {}

    def _resolve_canal_estabelecimento_ids_for_step1(self) -> Optional[List[int]]:
        """
        Escopo de CanalEstabelecimento.Id para store_segments.
        None: sem aggregated_ids no JSON → comportamento legado (todos os canais).
        lista (pode ser vazia): restringe inserções a CanalEstabelecimento.Id nesses valores.
        """
        filter_data = self.load_filter_json()
        if not filter_data or not isinstance(filter_data.get("aggregated_ids"), dict):
            return None
        agg = filter_data["aggregated_ids"]
        raw = agg.get("IdCanalEstabelecimento")
        explicit = sorted({int(x) for x in (raw or []) if x is not None})
        if explicit:
            return explicit
        estab = [int(x) for x in (agg.get("IdEstabelecimento") or []) if x is not None]
        if estab:
            return fetch_distinct_id_canal_from_estabelecimento_ids(estab)
        return []

    def _ensure_store_scope_ids_in_filter_json(self) -> None:
        """
        Se o JSON tem aggregated_ids sem IdEstabelecimento/IdCanal, preenche pela view
        (IdOrcamento do CLI ou do JSON). Evita escopo [] antes da ETAPA 2 de stores.
        """
        fd = self.load_filter_json()
        agg = fd.get("aggregated_ids") if fd and isinstance(fd.get("aggregated_ids"), dict) else None
        if not agg:
            return
        if (agg.get("IdCanalEstabelecimento") or []) or (agg.get("IdEstabelecimento") or []):
            return
        orch = None
        if self.id_orcamento_filter:
            orch = list(self.id_orcamento_filter)
        if not orch:
            raw = agg.get("IdOrcamento") or []
            orch = list(raw) if raw else None
        if not orch and fd and isinstance(fd.get("filters_applied"), dict):
            fo = fd["filters_applied"].get("id_orcamento")
            orch = list(fo) if fo else None
        if not orch:
            return
        try:
            self.save_filter_json_from_view(id_orcamento_list=orch)
            logger.info(
                "[ETAPA 1] contracts_filter_main.json: IdEstabelecimento/IdCanal preenchidos pela view "
                "antes de store_segments."
            )
        except Exception as e:
            logger.warning("[ETAPA 1] Não foi possível preencher lojas no JSON pela view: %s", e)

    def save_filter_json_from_view(self, id_orcamento_list=None):
        """
        Atualiza o JSON de filtros do contracts com IDs coletados da ViewOrcamentosLojas
        Chamado quando stores busca diretamente da ViewOrcamentosLojas (com ou sem LIMIT)
        Aplica os mesmos filtros do JSON existente (DataAvisoPrevio, DataInicioOperacao)
        Usa queries separadas para cada tipo de ID para garantir resultados corretos
        
        Args:
            id_orcamento_list: Lista opcional de IdOrcamento para filtrar (se None, busca todos)
        """
        try:
            print("[STORES] Coletando IDs únicos da ViewOrcamentosLojas para atualizar JSON...")
            
            # ⚠️ IMPORTANTE: Usar filtros de data da linha de comando (CMD) com prioridade sobre JSON
            # Prioridade: CMD > JSON > None
            # ⚠️ CRÍTICO: id_orcamento_list sempre vem do CMD, nunca do JSON (para garantir que o filtro seja aplicado)
            data_aviso_previo_min = self.data_aviso_previo_min
            data_inicio_operacao_max = self.data_inicio_operacao_max
            status_pedido_filter = self.status_pedido_filter
            
            # Se CMD não especificou filtros, usar do JSON se existir
            existing_data = self.load_filter_json()
            if data_aviso_previo_min is None and existing_data and 'filters_applied' in existing_data:
                filters = existing_data['filters_applied']
                data_aviso_previo_min = filters.get('data_aviso_previo_min')
            
            if data_inicio_operacao_max is None and existing_data and 'filters_applied' in existing_data:
                filters = existing_data['filters_applied']
                data_inicio_operacao_max = filters.get('data_inicio_operacao_max')
            
            if len(status_pedido_filter) == 0 and existing_data and 'filters_applied' in existing_data:
                filters = existing_data['filters_applied']
                json_status_pedido = filters.get('status_pedido')
                if json_status_pedido:
                    status_pedido_filter = json_status_pedido
            
            # Log dos filtros que estão sendo usados
            logger.info(f"[STORES] Filtros aplicados em save_filter_json_from_view: id_orcamento={id_orcamento_list if id_orcamento_list else 'None'}, data_aviso_previo_min={data_aviso_previo_min}, data_inicio_operacao_max={data_inicio_operacao_max}, status_pedido={status_pedido_filter}")
            
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            # Construir WHERE clause comum
            where_conditions = []
            query_params = []
            
            # Filtro IdOrcamento
            if id_orcamento_list:
                placeholders = ','.join(['?' for _ in id_orcamento_list])
                where_conditions.append(f"v.IdOrcamento IN ({placeholders})")
                query_params.extend(id_orcamento_list)
            
            # Filtro DataAvisoPrevio (data mínima)
            if data_aviso_previo_min:
                where_conditions.append("(CONVERT(DATE, v.DataAvisoPrevio) >= ? OR v.DataAvisoPrevio IS NULL)")
                query_params.append(data_aviso_previo_min)
            
            # Filtro DataInicioOperacao (data máxima)
            if data_inicio_operacao_max:
                where_conditions.append("CONVERT(DATE, v.DataInicioOperacao) <= ?")
                query_params.append(data_inicio_operacao_max)
            
            # Filtro StatusPedido
            if len(status_pedido_filter) > 0:
                placeholders = ','.join(['?' for _ in status_pedido_filter])
                where_conditions.append(f"v.StatusPedido IN ({placeholders})")
                query_params.extend(status_pedido_filter)
            
            # Construir WHERE clause exatamente como na query do usuário (com WHERE 1 = 1)
            where_clause = ""
            if where_conditions:
                where_clause = " WHERE 1 = 1 AND " + " AND ".join(where_conditions)
            
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
                        # Escapar aspas simples em strings
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
            aggregated_ids['IdSegmentoProduto'] = resolve_id_segmento_produto_for_json(
                aggregated_ids['IdCliente'],
                aggregated_ids['IdOrcamento'],
                data_aviso_previo_min,
                data_inicio_operacao_max,
                status_pedido_filter,
            )
            
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
            estab_for_canal = [int(x) for x in aggregated_ids['IdEstabelecimento'] if x is not None]
            aggregated_ids['IdCanalEstabelecimento'] = fetch_distinct_id_canal_from_estabelecimento_ids(
                estab_for_canal
            )
            
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
            logger.info("[STORES] DEBUG - QUERIES PARA COLETA DE IDs (ESTRUTURA SIMPLIFICADA)")
            logger.info("="*80)
            logger.info(f"[STORES] Parâmetros da query: {query_params}")
            logger.info("="*80)
            
            # Log da query formatada para SSMS (exemplo com IdEstabelecimento)
            logger.info("="*80)
            logger.info("[STORES] QUERY PRONTA PARA COPIAR E COLAR NO SSMS (Exemplo: IdEstabelecimento)")
            logger.info("="*80)
            if query_params:
                logger.info("\n-- Query para IdEstabelecimento:")
                logger.info(format_query_for_ssms(query_id_estabelecimento, query_params))
            else:
                logger.info("\n-- Query para IdEstabelecimento:")
                logger.info(query_id_estabelecimento)
            logger.info("="*80)
            
            # Log dos resultados
            for id_type, id_list in aggregated_ids.items():
                preview = id_list[:10] if id_list else []
                logger.info(f"[STORES] Resultado {id_type}: {len(id_list)} registros - {preview}")
            
            cursor_sql.close()
            conn_sql.close()
            
            print(f"[STORES] IDs coletados: {len(aggregated_ids['IdOrcamento'])} contratos, "
                  f"{len(aggregated_ids['IdEstabelecimento'])} estabelecimentos, "
                  f"{len(aggregated_ids['IdCanalEstabelecimento'])} canais estabelecimento, "
                  f"{len(aggregated_ids['IdBandeira'])} bandeiras, "
                  f"{len(aggregated_ids['IdRede'])} redes")
            
            # Preparar estrutura do JSON (preservar filtros existentes)
            filter_data = {
                'filters_applied': {
                    'id_orcamento': id_orcamento_list if id_orcamento_list else (existing_data.get('filters_applied', {}).get('id_orcamento', []) if existing_data else []),
                    'data_aviso_previo_min': data_aviso_previo_min,
                    'data_inicio_operacao_max': data_inicio_operacao_max,
                    'limit_rows': self.limit_rows,
                    'clear_data': existing_data.get('filters_applied', {}).get('clear_data', False) if existing_data else False
                },
                'aggregated_ids': {
                    'IdOrcamento': sorted(aggregated_ids['IdOrcamento']),
                    'IdCliente': sorted([x for x in aggregated_ids['IdCliente'] if x is not None]),
                    'IdSegmentoProduto': sorted([x for x in aggregated_ids['IdSegmentoProduto'] if x is not None]),
                    'IdEstabelecimento': sorted([x for x in aggregated_ids['IdEstabelecimento'] if x is not None]),
                    'IdCanalEstabelecimento': sorted([x for x in aggregated_ids['IdCanalEstabelecimento'] if x is not None]),
                    'IdBandeira': sorted([x for x in aggregated_ids['IdBandeira'] if x is not None]),
                    'IdRede': sorted([x for x in aggregated_ids['IdRede'] if x is not None])
                },
                'execution_info': {
                    'timestamp': datetime.now().isoformat(),
                    'source': 'stores',
                    'total_stores_migrated': self.stats.get('stores', 0)
                }
            }
            
            # Salvar JSON
            with open(self.filter_json_path, 'w', encoding='utf-8') as f:
                json.dump(filter_data, f, indent=2, ensure_ascii=False)
            
            print(f"[STORES] JSON atualizado: {len(aggregated_ids['IdOrcamento'])} contratos, "
                  f"{len(aggregated_ids['IdEstabelecimento'])} estabelecimentos, "
                  f"{len(aggregated_ids['IdCanalEstabelecimento'])} canais estabelecimento, "
                  f"{len(aggregated_ids['IdBandeira'])} bandeiras, "
                  f"{len(aggregated_ids['IdRede'])} redes")
            logger.info(f"[STORES] JSON atualizado: {self.filter_json_path}")
            logger.info(f"[STORES] IDs coletados - Contratos: {len(aggregated_ids['IdOrcamento'])}, "
                       f"Estabelecimentos: {len(aggregated_ids['IdEstabelecimento'])}, "
                       f"Canais estabelecimento: {len(aggregated_ids['IdCanalEstabelecimento'])}, "
                       f"Bandeiras: {len(aggregated_ids['IdBandeira'])}, "
                       f"Redes: {len(aggregated_ids['IdRede'])}")
            
        except Exception as e:
            logger.error(f"Erro ao atualizar arquivo de filtros: {e}")
            print(f"AVISO - Erro ao atualizar arquivo de filtros: {e}")
    
    def _get_or_create_default_store_brand(self, schema: str, include_legacy: bool):
        """Obtém ou cria um store_brand padrão chamado 'Teste store_brands'"""
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        try:
            # Tentar buscar store_brand padrão por descrição
            cursor_pg.execute(f"SELECT id FROM {schema}.store_brands WHERE description = %s", ('Teste store_brands',))
            result = cursor_pg.fetchone()
            
            if result:
                # Já existe, retornar o ID
                default_id = result[0]
                cursor_pg.close()
                conn_pg.close()
                print(f"[ETAPA 4] Store brand padrão encontrado: {default_id}")
                return default_id
            
            # Não existe, criar um novo
            default_id = uuid.uuid4()
            now = datetime.now()
            
            # Buscar um retail_chain_id padrão (primeiro disponível ou None)
            cursor_pg.execute(f"SELECT id FROM {schema}.retail_chains LIMIT 1")
            retail_chain_result = cursor_pg.fetchone()
            retail_chain_id = str(retail_chain_result[0]) if retail_chain_result else None
            
            # Buscar um store_segment_id padrão (primeiro disponível ou None)
            cursor_pg.execute(f"SELECT id FROM {schema}.store_segments LIMIT 1")
            store_segment_result = cursor_pg.fetchone()
            store_segment_id = str(store_segment_result[0]) if store_segment_result else None
            
            if include_legacy:
                insert_query = f"""
                INSERT INTO {schema}.store_brands (
                    id, description, abras_code, retail_chain_id, store_segment_id,
                    is_active, created_at, updated_at, legacy_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor_pg.execute(insert_query, (
                    str(default_id),
                    'Teste store_brands',
                    'empty',
                    retail_chain_id,
                    store_segment_id,
                    True,
                    now,
                    now,
                    0  # legacy_id padrão = 0
                ))
            else:
                insert_query = f"""
                INSERT INTO {schema}.store_brands (
                    id, description, abras_code, retail_chain_id, store_segment_id,
                    is_active, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor_pg.execute(insert_query, (
                    str(default_id),
                    'Teste store_brands',
                    'empty',
                    retail_chain_id,
                    store_segment_id,
                    True,
                    now,
                    now
                ))
            
            conn_pg.commit()
            cursor_pg.close()
            conn_pg.close()
            print(f"[ETAPA 4] Store brand padrão criado: {default_id}")
            logger.info(f"Store brand padrão 'Teste store_brands' criado: {default_id}")
            return default_id
            
        except Exception as e:
            logger.error(f"Erro ao criar/buscar store_brand padrão: {e}")
            cursor_pg.close()
            conn_pg.close()
            raise
    
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
    
    def clean_cnpj(self, value: Optional[str]) -> Optional[str]:
        """Remove formatação de CNPJ"""
        if not value:
            return None
        cleaned = re.sub(r'[^\d]', '', str(value))
        if not cleaned or cleaned == '0' * len(cleaned):
            return None
        return cleaned
    
    # ========================================================================
    # MÉTODOS VETORIZADOS COM PANDAS
    # ========================================================================
    
    def clean_string_vectorized(self, series: pd.Series, max_length: Optional[int] = None) -> pd.Series:
        """Limpa e trunca strings de forma vetorizada"""
        # Converter para string (mantendo None como None)
        cleaned = series.astype(str)
        # Substituir 'nan', 'None' por string vazia para processar
        cleaned = cleaned.replace(['nan', 'None', 'NULL', 'NONE'], '')
        # Remover espaços
        cleaned = cleaned.str.strip()
        # Substituir valores vazios, 'null', 'none' por None
        cleaned = cleaned.replace(['', 'null', 'none'], None)
        # Truncar se necessário
        if max_length:
            cleaned = cleaned.str[:max_length]
        return cleaned
    
    def clean_cnpj_vectorized(self, series: pd.Series) -> pd.Series:
        """Remove formatação de CNPJ de forma vetorizada"""
        # Converter para string
        cleaned = series.astype(str)
        # Substituir 'nan', 'None' por string vazia para processar
        cleaned = cleaned.replace(['nan', 'None', 'NULL', 'NONE'], '')
        # Remover tudo que não é dígito
        cleaned = cleaned.str.replace(r'[^\d]', '', regex=True)
        # Substituir strings de zeros (qualquer tamanho) por None
        cleaned = cleaned.apply(lambda x: None if x and x == '0' * len(x) else x)
        # Substituir valores vazios por None
        cleaned = cleaned.replace('', None)
        return cleaned
    
    def process_dataframe(self, all_rows: List[Tuple], column_names: List[str], 
                         transformations: Dict[str, callable] = None,
                         map_lookups: Dict[str, Dict] = None) -> pd.DataFrame:
        """
        Processa dados usando DataFrame com operações vetorizadas
        
        Args:
            all_rows: Lista de tuplas com os dados do SQL Server
            column_names: Lista com nomes das colunas
            transformations: Dict com funções de transformação por coluna
            map_lookups: Dict com mapeamentos (legacy_id -> uuid) por coluna
        
        Returns:
            DataFrame processado
        """
        # Criar DataFrame
        df = pd.DataFrame.from_records(all_rows, columns=column_names)
        
        # Aplicar transformações se especificadas
        if transformations:
            for col, func in transformations.items():
                if col in df.columns:
                    df[col] = func(df[col])
        
        # Aplicar lookups de mapeamento
        if map_lookups:
            for col, mapping_dict in map_lookups.items():
                if col in df.columns:
                    # Criar série de lookup
                    df[f'{col}_mapped'] = df[col].map(mapping_dict)
        
        return df
    
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
        Faz DELETE em uma tabela filtrando por legacy_id
        
        Args:
            table_name: Nome da tabela
            legacy_ids: Lista de legacy_ids para deletar
            schema: Schema (opcional, usa get_schema_atual() se None)
        """
        if schema is None:
            schema = get_schema_atual()
        
        if not legacy_ids:
            logger.info(f"Nenhum legacy_id fornecido para deletar de {schema}.{table_name}")
            print(f"[INFO] Nenhum legacy_id fornecido para deletar de {schema}.{table_name}")
            return
        
        conn = None
        try:
            conn = DatabaseConnection.get_postgresql_destino_connection()
            cursor = conn.cursor()
            
            # Verificar se a coluna legacy_id existe
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = '{schema}' 
                AND table_name = '{table_name}' 
                AND column_name = 'legacy_id'
            """)
            has_legacy_id = cursor.fetchone() is not None
            
            if not has_legacy_id:
                logger.warning(f"Tabela {schema}.{table_name} não possui coluna legacy_id. Não é possível deletar por filtro.")
                print(f"[AVISO] Tabela {schema}.{table_name} não possui coluna legacy_id. Pulando limpeza filtrada.")
                cursor.close()
                conn.close()
                return
            
            # Deletar registros com legacy_id na lista
            query = f"DELETE FROM {schema}.{table_name} WHERE legacy_id = ANY(%s)"
            cursor.execute(query, (legacy_ids,))
            deleted_count = cursor.rowcount
            conn.commit()
            
            logger.info(f"Tabela {schema}.{table_name}: {deleted_count} registros deletados (de {len(legacy_ids)} legacy_ids fornecidos)")
            print(f"OK - Tabela {schema}.{table_name}: {deleted_count} registros deletados")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Erro ao deletar registros da tabela {schema}.{table_name}: {e}")
            if conn:
                conn.rollback()
                conn.close()
            raise
    
    def clean_table(self, table_name: str, legacy_ids: List[int] = None, schema: str = None):
        """
        Limpa tabela usando TRUNCATE ou DELETE baseado em filtros
        
        Args:
            table_name: Nome da tabela
            legacy_ids: Lista de legacy_ids para filtrar (se None, usa TRUNCATE)
            schema: Schema (opcional)
        """
        if legacy_ids:
            self.delete_table_with_filter(table_name, legacy_ids, schema)
        else:
            self.truncate_table(table_name, schema)
    
    def delete_polymorphic_table(self, table_name: str, entity_type: str, type_column: str, schema: str = None):
        """
        Faz DELETE em tabela polimórfica filtrando por tipo de entidade
        Exemplo: delete_polymorphic_table('contacts', 'Store', 'contactable_type')
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
    
    # ========================================================================
    # ETAPA 1: STORE_SEGMENTS
    # ========================================================================
    
    def validate_step1_store_segments(self):
        """Validação e relatório de qualidade - ETAPA 1"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 1: STORE_SEGMENTS")
        print("-"*80)
        
        try:
            canal_scope = self._resolve_canal_estabelecimento_ids_for_step1()
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            if canal_scope is None:
                if self.limit_rows > 0:
                    cursor_sql.execute(
                        f"SELECT COUNT(*) FROM (SELECT TOP {self.limit_rows} Id FROM CanalEstabelecimento ORDER BY Id) AS limited"
                    )
                else:
                    cursor_sql.execute("SELECT COUNT(*) FROM CanalEstabelecimento")
            elif len(canal_scope) == 0:
                origem_count = 0
                cursor_sql.close()
                conn_sql.close()
                cursor_sql = None
            else:
                origem_count = 0
                chunk_sz = 1000
                for i in range(0, len(canal_scope), chunk_sz):
                    chunk = canal_scope[i : i + chunk_sz]
                    ph = ",".join(["?" for _ in chunk])
                    cursor_sql.execute(
                        f"SELECT COUNT(*) FROM CanalEstabelecimento WHERE Id IN ({ph})", chunk
                    )
                    origem_count += cursor_sql.fetchone()[0]
            if cursor_sql is not None:
                if canal_scope is None:
                    origem_count = cursor_sql.fetchone()[0]
                cursor_sql.close()
                conn_sql.close()
            
            # Contar destino
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.store_segments")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - CanalEstabelecimento):")
            print(f"  Total de registros: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.store_segments):")
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
    
    def step1_migrate_store_segments(self):
        """ETAPA 1: Migrar store_segments"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 1: MIGRANDO STORE_SEGMENTS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 1: Migrando store_segments")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        # Truncate
        print("\n[ETAPA 1] Limpando tabela store_segments...")
        self.truncate_table('store_segments')
        
        # JSON pode ter aggregated_ids (ex.: pós-customers) sem lojas — preencher antes do escopo
        self._ensure_store_scope_ids_in_filter_json()
        
        # Buscar dados do SQL Server (escopo: contracts_filter_main.json → aggregated_ids)
        print("[ETAPA 1] Buscando dados do SQL Server...")
        canal_scope = self._resolve_canal_estabelecimento_ids_for_step1()
        if canal_scope is None:
            logger.info("[ETAPA 1] store_segments: sem aggregated_ids no JSON — migrando todos os CanalEstabelecimento (legado).")
            print("[ETAPA 1] Escopo: todos os registros de CanalEstabelecimento (JSON sem aggregated_ids).")
        elif len(canal_scope) == 0:
            logger.warning("[ETAPA 1] store_segments: escopo vazio (IdCanalEstabelecimento / IdEstabelecimento) — nenhuma linha será inserida.")
            print("[ETAPA 1] AVISO: escopo de canais vazio no JSON — nenhum store_segment a inserir.")
        else:
            logger.info(f"[ETAPA 1] store_segments: escopo filtrado — {len(canal_scope)} CanalEstabelecimento.Id (JSON / Estabelecimento).")
            print(f"[ETAPA 1] Escopo filtrado: {len(canal_scope)} CanalEstabelecimento.Id (contracts_filter_main.json).")

        base_select = """
        SELECT 
            Id,
            Nome,
            Ativo,
            DataInclusao,
            DataAlteracao
        FROM CanalEstabelecimento
        """
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        all_rows = []
        try:
            if canal_scope is None:
                sql_query = base_select + "\n        ORDER BY Id\n"
                if self.limit_rows > 0:
                    sql_query = sql_query.replace(
                        "ORDER BY Id",
                        f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY",
                    )
                cursor_sql.execute(sql_query)
                all_rows = list(cursor_sql.fetchall())
            elif len(canal_scope) == 0:
                all_rows = []
            else:
                chunk_sz = 1000
                seen_ids = set()
                for i in range(0, len(canal_scope), chunk_sz):
                    chunk = canal_scope[i : i + chunk_sz]
                    ph = ",".join(["?" for _ in chunk])
                    sql_query = base_select + f"\n        WHERE Id IN ({ph})\n        ORDER BY Id\n"
                    if self.limit_rows > 0 and len(all_rows) >= self.limit_rows:
                        break
                    cursor_sql.execute(sql_query, chunk)
                    for row in cursor_sql.fetchall():
                        rid = row[0]
                        if rid not in seen_ids:
                            seen_ids.add(rid)
                            all_rows.append(row)
                            if self.limit_rows > 0 and len(all_rows) >= self.limit_rows:
                                break
                    if self.limit_rows > 0 and len(all_rows) >= self.limit_rows:
                        break
        finally:
            cursor_sql.close()
            conn_sql.close()

        # Carregar TODOS os dados na memória de uma vez
        print("[ETAPA 1] Carregando dados na memória...")
        
        print(f"[ETAPA 1] {len(all_rows)} registros carregados. Processando conversões em massa (vetorizado)...")
        
        # Processar conversões em massa usando DataFrame (vetorizado)
        include_legacy = self.should_include_legacy_id()
        
        # Criar DataFrame usando from_records que é mais adequado para dados de banco
        df = pd.DataFrame.from_records(all_rows, columns=['Id', 'Nome', 'Ativo', 'DataInclusao', 'DataAlteracao'])
        
        # Aplicar transformações vetorizadas
        df['legacy_id'] = df['Id']
        df['nome'] = self.clean_string_vectorized(df['Nome'])
        df['ativo'] = df['Ativo'].fillna(False).astype(bool)
        df['data_inclusao'] = df['DataInclusao']
        df['data_alteracao'] = df['DataAlteracao'].fillna(df['DataInclusao'])
        
        # Remover linhas com erros (nome None após limpeza)
        df = df[df['nome'].notna()]
        
        # Converter DataFrame diretamente para lista de tuplas (otimizado)
        if include_legacy:
            processed_tuples = list(zip(
                df['nome'].tolist(),
                df['ativo'].tolist(),
                df['data_inclusao'].tolist(),
                df['data_alteracao'].tolist(),
                df['legacy_id'].tolist()
            ))
            legacy_ids_list = df['legacy_id'].tolist()
        else:
            processed_tuples = list(zip(
                df['nome'].tolist(),
                df['ativo'].tolist(),
                df['data_inclusao'].tolist(),
                df['data_alteracao'].tolist()
            ))
            processed_data = df[['legacy_id', 'nome', 'ativo', 'data_inclusao', 'data_alteracao']].to_dict('records')
        
        print(f"[ETAPA 1] {len(processed_tuples)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Query de insert usando gen_random_uuid() - formato para execute_values
        if include_legacy:
            insert_query = f"""
            INSERT INTO {schema}.store_segments (
                id, name, is_active, created_at, updated_at, legacy_id
            ) VALUES %s
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s)"
        else:
            insert_query = f"""
            INSERT INTO {schema}.store_segments (
                id, name, is_active, created_at, updated_at
            ) VALUES %s
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s)"
        
        chunk_num = 0
        total_processed = 0
        all_legacy_ids_inserted = []  # Para buscar UUIDs depois
        
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
                        if include_legacy:
                            chunk_legacy_ids = [row[-1] for row in chunk]  # último elemento é legacy_id
                            all_legacy_ids_inserted.extend(chunk_legacy_ids)
                        
                        total_processed += len(chunk)
                        self.stats['store_segments'] += len(chunk)
                        
                    except Exception as batch_error:
                        error_msg = f"Erro ao inserir batch de store_segments: {batch_error}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
                
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
                    FROM {schema}.store_segments 
                    WHERE legacy_id = ANY(%s)
                """, (all_legacy_ids_inserted,))
                for uuid_row, leg_id in cursor_pg.fetchall():
                    self.store_segment_id_map[leg_id] = uuid_row
                print(f"[ETAPA 1] {len(self.store_segment_id_map)} UUIDs mapeados")
            
            # Se não tem legacy_id, buscar UUIDs por ordem
            if not include_legacy:
                cursor_pg.execute(f"SELECT id, created_at FROM {schema}.store_segments ORDER BY created_at")
                uuid_rows = cursor_pg.fetchall()
                for idx, (uuid_row, created_at) in enumerate(uuid_rows):
                    if idx < len(processed_data):
                        leg_id = processed_data[idx]['legacy_id']
                        self.store_segment_id_map[leg_id] = uuid_row
            
            print(f"\n[ETAPA 1] CONCLUIDA! Total de store_segments migrados: {self.stats['store_segments']}")
            logger.info(f"ETAPA 1 concluida: {self.stats['store_segments']} registros")
            
            self.validate_step1_store_segments()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 1: {e}")
            raise
        finally:
            try:
                if 'cursor_pg' in locals() and cursor_pg:
                    cursor_pg.close()
            except:
                pass
            try:
                if 'conn_pg' in locals() and conn_pg:
                    conn_pg.close()
            except:
                pass
    
    # ========================================================================
    # ETAPA 2: RETAIL_CHAINS
    # ========================================================================
    
    def validate_step2_retail_chains(self):
        """Validação e relatório de qualidade - ETAPA 2"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 2: RETAIL_CHAINS")
        print("-"*80)
        
        try:
            # Carregar filtros do contracts ou buscar da ViewOrcamentosLojas
            filter_ids = self._get_filter_ids_for_validation()
            id_rede_list = filter_ids.get('IdRede', [])
            
            # Contar origem aplicando os mesmos filtros
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            if id_rede_list:
                # Aplicar filtro de IdRede do JSON
                placeholders = ','.join(['?' for _ in id_rede_list])
                cursor_sql.execute(f"SELECT COUNT(*) FROM Rede WHERE Id IN ({placeholders})", id_rede_list)
            else:
                # Buscar diretamente da ViewOrcamentosLojas (mesmo em full load)
                cursor_sql.execute("SELECT COUNT(DISTINCT IdRede) FROM ViewOrcamentosLojas WHERE IdRede IS NOT NULL")
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino aplicando os mesmos filtros
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            if id_rede_list:
                # Contar apenas os retail_chains migrados nesta execução
                cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.retail_chains WHERE legacy_id = ANY(%s)", (id_rede_list,))
            else:
                # Contar todos (fallback)
                cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.retail_chains")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - Rede):")
            print(f"  Total de registros: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.retail_chains):")
            print(f"  Total de registros: {destino_count}")
            
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
    
    def step2_migrate_retail_chains(self):
        """ETAPA 2: Migrar retail_chains"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 2: MIGRANDO RETAIL_CHAINS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 2: Migrando retail_chains")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        # Carregar filtros do contracts (se existir)
        # ⚠️ IMPORTANTE: Sempre buscar da ViewOrcamentosLojas aplicando filtros de data para garantir consistência
        filter_data = None
        if self.limit_rows == 0:
            filter_data = self.load_filter_json()
        
        id_rede_filter_list = []
        
        # ⚠️ IMPORTANTE: Sempre buscar da ViewOrcamentosLojas aplicando filtros de data
        # Isso garante que os IDs correspondem aos filtros de data aplicados
        if self.limit_rows > 0:
            print(f"[ETAPA 2] LIMIT {self.limit_rows} especificado. Buscando IdRede da ViewOrcamentosLojas com LIMIT...")
            logger.info(f"[ETAPA 2] LIMIT {self.limit_rows} especificado. Buscando da ViewOrcamentosLojas")
        else:
            print("[ETAPA 2] Buscando IdRede da ViewOrcamentosLojas com filtros de data aplicados...")
            logger.info("[ETAPA 2] Buscando IdRede da ViewOrcamentosLojas com filtros de data aplicados...")
        
        conn_sql_view = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql_view = conn_sql_view.cursor()
        
        # Construir query com filtros de data aplicados
        where_conditions = []
        query_params = []
        
        # Carregar filtros de data: prioridade CMD > JSON > None
        data_aviso_previo_min = self.data_aviso_previo_min
        data_inicio_operacao_max = self.data_inicio_operacao_max
        status_pedido_filter = self.status_pedido_filter
        
        # Se CMD não especificou filtros, usar do JSON se existir
        if data_aviso_previo_min is None and filter_data and 'filters_applied' in filter_data:
            filters = filter_data['filters_applied']
            data_aviso_previo_min = filters.get('data_aviso_previo_min')
        
        if data_inicio_operacao_max is None and filter_data and 'filters_applied' in filter_data:
            filters = filter_data['filters_applied']
            data_inicio_operacao_max = filters.get('data_inicio_operacao_max')
        
        if len(status_pedido_filter) == 0 and filter_data and 'filters_applied' in filter_data:
            filters = filter_data['filters_applied']
            json_status_pedido = filters.get('status_pedido')
            if json_status_pedido:
                status_pedido_filter = json_status_pedido
        
        # Filtro IdOrcamento
        if self.id_orcamento_filter:
            placeholders = ','.join(['?' for _ in self.id_orcamento_filter])
            where_conditions.append(f"v.IdOrcamento IN ({placeholders})")
            query_params.extend(self.id_orcamento_filter)
        
        # Filtro DataAvisoPrevio (data mínima)
        if data_aviso_previo_min:
            where_conditions.append("(CONVERT(DATE, v.DataAvisoPrevio) >= ? OR v.DataAvisoPrevio IS NULL)")
            query_params.append(data_aviso_previo_min)
        
        # Filtro DataInicioOperacao (data máxima)
        if data_inicio_operacao_max:
            where_conditions.append("CONVERT(DATE, v.DataInicioOperacao) <= ?")
            query_params.append(data_inicio_operacao_max)
        
        # Filtro StatusPedido
        if len(status_pedido_filter) > 0:
            placeholders = ','.join(['?' for _ in status_pedido_filter])
            where_conditions.append(f"v.StatusPedido IN ({placeholders})")
            query_params.extend(status_pedido_filter)
        
        # Construir query completa com INNER JOIN Orcamento e filtros
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions) + " AND v.IdRede IS NOT NULL"
        else:
            where_clause = "WHERE v.IdRede IS NOT NULL"
        
        # ⚠️ LÓGICA SIMPLIFICADA: Query direta sem subquery (conforme validação)
        if self.limit_rows > 0:
            query_id_rede = f"""
            SELECT DISTINCT TOP {self.limit_rows}
                v.IdRede
            FROM ViewOrcamentosLojas v
            INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
            {where_clause}
            """
        else:
            query_id_rede = f"""
            SELECT DISTINCT
                v.IdRede
            FROM ViewOrcamentosLojas v
            INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
            {where_clause}
            """
        
        # Log da query completa para debug
        logger.info(f"[ETAPA 2] Query completa para buscar IdRede da ViewOrcamentosLojas: {query_id_rede}")
        logger.info(f"[ETAPA 2] Parâmetros da query: {query_params}")
        
        # Executar query
        if query_params:
            cursor_sql_view.execute(query_id_rede, query_params)
        else:
            cursor_sql_view.execute(query_id_rede)
        
        # Carregar resultados (já são únicos devido ao DISTINCT na query externa)
        id_rede_filter_list = [row[0] for row in cursor_sql_view.fetchall()]
        cursor_sql_view.close()
        conn_sql_view.close()
        
        print(f"[ETAPA 2] Carregados {len(id_rede_filter_list)} IdRede únicos da ViewOrcamentosLojas")
        logger.info(f"[ETAPA 2] Carregados {len(id_rede_filter_list)} IdRede únicos da ViewOrcamentosLojas")
        
        # Atualizar JSON com IDs coletados da ViewOrcamentosLojas (apenas uma vez por execução)
        if not self.json_updated_this_run:
            self.save_filter_json_from_view(id_orcamento_list=self.id_orcamento_filter)
            self.json_updated_this_run = True
        
        # Limpar tabela
        # ⚠️ IMPORTANTE: Se clear_data=True, sempre usar TRUNCATE (mesmo com filtros)
        if self.clear_data:
            print("\n[ETAPA 2] Limpando tabela retail_chains (TRUNCATE - flag --clear-data ativo)...")
            logger.info("[ETAPA 2] Flag --clear-data ativo: usando TRUNCATE mesmo com filtros")
            self.truncate_table('retail_chains')
        elif not id_rede_filter_list:
            # Se não há filtros e SEM clear_data, usar TRUNCATE (caso raro)
            print("\n[ETAPA 2] Limpando tabela retail_chains (sem filtros)...")
            self.truncate_table('retail_chains')
        else:
            # Com filtros e SEM clear_data, usar DELETE
            print("\n[ETAPA 2] Limpando registros filtrados da tabela retail_chains...")
            self.delete_table_with_filter('retail_chains', legacy_ids=id_rede_filter_list)
        
        # Buscar dados do SQL Server aplicando filtro de IdRede
        # ⚠️ CRÍTICO: Sempre filtrar pela ViewOrcamentosLojas, mesmo com --limit
        print("[ETAPA 2] Buscando dados do SQL Server...")
        query_params = []
        
        if id_rede_filter_list:
            # Aplicar filtro de IdRede do JSON ou ViewOrcamentosLojas
            placeholders = ','.join(['?' for _ in id_rede_filter_list])
            sql_query = f"""
            SELECT 
                Id,
                Nome,
                Codigo,
                Ativo,
                DataInclusao,
                DataAlteracao
            FROM Rede
            WHERE Id IN ({placeholders})
            ORDER BY Id
            """
            query_params.extend(id_rede_filter_list)
            
            # Aplicar limite se especificado
            if self.limit_rows > 0:
                sql_query = f"""
                SELECT 
                    Id,
                    Nome,
                    Codigo,
                    Ativo,
                    DataInclusao,
                    DataAlteracao
                FROM Rede
                WHERE Id IN ({placeholders})
                ORDER BY Id
                OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY
                """
        else:
            # Sem filtros: buscar apenas da ViewOrcamentosLojas (mesmo em full load ou com --limit)
            if self.limit_rows > 0:
                # Com limite: primeiro buscar IdRede da ViewOrcamentosLojas, depois aplicar limite
                sql_query = f"""
                SELECT TOP {self.limit_rows}
                    r.Id,
                    r.Nome,
                    r.Codigo,
                    r.Ativo,
                    r.DataInclusao,
                    r.DataAlteracao
                FROM Rede r
                WHERE r.Id IN (
                    SELECT DISTINCT IdRede 
                    FROM ViewOrcamentosLojas
                    WHERE IdRede IS NOT NULL
                )
                ORDER BY r.Id
                """
            else:
                # Sem limite: buscar todos da ViewOrcamentosLojas
                sql_query = """
                SELECT 
                    r.Id,
                    r.Nome,
                    r.Codigo,
                    r.Ativo,
                    r.DataInclusao,
                    r.DataAlteracao
                FROM Rede r
                WHERE r.Id IN (
                    SELECT DISTINCT IdRede 
                    FROM ViewOrcamentosLojas
                    WHERE IdRede IS NOT NULL
                )
                ORDER BY r.Id
                """
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        if query_params:
            cursor_sql.execute(sql_query, query_params)
        else:
            cursor_sql.execute(sql_query)
        
        # Carregar TODOS os dados na memória de uma vez
        print("[ETAPA 2] Carregando dados na memória...")
        all_rows = cursor_sql.fetchall()
        cursor_sql.close()
        conn_sql.close()
        
        print(f"[ETAPA 2] {len(all_rows)} registros carregados. Processando conversões em massa (vetorizado)...")
        
        # Processar conversões em massa usando DataFrame (vetorizado)
        include_legacy = self.should_include_legacy_id()
        
        # Criar DataFrame usando from_records que é mais adequado para dados de banco
        df = pd.DataFrame.from_records(all_rows, columns=['Id', 'Nome', 'Codigo', 'Ativo', 'DataInclusao', 'DataAlteracao'])
        
        # Aplicar transformações vetorizadas
        df['legacy_id'] = df['Id']
        df['nome'] = self.clean_string_vectorized(df['Nome'])
        df['codigo'] = df['Codigo'].astype(str).replace('nan', '')
        df['ativo'] = df['Ativo'].fillna(False).astype(bool)
        df['data_inclusao'] = df['DataInclusao']
        df['data_alteracao'] = df['DataAlteracao'].fillna(df['DataInclusao'])
        
        # Remover linhas com erros (nome None após limpeza)
        df = df[df['nome'].notna()]
        
        # Converter DataFrame diretamente para lista de tuplas (otimizado)
        if include_legacy:
            processed_tuples = list(zip(
                df['nome'].tolist(),
                df['codigo'].tolist(),
                df['ativo'].tolist(),
                df['data_inclusao'].tolist(),
                df['data_alteracao'].tolist(),
                df['legacy_id'].tolist()
            ))
            legacy_ids_list = df['legacy_id'].tolist()
        else:
            processed_tuples = list(zip(
                df['nome'].tolist(),
                df['codigo'].tolist(),
                df['ativo'].tolist(),
                df['data_inclusao'].tolist(),
                df['data_alteracao'].tolist()
            ))
            processed_data = df[['legacy_id', 'nome', 'codigo', 'ativo', 'data_inclusao', 'data_alteracao']].to_dict('records')
        
        print(f"[ETAPA 2] {len(processed_tuples)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Query de insert usando gen_random_uuid() - formato para execute_values
        if include_legacy:
            insert_query = f"""
            INSERT INTO {schema}.retail_chains (
                id, name, description, is_active, created_at, updated_at, legacy_id
            ) VALUES %s
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s)"
        else:
            insert_query = f"""
            INSERT INTO {schema}.retail_chains (
                id, name, description, is_active, created_at, updated_at
            ) VALUES %s
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s)"
        
        chunk_num = 0
        total_processed = 0
        all_legacy_ids_inserted = []  # Para buscar UUIDs depois
        
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
                        if include_legacy:
                            chunk_legacy_ids = [row[-1] for row in chunk]  # último elemento é legacy_id
                            all_legacy_ids_inserted.extend(chunk_legacy_ids)
                        
                        total_processed += len(chunk)
                        self.stats['retail_chains'] += len(chunk)
                        
                    except Exception as batch_error:
                        error_msg = f"Erro ao inserir batch de retail_chains: {batch_error}"
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
            if include_legacy and all_legacy_ids_inserted:
                print(f"[ETAPA 2] Buscando UUIDs gerados para {len(all_legacy_ids_inserted)} registros...")
                cursor_pg.execute(f"""
                    SELECT id, legacy_id 
                    FROM {schema}.retail_chains 
                    WHERE legacy_id = ANY(%s)
                """, (all_legacy_ids_inserted,))
                for uuid_row, leg_id in cursor_pg.fetchall():
                    self.retail_chain_id_map[leg_id] = uuid_row
                print(f"[ETAPA 2] {len(self.retail_chain_id_map)} UUIDs mapeados")
            
            # Se não tem legacy_id, buscar todos os UUIDs gerados após inserção completa
            if not include_legacy:
                print("[ETAPA 2] Carregando mapeamento de UUIDs gerados...")
                cursor_pg.execute(f"SELECT id, created_at FROM {schema}.retail_chains ORDER BY created_at")
                uuid_rows = cursor_pg.fetchall()
                for idx, (uuid_row, created_at) in enumerate(uuid_rows):
                    if idx < len(processed_data):
                        leg_id = processed_data[idx]['legacy_id']
                        self.retail_chain_id_map[leg_id] = uuid_row
            
            print(f"\n[ETAPA 2] CONCLUIDA! Total de retail_chains migrados: {self.stats['retail_chains']}")
            logger.info(f"ETAPA 2 concluida: {self.stats['retail_chains']} registros")
            
            # Validação e relatório de qualidade
            self.validate_step2_retail_chains()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 2: {e}")
            raise
        finally:
            try:
                if 'cursor_pg' in locals() and cursor_pg:
                    cursor_pg.close()
            except:
                pass
            try:
                if 'conn_pg' in locals() and conn_pg:
                    conn_pg.close()
            except:
                pass
    
    # ========================================================================
    # ETAPA 3: STORE_BRANDS
    # ========================================================================
    
    def validate_step3_store_brands(self):
        """Validação e relatório de qualidade - ETAPA 3"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 3: STORE_BRANDS")
        print("-"*80)
        
        try:
            # Carregar filtros do contracts ou buscar da ViewOrcamentosLojas
            filter_ids = self._get_filter_ids_for_validation()
            id_bandeira_list = filter_ids.get('IdBandeira', [])
            
            # Contar origem aplicando os mesmos filtros
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            if id_bandeira_list:
                # Aplicar filtro de IdBandeira do JSON
                placeholders = ','.join(['?' for _ in id_bandeira_list])
                cursor_sql.execute(f"SELECT COUNT(*) FROM Bandeira WHERE Id IN ({placeholders})", id_bandeira_list)
            else:
                # Buscar diretamente da ViewOrcamentosLojas (mesmo em full load)
                cursor_sql.execute("SELECT COUNT(DISTINCT IdBandeira) FROM ViewOrcamentosLojas WHERE IdBandeira IS NOT NULL")
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino aplicando os mesmos filtros
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            if id_bandeira_list:
                # Contar apenas os store_brands migrados nesta execução
                cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.store_brands WHERE legacy_id = ANY(%s)", (id_bandeira_list,))
            else:
                # Contar todos (fallback)
                cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.store_brands")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - Bandeira):")
            print(f"  Total de registros: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.store_brands):")
            print(f"  Total de registros: {destino_count}")
            
            diferenca = origem_count - destino_count
            
            if diferenca == 0:
                print(f"\nOK - Todos os registros foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 3: OK - Origem: {origem_count}, Destino: {destino_count}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} registros")
                print(f"  {abs(diferenca)} registros {'faltando' if diferenca > 0 else 'extras'} no destino")
                logger.warning(f"VALIDACAO ETAPA 3: Diferenca - Origem: {origem_count}, Destino: {destino_count}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 3: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step3_migrate_store_brands(self):
        """ETAPA 3: Migrar store_brands"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 3: MIGRANDO STORE_BRANDS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 3: Migrando store_brands")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        # Carregar filtros do contracts (se existir)
        # ⚠️ IMPORTANTE: Sempre buscar da ViewOrcamentosLojas aplicando filtros de data para garantir consistência
        filter_data = None
        if self.limit_rows == 0:
            filter_data = self.load_filter_json()
        
        id_bandeira_filter_list = []
        
        # ⚠️ IMPORTANTE: Sempre buscar da ViewOrcamentosLojas aplicando filtros de data
        # Isso garante que os IDs correspondem aos filtros de data aplicados
        if self.limit_rows > 0:
            print(f"[ETAPA 3] LIMIT {self.limit_rows} especificado. Buscando IdBandeira da ViewOrcamentosLojas com LIMIT...")
            logger.info(f"[ETAPA 3] LIMIT {self.limit_rows} especificado. Buscando da ViewOrcamentosLojas")
        else:
            print("[ETAPA 3] Buscando IdBandeira da ViewOrcamentosLojas com filtros de data aplicados...")
            logger.info("[ETAPA 3] Buscando IdBandeira da ViewOrcamentosLojas com filtros de data aplicados...")
        
        conn_sql_view = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql_view = conn_sql_view.cursor()
        
        # Construir query com filtros de data aplicados
        where_conditions = []
        query_params = []
        
        # Carregar filtros de data: prioridade CMD > JSON > None
        data_aviso_previo_min = self.data_aviso_previo_min
        data_inicio_operacao_max = self.data_inicio_operacao_max
        status_pedido_filter = self.status_pedido_filter
        
        # Se CMD não especificou filtros, usar do JSON se existir
        if data_aviso_previo_min is None and filter_data and 'filters_applied' in filter_data:
            filters = filter_data['filters_applied']
            data_aviso_previo_min = filters.get('data_aviso_previo_min')
        
        if data_inicio_operacao_max is None and filter_data and 'filters_applied' in filter_data:
            filters = filter_data['filters_applied']
            data_inicio_operacao_max = filters.get('data_inicio_operacao_max')
        
        if len(status_pedido_filter) == 0 and filter_data and 'filters_applied' in filter_data:
            filters = filter_data['filters_applied']
            json_status_pedido = filters.get('status_pedido')
            if json_status_pedido:
                status_pedido_filter = json_status_pedido
        
        # Filtro IdOrcamento
        if self.id_orcamento_filter:
            placeholders = ','.join(['?' for _ in self.id_orcamento_filter])
            where_conditions.append(f"v.IdOrcamento IN ({placeholders})")
            query_params.extend(self.id_orcamento_filter)
        
        # Filtro DataAvisoPrevio (data mínima)
        if data_aviso_previo_min:
            where_conditions.append("(CONVERT(DATE, v.DataAvisoPrevio) >= ? OR v.DataAvisoPrevio IS NULL)")
            query_params.append(data_aviso_previo_min)
        
        # Filtro DataInicioOperacao (data máxima)
        if data_inicio_operacao_max:
            where_conditions.append("CONVERT(DATE, v.DataInicioOperacao) <= ?")
            query_params.append(data_inicio_operacao_max)
        
        # Filtro StatusPedido
        if len(status_pedido_filter) > 0:
            placeholders = ','.join(['?' for _ in status_pedido_filter])
            where_conditions.append(f"v.StatusPedido IN ({placeholders})")
            query_params.extend(status_pedido_filter)
        
        # Construir query completa com INNER JOIN Orcamento e filtros
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions) + " AND v.IdBandeira IS NOT NULL"
        else:
            where_clause = "WHERE v.IdBandeira IS NOT NULL"
        
        # ⚠️ LÓGICA SIMPLIFICADA: Query direta sem subquery (conforme validação)
        if self.limit_rows > 0:
            query_id_bandeira = f"""
            SELECT DISTINCT TOP {self.limit_rows}
                v.IdBandeira
            FROM ViewOrcamentosLojas v
            INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
            {where_clause}
            """
        else:
            query_id_bandeira = f"""
            SELECT DISTINCT
                v.IdBandeira
            FROM ViewOrcamentosLojas v
            INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
            {where_clause}
            """
        
        # Log da query completa para debug
        logger.info(f"[ETAPA 3] Query completa para buscar IdBandeira da ViewOrcamentosLojas: {query_id_bandeira}")
        logger.info(f"[ETAPA 3] Parâmetros da query: {query_params}")
        
        # Executar query
        if query_params:
            cursor_sql_view.execute(query_id_bandeira, query_params)
        else:
            cursor_sql_view.execute(query_id_bandeira)
        
        # Carregar resultados (já são únicos devido ao DISTINCT na query externa)
        id_bandeira_filter_list = [row[0] for row in cursor_sql_view.fetchall()]
        cursor_sql_view.close()
        conn_sql_view.close()
        
        print(f"[ETAPA 3] Carregados {len(id_bandeira_filter_list)} IdBandeira únicos da ViewOrcamentosLojas")
        logger.info(f"[ETAPA 3] Carregados {len(id_bandeira_filter_list)} IdBandeira únicos da ViewOrcamentosLojas")
        
        # Atualizar JSON com IDs coletados da ViewOrcamentosLojas (apenas uma vez por execução)
        if not self.json_updated_this_run:
            self.save_filter_json_from_view(id_orcamento_list=self.id_orcamento_filter)
            self.json_updated_this_run = True
        
        # Limpar tabela
        # ⚠️ IMPORTANTE: Se clear_data=True, sempre usar TRUNCATE (mesmo com filtros)
        if self.clear_data:
            print("\n[ETAPA 3] Limpando tabela store_brands (TRUNCATE - flag --clear-data ativo)...")
            logger.info("[ETAPA 3] Flag --clear-data ativo: usando TRUNCATE mesmo com filtros")
            self.truncate_table('store_brands')
        elif not id_bandeira_filter_list:
            # Se não há filtros e SEM clear_data, usar TRUNCATE (caso raro)
            print("\n[ETAPA 3] Limpando tabela store_brands (sem filtros)...")
            self.truncate_table('store_brands')
        else:
            # Com filtros e SEM clear_data, usar DELETE
            print("\n[ETAPA 3] Limpando registros filtrados da tabela store_brands...")
            self.delete_table_with_filter('store_brands', legacy_ids=id_bandeira_filter_list)
        
        # Carregar mapeamentos necessários
        print("[ETAPA 3] Carregando mapeamentos de retail_chains e store_segments...")
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Carregar retail_chains
        include_legacy = self.should_include_legacy_id()
        if include_legacy:
            cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.retail_chains")
            retail_chain_map = {}
            for row in cursor_pg.fetchall():
                retail_chain_map[row[1]] = row[0]
        else:
            # Em PRD, usar o mapeamento já criado (retail_chain_id_map)
            retail_chain_map = self.retail_chain_id_map
        
        # Carregar store_segments (precisamos buscar pelo Estabelecimento.IdCanalEstabelecimento)
        if include_legacy:
            cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.store_segments")
            store_segment_map = {}
            for row in cursor_pg.fetchall():
                store_segment_map[row[1]] = row[0]
        else:
            # Em PRD, usar o mapeamento já criado (store_segment_id_map)
            store_segment_map = self.store_segment_id_map
        
        if include_legacy:
            cursor_pg.close()
            conn_pg.close()
        
        print(f"[ETAPA 3] {len(retail_chain_map)} retail_chains e {len(store_segment_map)} store_segments carregados")
        
        # Pré-carregar mapeamento de IdCanalEstabelecimento por Bandeira (evita queries durante processamento)
        print("[ETAPA 3] Pré-carregando mapeamento de IdCanalEstabelecimento por Bandeira...")
        conn_sql_temp = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql_temp = conn_sql_temp.cursor()
        
        # Aplicar filtro de IdBandeira na query de mapeamento também
        if id_bandeira_filter_list:
            placeholders = ','.join(['?' for _ in id_bandeira_filter_list])
            cursor_sql_temp.execute(f"""
                SELECT IdBandeira, IdCanalEstabelecimento
                FROM Estabelecimento
                WHERE IdBandeira IS NOT NULL AND IdCanalEstabelecimento IS NOT NULL
                  AND IdBandeira IN ({placeholders})
                GROUP BY IdBandeira, IdCanalEstabelecimento
                ORDER BY IdBandeira
            """, id_bandeira_filter_list)
        else:
            cursor_sql_temp.execute("""
                SELECT IdBandeira, IdCanalEstabelecimento
                FROM Estabelecimento
                WHERE IdBandeira IS NOT NULL AND IdCanalEstabelecimento IS NOT NULL
                GROUP BY IdBandeira, IdCanalEstabelecimento
                ORDER BY IdBandeira
            """)
        bandeira_canal_map = {}
        for temp_row in cursor_sql_temp.fetchall():
            bandeira_id, canal_id = temp_row
            if bandeira_id not in bandeira_canal_map:
                bandeira_canal_map[bandeira_id] = canal_id
        cursor_sql_temp.close()
        conn_sql_temp.close()
        print(f"[ETAPA 3] {len(bandeira_canal_map)} mapeamentos de Bandeira->Canal carregados")
        
        # Obter store_segment padrão uma vez
        default_store_segment_id = list(store_segment_map.values())[0] if store_segment_map else None
        
        # Buscar dados do SQL Server aplicando filtro de IdBandeira
        # ⚠️ CRÍTICO: Sempre filtrar pela ViewOrcamentosLojas, mesmo com --limit
        print("[ETAPA 3] Buscando dados do SQL Server...")
        query_params = []
        
        if id_bandeira_filter_list:
            # Aplicar filtro de IdBandeira do JSON ou ViewOrcamentosLojas
            placeholders = ','.join(['?' for _ in id_bandeira_filter_list])
            sql_query = f"""
            SELECT 
                b.Id,
                b.NomeFantasia,
                b.Codigo,
                b.IdRede,
                b.Ativo,
                b.DataInclusao,
                b.DataAlteracao
            FROM Bandeira b
            WHERE b.Id IN ({placeholders})
            ORDER BY b.Id
            """
            query_params.extend(id_bandeira_filter_list)
            
            # Aplicar limite se especificado
            if self.limit_rows > 0:
                sql_query = f"""
                SELECT 
                    b.Id,
                    b.NomeFantasia,
                    b.Codigo,
                    b.IdRede,
                    b.Ativo,
                    b.DataInclusao,
                    b.DataAlteracao
                FROM Bandeira b
                WHERE b.Id IN ({placeholders})
                ORDER BY b.Id
                OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY
                """
        else:
            # Sem filtros: buscar apenas da ViewOrcamentosLojas (mesmo em full load ou com --limit)
            if self.limit_rows > 0:
                # Com limite: primeiro buscar IdBandeira da ViewOrcamentosLojas, depois aplicar limite
                sql_query = f"""
                SELECT TOP {self.limit_rows}
                    b.Id,
                    b.NomeFantasia,
                    b.Codigo,
                    b.IdRede,
                    b.Ativo,
                    b.DataInclusao,
                    b.DataAlteracao
                FROM Bandeira b
                WHERE b.Id IN (
                    SELECT DISTINCT IdBandeira 
                    FROM ViewOrcamentosLojas
                    WHERE IdBandeira IS NOT NULL
                )
                ORDER BY b.Id
                """
            else:
                # Sem limite: buscar todos da ViewOrcamentosLojas
                sql_query = """
                SELECT 
                    b.Id,
                    b.NomeFantasia,
                    b.Codigo,
                    b.IdRede,
                    b.Ativo,
                    b.DataInclusao,
                    b.DataAlteracao
                FROM Bandeira b
                WHERE b.Id IN (
                    SELECT DISTINCT IdBandeira 
                    FROM ViewOrcamentosLojas
                    WHERE IdBandeira IS NOT NULL
                )
                ORDER BY b.Id
                """
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        if query_params:
            cursor_sql.execute(sql_query, query_params)
        else:
            cursor_sql.execute(sql_query)
        
        # Carregar TODOS os dados na memória de uma vez
        print("[ETAPA 3] Carregando dados na memória...")
        all_rows = cursor_sql.fetchall()
        cursor_sql.close()
        conn_sql.close()
        
        print(f"[ETAPA 3] {len(all_rows)} registros carregados. Processando conversões em massa (vetorizado)...")
        
        # Processar conversões em massa usando DataFrame (vetorizado)
        # Criar DataFrame usando from_records que é mais adequado para dados de banco
        df = pd.DataFrame.from_records(all_rows, columns=['Id', 'NomeFantasia', 'Codigo', 'IdRede', 'Ativo', 'DataInclusao', 'DataAlteracao'])
        
        # Aplicar transformações vetorizadas
        df['legacy_id'] = df['Id']
        df['nome'] = self.clean_string_vectorized(df['NomeFantasia'])
        
        # Lookup retail_chain_id (vetorizado)
        df['retail_chain_id'] = df['IdRede'].map(retail_chain_map)
        
        # Lookup store_segment_id (vetorizado)
        df['canal_id'] = df['Id'].map(bandeira_canal_map)
        df['store_segment_id'] = df['canal_id'].map(store_segment_map)
        
        # Preencher store_segment_id padrão onde está None
        if default_store_segment_id:
            mask_missing = df['store_segment_id'].isna()
            df.loc[mask_missing, 'store_segment_id'] = default_store_segment_id
            warnings_count = mask_missing.sum()
            if warnings_count > 0:
                logger.warning(f"Store segment não encontrado para {warnings_count} Bandeiras. Usando padrão: {default_store_segment_id}")
        
        # Outras transformações
        df['abras_code'] = df['Codigo'].astype(str).replace('nan', 'empty')
        df['ativo'] = df['Ativo'].fillna(False).astype(bool)
        df['data_inclusao'] = df['DataInclusao']
        df['data_alteracao'] = df['DataAlteracao'].fillna(df['DataInclusao'])
        
        # Converter retail_chain_id e store_segment_id para string
        df['retail_chain_id'] = df['retail_chain_id'].astype(str)
        df['store_segment_id'] = df['store_segment_id'].astype(str).replace('nan', None)
        
        # Remover linhas com erros (nome None ou retail_chain_id None)
        df = df[df['nome'].notna() & df['retail_chain_id'].notna()]
        
        # Converter DataFrame diretamente para lista de tuplas (otimizado)
        if include_legacy:
            processed_tuples = list(zip(
                df['nome'].tolist(),
                df['abras_code'].tolist(),
                df['retail_chain_id'].tolist(),
                df['store_segment_id'].tolist(),
                df['ativo'].tolist(),
                df['data_inclusao'].tolist(),
                df['data_alteracao'].tolist(),
                df['legacy_id'].tolist()
            ))
            legacy_ids_list = df['legacy_id'].tolist()
        else:
            processed_tuples = list(zip(
                df['nome'].tolist(),
                df['abras_code'].tolist(),
                df['retail_chain_id'].tolist(),
                df['store_segment_id'].tolist(),
                df['ativo'].tolist(),
                df['data_inclusao'].tolist(),
                df['data_alteracao'].tolist()
            ))
        
        print(f"[ETAPA 3] {len(processed_tuples)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL (recriar conexão se foi fechada anteriormente)
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Query de insert usando gen_random_uuid() - formato para execute_values
        # execute_values usa %s como placeholder único que será substituído pelos valores
        if include_legacy:
            insert_query = f"""
            INSERT INTO {schema}.store_brands (
                id, description, abras_code, retail_chain_id, store_segment_id,
                is_active, created_at, updated_at, legacy_id
            ) VALUES %s
            """
            # Template para execute_values com gen_random_uuid()
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s)"
        else:
            insert_query = f"""
            INSERT INTO {schema}.store_brands (
                id, description, abras_code, retail_chain_id, store_segment_id,
                is_active, created_at, updated_at
            ) VALUES %s
            """
            # Template para execute_values com gen_random_uuid()
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s)"
        
        chunk_num = 0
        total_processed = 0
        all_legacy_ids_inserted = []  # Para buscar UUIDs depois
        
        try:
            for i in range(0, len(processed_tuples), CHUNK_SIZE):
                chunk = processed_tuples[i:i + CHUNK_SIZE]
                chunk_num += 1
                
                if chunk:
                    try:
                        # Usar execute_values() para inserção otimizada em bulk (muito mais rápido que executemany)
                        execute_values(
                            cursor_pg,
                            insert_query,
                            chunk,
                            template=insert_template,
                            page_size=CHUNK_SIZE,
                            fetch=False
                        )
                        
                        # Coletar legacy_ids para lookup depois (se necessário)
                        if include_legacy:
                            chunk_legacy_ids = [row[-1] for row in chunk]  # último elemento é legacy_id
                            all_legacy_ids_inserted.extend(chunk_legacy_ids)
                        
                        total_processed += len(chunk)
                        self.stats['store_brands'] += len(chunk)
                        
                    except Exception as batch_error:
                        error_msg = f"Erro ao inserir batch de store_brands: {batch_error}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
                
                try:
                    conn_pg.commit()
                    print(f"[ETAPA 3] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                    logger.info(f"[ETAPA 3] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            # Buscar UUIDs gerados para mapeamento (uma única query após todas as inserções)
            if include_legacy and all_legacy_ids_inserted:
                print(f"[ETAPA 3] Buscando UUIDs gerados para {len(all_legacy_ids_inserted)} registros...")
                cursor_pg.execute(f"""
                    SELECT id, legacy_id 
                    FROM {schema}.store_brands 
                    WHERE legacy_id = ANY(%s)
                """, (all_legacy_ids_inserted,))
                for uuid_row, leg_id in cursor_pg.fetchall():
                    self.store_brand_id_map[leg_id] = uuid_row
                print(f"[ETAPA 3] {len(self.store_brand_id_map)} UUIDs mapeados")
            
            # Se não tem legacy_id, buscar UUIDs por ordem (não ideal, mas necessário)
            if not include_legacy:
                # Manter processed_data como dict para mapeamento por ordem
                processed_data = df[['legacy_id', 'nome', 'abras_code', 'retail_chain_id', 'store_segment_id', 'ativo', 'data_inclusao', 'data_alteracao']].to_dict('records')
                cursor_pg.execute(f"SELECT id, created_at FROM {schema}.store_brands ORDER BY created_at")
                uuid_rows = cursor_pg.fetchall()
                for idx, (uuid_row, created_at) in enumerate(uuid_rows):
                    if idx < len(processed_data):
                        leg_id = processed_data[idx]['legacy_id']
                        self.store_brand_id_map[leg_id] = uuid_row
            
            print(f"\n[ETAPA 3] CONCLUIDA! Total de store_brands migrados: {self.stats['store_brands']}")
            logger.info(f"ETAPA 3 concluida: {self.stats['store_brands']} registros")
            
            self.validate_step3_store_brands()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 3: {e}")
            raise
        finally:
            # Garantir que tudo está fechado
            try:
                if 'cursor_sql' in locals() and cursor_sql:
                    cursor_sql.close()
            except:
                pass
            try:
                if 'conn_sql' in locals() and conn_sql:
                    conn_sql.close()
            except:
                pass
            try:
                if 'cursor_pg' in locals() and cursor_pg:
                    cursor_pg.close()
            except:
                pass
            try:
                if 'conn_pg' in locals() and conn_pg:
                    conn_pg.close()
            except:
                pass
    
    # ========================================================================
    # ETAPA 4: STORES
    # ========================================================================
    
    def validate_step4_stores(self):
        """Validação e relatório de qualidade - ETAPA 4"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 4: STORES")
        print("-"*80)
        
        try:
            # Carregar filtros do contracts ou buscar da ViewOrcamentosLojas
            filter_ids = self._get_filter_ids_for_validation()
            id_estabelecimento_list = filter_ids.get('IdEstabelecimento', [])
            
            # Contar origem aplicando os mesmos filtros
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            if id_estabelecimento_list:
                # ⚠️ IMPORTANTE: SQL Server tem limite de ~2100 parâmetros
                # Dividir em chunks de 2000 para evitar erro "COUNT field incorrect"
                MAX_PARAMS_PER_QUERY = 2000
                origem_count = 0
                
                if len(id_estabelecimento_list) > MAX_PARAMS_PER_QUERY:
                    # Dividir em chunks
                    chunks = [id_estabelecimento_list[i:i + MAX_PARAMS_PER_QUERY] 
                             for i in range(0, len(id_estabelecimento_list), MAX_PARAMS_PER_QUERY)]
                    for chunk in chunks:
                        placeholders = ','.join(['?' for _ in chunk])
                        cursor_sql.execute(f"SELECT COUNT(*) FROM Estabelecimento WHERE Id IN ({placeholders})", chunk)
                        origem_count += cursor_sql.fetchone()[0]
                else:
                    # Menos de 2000 parâmetros, executar query normal
                    placeholders = ','.join(['?' for _ in id_estabelecimento_list])
                    cursor_sql.execute(f"SELECT COUNT(*) FROM Estabelecimento WHERE Id IN ({placeholders})", id_estabelecimento_list)
                    origem_count = cursor_sql.fetchone()[0]
            else:
                # Buscar diretamente da ViewOrcamentosLojas (mesmo em full load)
                cursor_sql.execute("SELECT COUNT(DISTINCT IdEstabelecimento) FROM ViewOrcamentosLojas WHERE IdEstabelecimento IS NOT NULL")
                origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino aplicando os mesmos filtros
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            if id_estabelecimento_list:
                # ⚠️ IMPORTANTE: PostgreSQL pode ter problemas com arrays muito grandes
                # Dividir em chunks de 2000 para evitar erro
                MAX_PARAMS_PER_QUERY = 2000
                destino_count = 0
                
                if len(id_estabelecimento_list) > MAX_PARAMS_PER_QUERY:
                    # Dividir em chunks
                    chunks = [id_estabelecimento_list[i:i + MAX_PARAMS_PER_QUERY] 
                             for i in range(0, len(id_estabelecimento_list), MAX_PARAMS_PER_QUERY)]
                    for chunk in chunks:
                        cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.stores WHERE legacy_id = ANY(%s)", (chunk,))
                        destino_count += cursor_pg.fetchone()[0]
                else:
                    # Menos de 2000 parâmetros, executar query normal
                    cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.stores WHERE legacy_id = ANY(%s)", (id_estabelecimento_list,))
                    destino_count = cursor_pg.fetchone()[0]
            else:
                # Contar todos (fallback)
                cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.stores")
                destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - Estabelecimento):")
            print(f"  Total de registros: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.stores):")
            print(f"  Total de registros: {destino_count}")
            
            diferenca = origem_count - destino_count
            
            if diferenca == 0:
                print(f"\nOK - Todos os registros foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 4: OK - Origem: {origem_count}, Destino: {destino_count}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} registros")
                print(f"  {abs(diferenca)} registros {'faltando' if diferenca > 0 else 'extras'} no destino")
                logger.warning(f"VALIDACAO ETAPA 4: Diferenca - Origem: {origem_count}, Destino: {destino_count}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 4: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step4_migrate_stores(self):
        """ETAPA 4: Migrar stores"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 4: MIGRANDO STORES")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 4: Migrando stores")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        # Carregar filtros do contracts (se existir)
        # ⚠️ IMPORTANTE: Sempre buscar da ViewOrcamentosLojas aplicando filtros de data para garantir consistência
        filter_data = None
        if self.limit_rows == 0:
            filter_data = self.load_filter_json()
        
        id_estabelecimento_filter_list = []
        
        # ⚠️ IMPORTANTE: Sempre buscar da ViewOrcamentosLojas aplicando filtros de data
        # Isso garante que os IDs correspondem aos filtros de data aplicados
        if self.limit_rows > 0:
            print(f"[ETAPA 4] LIMIT {self.limit_rows} especificado. Buscando IdEstabelecimento da ViewOrcamentosLojas com LIMIT...")
            logger.info(f"[ETAPA 4] LIMIT {self.limit_rows} especificado. Buscando da ViewOrcamentosLojas")
        else:
            print("[ETAPA 4] Buscando IdEstabelecimento da ViewOrcamentosLojas com filtros de data aplicados...")
            logger.info("[ETAPA 4] Buscando IdEstabelecimento da ViewOrcamentosLojas com filtros de data aplicados...")
        
        conn_sql_view = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql_view = conn_sql_view.cursor()
        
        # Construir query com filtros de data aplicados
        where_conditions = []
        query_params = []
        
        # Carregar filtros de data: prioridade CMD > JSON > None
        data_aviso_previo_min = self.data_aviso_previo_min
        data_inicio_operacao_max = self.data_inicio_operacao_max
        status_pedido_filter = self.status_pedido_filter
        
        # Se CMD não especificou filtros, usar do JSON se existir
        if data_aviso_previo_min is None and filter_data and 'filters_applied' in filter_data:
            filters = filter_data['filters_applied']
            data_aviso_previo_min = filters.get('data_aviso_previo_min')
        
        if data_inicio_operacao_max is None and filter_data and 'filters_applied' in filter_data:
            filters = filter_data['filters_applied']
            data_inicio_operacao_max = filters.get('data_inicio_operacao_max')
        
        if len(status_pedido_filter) == 0 and filter_data and 'filters_applied' in filter_data:
            filters = filter_data['filters_applied']
            json_status_pedido = filters.get('status_pedido')
            if json_status_pedido:
                status_pedido_filter = json_status_pedido
        
        # Filtro IdOrcamento
        if self.id_orcamento_filter:
            placeholders = ','.join(['?' for _ in self.id_orcamento_filter])
            where_conditions.append(f"v.IdOrcamento IN ({placeholders})")
            query_params.extend(self.id_orcamento_filter)
        
        # Filtro DataAvisoPrevio (data mínima)
        if data_aviso_previo_min:
            where_conditions.append("(CONVERT(DATE, v.DataAvisoPrevio) >= ? OR v.DataAvisoPrevio IS NULL)")
            query_params.append(data_aviso_previo_min)
        
        # Filtro DataInicioOperacao (data máxima)
        if data_inicio_operacao_max:
            where_conditions.append("CONVERT(DATE, v.DataInicioOperacao) <= ?")
            query_params.append(data_inicio_operacao_max)
        
        # Filtro StatusPedido
        if len(status_pedido_filter) > 0:
            placeholders = ','.join(['?' for _ in status_pedido_filter])
            where_conditions.append(f"v.StatusPedido IN ({placeholders})")
            query_params.extend(status_pedido_filter)
        
        # Construir query completa com INNER JOIN Orcamento e filtros
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions) + " AND v.IdEstabelecimento IS NOT NULL"
        else:
            where_clause = "WHERE v.IdEstabelecimento IS NOT NULL"
        
        # ⚠️ LÓGICA SIMPLIFICADA: Query direta sem subquery (conforme validação)
        if self.limit_rows > 0:
            query_id_estabelecimento = f"""
            SELECT DISTINCT TOP {self.limit_rows}
                v.IdEstabelecimento
            FROM ViewOrcamentosLojas v
            INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
            {where_clause}
            """
        else:
            query_id_estabelecimento = f"""
            SELECT DISTINCT
                v.IdEstabelecimento
            FROM ViewOrcamentosLojas v
            INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
            {where_clause}
            """
        
        # Log da query completa para debug
        logger.info(f"[ETAPA 4] Query completa para buscar IdEstabelecimento da ViewOrcamentosLojas: {query_id_estabelecimento}")
        logger.info(f"[ETAPA 4] Parâmetros da query: {query_params}")
        
        # Executar query
        if query_params:
            cursor_sql_view.execute(query_id_estabelecimento, query_params)
        else:
            cursor_sql_view.execute(query_id_estabelecimento)
        
        # Carregar resultados (já são únicos devido ao DISTINCT na query externa)
        id_estabelecimento_filter_list = [row[0] for row in cursor_sql_view.fetchall()]
        cursor_sql_view.close()
        conn_sql_view.close()
        
        print(f"[ETAPA 4] Carregados {len(id_estabelecimento_filter_list)} IdEstabelecimento únicos da ViewOrcamentosLojas")
        logger.info(f"[ETAPA 4] Carregados {len(id_estabelecimento_filter_list)} IdEstabelecimento únicos da ViewOrcamentosLojas")
        
        # Atualizar JSON com IDs coletados da ViewOrcamentosLojas (apenas uma vez por execução)
        if not self.json_updated_this_run:
            self.save_filter_json_from_view(id_orcamento_list=self.id_orcamento_filter)
            self.json_updated_this_run = True
        
        # Limpar tabela
        # ⚠️ IMPORTANTE: Se clear_data=True, sempre usar TRUNCATE (mesmo com filtros)
        if self.clear_data:
            print("\n[ETAPA 4] Limpando tabela stores (TRUNCATE - flag --clear-data ativo)...")
            logger.info("[ETAPA 4] Flag --clear-data ativo: usando TRUNCATE mesmo com filtros")
            self.truncate_table('stores')
        elif not id_estabelecimento_filter_list:
            # Se não há filtros e SEM clear_data, usar TRUNCATE (caso raro)
            print("\n[ETAPA 4] Limpando tabela stores (sem filtros)...")
            self.truncate_table('stores')
        else:
            # Com filtros e SEM clear_data, usar DELETE
            print("\n[ETAPA 4] Limpando registros filtrados da tabela stores...")
            self.delete_table_with_filter('stores', legacy_ids=id_estabelecimento_filter_list)
        
        # Carregar mapeamento de store_brands
        print("[ETAPA 4] Carregando mapeamento de store_brands...")
        include_legacy = self.should_include_legacy_id()
        if include_legacy:
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.store_brands WHERE legacy_id IS NOT NULL")
            store_brand_map = {}
            for row in cursor_pg.fetchall():
                if row[1] is not None:  # Garantir que legacy_id não é NULL
                    store_brand_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
        else:
            # Em PRD, usar o mapeamento já criado (store_brand_id_map)
            # Mas também verificar quais realmente existem no banco
            store_brand_map = {}
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            # Buscar todos os store_brands do banco para verificar quais existem
            cursor_pg.execute(f"SELECT id FROM {schema}.store_brands")
            existing_store_brand_ids = {str(row[0]) for row in cursor_pg.fetchall()}
            cursor_pg.close()
            conn_pg.close()
            # Filtrar apenas os que realmente existem no banco
            for legado_id, uuid_id in self.store_brand_id_map.items():
                if str(uuid_id) in existing_store_brand_ids:
                    store_brand_map[legado_id] = uuid_id
        
        print(f"[ETAPA 4] {len(store_brand_map)} store_brands carregados")
        
        # Obter ou criar store_brand padrão
        default_store_brand_id = self._get_or_create_default_store_brand(schema, include_legacy)
        
        if not store_brand_map:
            print(f"AVISO: Nenhum store_brand encontrado no mapeamento. Usando store_brand padrão 'Teste store_brands'.")
            logger.warning("Nenhum store_brand encontrado no mapeamento. Usando padrão.")
        
        # Buscar dados do SQL Server aplicando filtro de IdEstabelecimento
        print("[ETAPA 4] Buscando dados do SQL Server...")
        query_params = []
        
        if id_estabelecimento_filter_list:
            # ⚠️ IMPORTANTE: SQL Server tem limite de ~2100 parâmetros
            # Dividir em chunks de 2000 para evitar erro "COUNT field incorrect"
            MAX_PARAMS_PER_QUERY = 2000
            all_rows = []
            
            # Dividir lista em chunks
            chunks = [id_estabelecimento_filter_list[i:i + MAX_PARAMS_PER_QUERY] 
                     for i in range(0, len(id_estabelecimento_filter_list), MAX_PARAMS_PER_QUERY)]
            
            print(f"[ETAPA 4] Processando {len(id_estabelecimento_filter_list)} IdEstabelecimento em {len(chunks)} chunks de até {MAX_PARAMS_PER_QUERY} registros cada...")
            logger.info(f"[ETAPA 4] Processando {len(id_estabelecimento_filter_list)} IdEstabelecimento em {len(chunks)} chunks")
            
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            for chunk_idx, chunk in enumerate(chunks, 1):
                placeholders = ','.join(['?' for _ in chunk])
                sql_query = f"""
                SELECT 
                    Id,
                    NomeFantasia,
                    IdBandeira,
                    Ativo,
                    DataInclusao,
                    DataAlteracao,
                    codigo
                FROM Estabelecimento
                WHERE Id IN ({placeholders})
                ORDER BY Id
                """
                cursor_sql.execute(sql_query, chunk)
                chunk_rows = cursor_sql.fetchall()
                all_rows.extend(chunk_rows)
                print(f"[ETAPA 4] Chunk {chunk_idx}/{len(chunks)}: {len(chunk_rows)} registros carregados")
                logger.info(f"[ETAPA 4] Chunk {chunk_idx}/{len(chunks)}: {len(chunk_rows)} registros carregados")
            
            cursor_sql.close()
            conn_sql.close()
            
            print(f"[ETAPA 4] Total: {len(all_rows)} registros carregados de {len(id_estabelecimento_filter_list)} IdEstabelecimento")
            logger.info(f"[ETAPA 4] Total: {len(all_rows)} registros carregados")
        elif self.limit_rows > 0:
            if store_brand_map:
                # Buscar apenas estabelecimentos cujas bandeiras foram migradas
                bandeiras_ids = tuple(store_brand_map.keys())
                sql_query = f"""
                SELECT 
                    e.Id,
                    e.NomeFantasia,
                    e.IdBandeira,
                    e.Ativo,
                    e.DataInclusao,
                    e.DataAlteracao,
                    e.codigo
                FROM Estabelecimento e
                WHERE e.IdBandeira IN {bandeiras_ids}
                ORDER BY e.Id
                OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY
                """
            else:
                # Se não há store_brands no mapeamento, buscar todos (usará padrão)
                sql_query = f"""
                SELECT TOP {self.limit_rows}
                    Id,
                    NomeFantasia,
                    IdBandeira,
                    Ativo,
                    DataInclusao,
                    DataAlteracao,
                    codigo
                FROM Estabelecimento
                ORDER BY Id
                """
            
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            cursor_sql.execute(sql_query)
            all_rows = cursor_sql.fetchall()
            cursor_sql.close()
            conn_sql.close()
            print(f"[ETAPA 4] {len(all_rows)} registros carregados. Processando conversões em massa (vetorizado)...")
        else:
            # Sem filtros e sem limite: buscar apenas da ViewOrcamentosLojas (mesmo em full load)
            sql_query = """
            SELECT 
                e.Id,
                e.NomeFantasia,
                e.IdBandeira,
                e.Ativo,
                e.DataInclusao,
                e.DataAlteracao,
                e.codigo
            FROM Estabelecimento e
            WHERE e.Id IN (
                SELECT DISTINCT IdEstabelecimento 
                FROM ViewOrcamentosLojas
                WHERE IdEstabelecimento IS NOT NULL
            )
            ORDER BY e.Id
            """
            
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            cursor_sql.execute(sql_query)
            all_rows = cursor_sql.fetchall()
            cursor_sql.close()
            conn_sql.close()
            print(f"[ETAPA 4] {len(all_rows)} registros carregados. Processando conversões em massa (vetorizado)...")
        
        # Processar conversões em massa usando DataFrame (vetorizado)
        # Criar DataFrame usando from_records que é mais adequado para dados de banco
        df = pd.DataFrame.from_records(all_rows, columns=['Id', 'NomeFantasia', 'IdBandeira', 'Ativo', 'DataInclusao', 'DataAlteracao', 'codigo'])
        
        # Aplicar transformações vetorizadas
        df['legacy_id'] = df['Id']
        df['nome'] = self.clean_string_vectorized(df['NomeFantasia'], max_length=255)
        
        # Lookup store_brand_id (vetorizado)
        df['store_brand_id'] = df['IdBandeira'].map(store_brand_map)
        df['code'] = df['codigo'].fillna('')  # Preencher None/NaN com string vazia
        
        # Preencher store_brand_id padrão onde está None
        mask_missing = df['store_brand_id'].isna()
        df.loc[mask_missing, 'store_brand_id'] = default_store_brand_id
        warnings_count = mask_missing.sum()
        if warnings_count > 0:
            logger.warning(f"Store brand não encontrado para {warnings_count} IdBandeiras. Usando store_brand padrão 'Teste store_brands'")
        
        # Outras transformações
        df['ativo'] = df['Ativo'].fillna(False).astype(bool)
        df['data_inclusao'] = df['DataInclusao']
        df['data_alteracao'] = df['DataAlteracao'].fillna(df['DataInclusao'])
        
        # Converter store_brand_id para string
        df['store_brand_id'] = df['store_brand_id'].astype(str)
        
        # Remover linhas com erros (nome None)
        df = df[df['nome'].notna()]
        
        # Converter DataFrame diretamente para lista de tuplas (otimizado)
        if include_legacy:
            processed_tuples = list(zip(
                df['nome'].tolist(),
                df['store_brand_id'].tolist(),
                df['ativo'].tolist(),
                df['data_inclusao'].tolist(),
                df['data_alteracao'].tolist(),
                df['legacy_id'].tolist(),
                df['code'].tolist()
            ))
            legacy_ids_list = df['legacy_id'].tolist()
        else:
            processed_tuples = list(zip(
                df['nome'].tolist(),
                df['store_brand_id'].tolist(),
                df['ativo'].tolist(),
                df['data_inclusao'].tolist(),
                df['data_alteracao'].tolist(),
                df['code'].tolist()
            ))
            # Manter processed_data como dict para mapeamento por ordem quando não há legacy_id
            processed_data = df[['legacy_id', 'nome', 'store_brand_id', 'ativo', 'data_inclusao', 'data_alteracao', 'code']].to_dict('records')
        
        print(f"[ETAPA 4] {len(processed_tuples)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Query de insert usando gen_random_uuid() - formato para execute_values
        if include_legacy:
            insert_query = f"""
            INSERT INTO {schema}.stores (
                id, name, store_brand_id, is_active, created_at, updated_at, legacy_id, code
            ) VALUES %s
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s)"
        else:
            insert_query = f"""
            INSERT INTO {schema}.stores (
                id, name, store_brand_id, is_active, created_at, updated_at, code
            ) VALUES %s
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s)"
        
        chunk_num = 0
        total_processed = 0
        all_legacy_ids_inserted = []  # Para buscar UUIDs depois
        
        try:
            for i in range(0, len(processed_tuples), CHUNK_SIZE):
                chunk = processed_tuples[i:i + CHUNK_SIZE]
                chunk_num += 1
                print(f"[ETAPA 4] Processando chunk {chunk_num} ({len(chunk)} registros)...")
                
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
                        if include_legacy:
                            # Ordem da tupla: (nome, store_brand_id, ativo, data_inclusao, data_alteracao, legacy_id, code)
                            # legacy_id está na posição 5 (penúltimo, ou -2)
                            chunk_legacy_ids = [row[-2] for row in chunk]  # penúltimo elemento é legacy_id
                            all_legacy_ids_inserted.extend(chunk_legacy_ids)
                        
                        total_processed += len(chunk)
                        self.stats['stores'] += len(chunk)
                    except Exception as batch_error:
                        error_msg = f"Erro ao inserir batch de stores: {batch_error}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
                
                try:
                    conn_pg.commit()
                    print(f"[ETAPA 4] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                    logger.info(f"[ETAPA 4] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            # Buscar UUIDs gerados para mapeamento (uma única query após todas as inserções)
            if include_legacy and all_legacy_ids_inserted:
                print(f"[ETAPA 4] Buscando UUIDs gerados para {len(all_legacy_ids_inserted)} registros...")
                cursor_pg.execute(f"""
                    SELECT id, legacy_id 
                    FROM {schema}.stores 
                    WHERE legacy_id = ANY(%s)
                """, (all_legacy_ids_inserted,))
                for uuid_row, leg_id in cursor_pg.fetchall():
                    self.store_id_map[leg_id] = uuid_row
                print(f"[ETAPA 4] {len(self.store_id_map)} UUIDs mapeados")
            
            # Se não tem legacy_id, buscar UUIDs por ordem
            if not include_legacy:
                cursor_pg.execute(f"SELECT id, created_at FROM {schema}.stores ORDER BY created_at")
                uuid_rows = cursor_pg.fetchall()
                for idx, (uuid_row, created_at) in enumerate(uuid_rows):
                    if idx < len(processed_data):
                        leg_id = processed_data[idx]['legacy_id']
                        self.store_id_map[leg_id] = uuid_row
            
            print(f"\n[ETAPA 4] CONCLUIDA! Total de stores migrados: {self.stats['stores']}")
            logger.info(f"ETAPA 4 concluida: {self.stats['stores']} registros")
            
            self.validate_step4_stores()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 4: {e}")
            raise
        finally:
            # Garantir que tudo está fechado
            try:
                if 'cursor_sql' in locals() and cursor_sql:
                    cursor_sql.close()
            except:
                pass
            try:
                if 'conn_sql' in locals() and conn_sql:
                    conn_sql.close()
            except:
                pass
            try:
                if 'cursor_pg' in locals() and cursor_pg:
                    cursor_pg.close()
            except:
                pass
            try:
                if 'conn_pg' in locals() and conn_pg:
                    conn_pg.close()
            except:
                pass
    
    # ========================================================================
    # ETAPA 5: STORE_CNPJS
    # ========================================================================
    
    def validate_step5_store_cnpjs(self):
        """Validação e relatório de qualidade - ETAPA 5"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 5: STORE_CNPJS")
        print("-"*80)
        
        try:
            # Contar origem (aplicando limite se especificado)
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            # Se houver mapeamento de stores, usar apenas os estabelecimentos migrados
            if self.store_id_map:
                estabelecimentos_ids = list(self.store_id_map.keys())
                if estabelecimentos_ids:
                    placeholders = ','.join(['?' for _ in estabelecimentos_ids])
                    cursor_sql.execute(f"""
                        SELECT COUNT(*) 
                        FROM Estabelecimento
                        WHERE Id IN ({placeholders})
                          AND Cnpj IS NOT NULL AND LTRIM(RTRIM(Cnpj)) != ''
                    """, estabelecimentos_ids)
                    origem_count = cursor_sql.fetchone()[0]
                else:
                    origem_count = 0
            elif self.limit_rows > 0:
                cursor_sql.execute(f"""
                    SELECT COUNT(*) 
                    FROM (SELECT TOP {self.limit_rows} Id FROM Estabelecimento WHERE Cnpj IS NOT NULL AND LTRIM(RTRIM(Cnpj)) != '' ORDER BY Id) AS limited
                """)
                origem_count = cursor_sql.fetchone()[0]
            else:
                cursor_sql.execute("SELECT COUNT(*) FROM Estabelecimento WHERE Cnpj IS NOT NULL AND LTRIM(RTRIM(Cnpj)) != ''")
                origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.store_cnpjs")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - Estabelecimento com CNPJ):")
            print(f"  Total de registros: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.store_cnpjs):")
            print(f"  Total de registros: {destino_count}")
            
            diferenca = origem_count - destino_count
            
            if diferenca == 0:
                print(f"\nOK - Todos os registros foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 5: OK - Origem: {origem_count}, Destino: {destino_count}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} registros")
                print(f"  {abs(diferenca)} registros {'faltando' if diferenca > 0 else 'extras'} no destino")
                logger.warning(f"VALIDACAO ETAPA 5: Diferenca - Origem: {origem_count}, Destino: {destino_count}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 5: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step5_migrate_store_cnpjs(self):
        """ETAPA 5: Migrar store_cnpjs"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 5: MIGRANDO STORE_CNPJS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 5: Migrando store_cnpjs")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        # Limpar tabela
        # ⚠️ IMPORTANTE: store_cnpjs sempre usa TRUNCATE (não há filtros aplicados diretamente)
        # Mas quando clear_data=True, garante que está limpando tudo antes de migrar
        if self.clear_data:
            print("\n[ETAPA 5] Limpando tabela store_cnpjs (TRUNCATE - flag --clear-data ativo)...")
            logger.info("[ETAPA 5] Flag --clear-data ativo: usando TRUNCATE")
        else:
            print("\n[ETAPA 5] Limpando tabela store_cnpjs...")
        self.truncate_table('store_cnpjs')
        
        # Buscar dados do SQL Server
        # Se houver limite, filtrar apenas estabelecimentos que foram migrados
        print("[ETAPA 5] Buscando dados do SQL Server...")
        query_params = []
        
        if self.store_id_map:
            # Buscar apenas estabelecimentos que foram migrados
            estabelecimentos_ids = list(self.store_id_map.keys())
            if estabelecimentos_ids:
                # ⚠️ IMPORTANTE: SQL Server tem limite de ~2100 parâmetros
                # Dividir em chunks de 2000 para evitar erro "COUNT field incorrect"
                MAX_PARAMS_PER_QUERY = 2000
                all_rows = []
                
                if len(estabelecimentos_ids) > MAX_PARAMS_PER_QUERY:
                    # Dividir em chunks
                    chunks = [estabelecimentos_ids[i:i + MAX_PARAMS_PER_QUERY] 
                             for i in range(0, len(estabelecimentos_ids), MAX_PARAMS_PER_QUERY)]
                    
                    print(f"[ETAPA 5] Processando {len(estabelecimentos_ids)} IdEstabelecimento em {len(chunks)} chunks de até {MAX_PARAMS_PER_QUERY} registros cada...")
                    logger.info(f"[ETAPA 5] Processando {len(estabelecimentos_ids)} IdEstabelecimento em {len(chunks)} chunks")
                    
                    conn_sql = DatabaseConnection.get_sql_server_prd_connection()
                    cursor_sql = conn_sql.cursor()
                    
                    for chunk_idx, chunk in enumerate(chunks, 1):
                        placeholders = ','.join(['?' for _ in chunk])
                        chunk_query = f"""
                        SELECT 
                            Id,
                            Cnpj,
                            Ativo,
                            DataInclusao,
                            DataAlteracao
                        FROM Estabelecimento
                        WHERE Id IN ({placeholders})
                          AND Cnpj IS NOT NULL AND LTRIM(RTRIM(Cnpj)) != ''
                        ORDER BY Id
                        """
                        cursor_sql.execute(chunk_query, chunk)
                        chunk_rows = cursor_sql.fetchall()
                        all_rows.extend(chunk_rows)
                        print(f"[ETAPA 5] Chunk {chunk_idx}/{len(chunks)}: {len(chunk_rows)} registros carregados")
                        logger.info(f"[ETAPA 5] Chunk {chunk_idx}/{len(chunks)}: {len(chunk_rows)} registros carregados")
                    
                    cursor_sql.close()
                    conn_sql.close()
                    
                    print(f"[ETAPA 5] Total: {len(all_rows)} registros carregados de {len(estabelecimentos_ids)} IdEstabelecimento")
                    logger.info(f"[ETAPA 5] Total: {len(all_rows)} registros carregados")
                else:
                    # Menos de 2000 parâmetros, executar query normal
                    placeholders = ','.join(['?' for _ in estabelecimentos_ids])
                    sql_query = f"""
                    SELECT 
                        Id,
                        Cnpj,
                        Ativo,
                        DataInclusao,
                        DataAlteracao
                    FROM Estabelecimento
                    WHERE Id IN ({placeholders})
                      AND Cnpj IS NOT NULL AND LTRIM(RTRIM(Cnpj)) != ''
                    ORDER BY Id
                    """
                    query_params.extend(estabelecimentos_ids)
                    
                    # Aplicar limite se especificado
                    if self.limit_rows > 0:
                        sql_query = f"""
                        SELECT 
                            Id,
                            Cnpj,
                            Ativo,
                            DataInclusao,
                            DataAlteracao
                        FROM Estabelecimento
                        WHERE Id IN ({placeholders})
                          AND Cnpj IS NOT NULL AND LTRIM(RTRIM(Cnpj)) != ''
                        ORDER BY Id
                        OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY
                        """
                    
                    conn_sql = DatabaseConnection.get_sql_server_prd_connection()
                    cursor_sql = conn_sql.cursor()
                    cursor_sql.execute(sql_query, query_params)
                    all_rows = cursor_sql.fetchall()
                    cursor_sql.close()
                    conn_sql.close()
                    print(f"[ETAPA 5] {len(all_rows)} registros carregados. Processando conversões...")
            else:
                # Se não há estabelecimentos migrados, retornar query vazia
                all_rows = []
        else:
            sql_query = """
            SELECT 
                Id,
                Cnpj,
                Ativo,
                DataInclusao,
                DataAlteracao
            FROM Estabelecimento
            WHERE Cnpj IS NOT NULL AND LTRIM(RTRIM(Cnpj)) != ''
            ORDER BY Id
            """
            # Adicionar LIMIT se especificado e não houver mapeamento
            if self.limit_rows > 0:
                sql_query = sql_query.replace("ORDER BY Id", f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
            
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            cursor_sql.execute(sql_query)
            all_rows = cursor_sql.fetchall()
            cursor_sql.close()
            conn_sql.close()
            print(f"[ETAPA 5] {len(all_rows)} registros carregados. Processando conversões...")
        
        # Processar conversões em massa usando DataFrame (vetorizado)
        # Criar DataFrame usando from_records que é mais adequado para dados de banco
        df = pd.DataFrame.from_records(all_rows, columns=['Id', 'Cnpj', 'Ativo', 'DataInclusao', 'DataAlteracao'])
        
        # Lookup store_id (vetorizado)
        df['store_id'] = df['Id'].map(self.store_id_map)
        
        # Remover linhas onde store_id não foi encontrado
        df = df[df['store_id'].notna()]
        
        # Limpar CNPJ (vetorizado)
        df['cnpj'] = self.clean_cnpj_vectorized(df['Cnpj'])
        
        # Remover linhas onde CNPJ não é válido
        df = df[df['cnpj'].notna()]
        
        # Outras transformações
        df['ativo'] = df['Ativo'].fillna(False).astype(bool)
        df['data_inclusao'] = df['DataInclusao']
        df['data_alteracao'] = df['DataAlteracao'].fillna(df['DataInclusao'])
        
        # Converter store_id para string
        df['store_id'] = df['store_id'].astype(str)
        
        # Converter DataFrame diretamente para lista de tuplas (otimizado)
        processed_tuples = list(zip(
            df['cnpj'].tolist(),
            [True] * len(df),  # is_main (sempre true para CNPJ principal)
            df['store_id'].tolist(),
            df['ativo'].tolist(),
            df['data_inclusao'].tolist(),
            df['data_alteracao'].tolist()
        ))
        
        print(f"[ETAPA 5] {len(processed_tuples)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Query de insert usando gen_random_uuid() - formato para execute_values
        insert_query = f"""
        INSERT INTO {schema}.store_cnpjs (
            id, cnpj, is_main, store_id, is_active, created_at, updated_at
        ) VALUES %s
        """
        insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s)"
        
        chunk_num = 0
        total_processed = 0
        
        try:
            for i in range(0, len(processed_tuples), CHUNK_SIZE):
                chunk = processed_tuples[i:i + CHUNK_SIZE]
                chunk_num += 1
                print(f"[ETAPA 5] Processando chunk {chunk_num} ({len(chunk)} registros)...")
                
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
                        self.stats['store_cnpjs'] += len(chunk)
                    except Exception as batch_error:
                        error_msg = f"Erro ao inserir batch de store_cnpjs: {batch_error}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
                
                try:
                    conn_pg.commit()
                    print(f"[ETAPA 5] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                    logger.info(f"[ETAPA 5] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            print(f"\n[ETAPA 5] CONCLUIDA! Total de store_cnpjs migrados: {self.stats['store_cnpjs']}")
            logger.info(f"ETAPA 5 concluida: {self.stats['store_cnpjs']} registros")
            
            self.validate_step5_store_cnpjs()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 5: {e}")
            raise
        finally:
            try:
                if 'cursor_pg' in locals() and cursor_pg:
                    cursor_pg.close()
            except:
                pass
            try:
                if 'conn_pg' in locals() and conn_pg:
                    conn_pg.close()
            except:
                pass
    
    # ========================================================================
    # ETAPA 6: ADDRESSES (polimórfica para stores)
    # ========================================================================
    
    def validate_step6_addresses(self):
        """Validação e relatório de qualidade - ETAPA 6"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 6: ADDRESSES (STORES)")
        print("-"*80)
        
        try:
            # Contar origem - endereços de estabelecimentos (aplicando mesmo filtro e limite da migração)
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            # Se houver mapeamento de stores, usar apenas os estabelecimentos migrados
            if self.store_id_map:
                estabelecimentos_ids = list(self.store_id_map.keys())
                if estabelecimentos_ids:
                    # ⚠️ IMPORTANTE: SQL Server tem limite de ~2100 parâmetros
                    # Dividir em chunks de 2000 para evitar erro "COUNT field incorrect"
                    MAX_PARAMS_PER_QUERY = 2000
                    origem_count = 0
                    
                    if len(estabelecimentos_ids) > MAX_PARAMS_PER_QUERY:
                        # Dividir em chunks
                        chunks = [estabelecimentos_ids[i:i + MAX_PARAMS_PER_QUERY] 
                                 for i in range(0, len(estabelecimentos_ids), MAX_PARAMS_PER_QUERY)]
                        for chunk in chunks:
                            placeholders = ','.join(['?' for _ in chunk])
                            query_main = f"""
                                SELECT COUNT(*) 
                                FROM Estabelecimento
                                WHERE Id IN ({placeholders})
                                  AND Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != ''
                            """
                            cursor_sql.execute(query_main, chunk)
                            origem_count += cursor_sql.fetchone()[0]
                    else:
                        # Menos de 2000 parâmetros, executar query normal
                        placeholders = ','.join(['?' for _ in estabelecimentos_ids])
                        query_main = f"""
                            SELECT COUNT(*) 
                            FROM Estabelecimento
                            WHERE Id IN ({placeholders})
                              AND Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != ''
                        """
                        cursor_sql.execute(query_main, estabelecimentos_ids)
                        origem_count = cursor_sql.fetchone()[0]
                else:
                    origem_count = 0
            elif self.limit_rows > 0:
                # Se houver limite mas não houver mapeamento, usar TOP para aplicar o limite
                query_main = f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT TOP {self.limit_rows} Id, Endereco
                        FROM Estabelecimento
                        WHERE Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != ''
                        ORDER BY Id
                    ) AS limited
                """
                cursor_sql.execute(query_main)
                origem_count = cursor_sql.fetchone()[0]
            else:
                # Sem limite, não precisa de subquery nem ORDER BY
                query_main = """
                    SELECT COUNT(*) 
                    FROM Estabelecimento
                    WHERE Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != ''
                """
                cursor_sql.execute(query_main)
                origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.addresses WHERE addressable_type IN ('Store','Stores')")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - Estabelecimento):")
            print(f"  Total de enderecos: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.addresses):")
            print(f"  Total inserido (addressable_type='Store'): {destino_count}")
            
            diferenca = origem_count - destino_count
            
            if diferenca == 0:
                print(f"\nOK - Todos os enderecos foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 6: OK - Origem: {origem_count}, Destino: {destino_count}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} enderecos")
                print(f"  {abs(diferenca)} enderecos {'faltando' if diferenca > 0 else 'extras'} no destino")
                logger.warning(f"VALIDACAO ETAPA 6: Diferenca - Origem: {origem_count}, Destino: {destino_count}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 6: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step6_migrate_addresses(self):
        """ETAPA 6: Migrar addresses para stores"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 6: MIGRANDO ADDRESSES (STORES)")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 6: Migrando addresses para stores")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        # Delete apenas addresses de stores (tabela polimórfica)
        # ⚠️ IMPORTANTE: Se clear_data=True, deleta TODOS os addresses de stores (não apenas filtrados)
        if self.clear_data:
            print("\n[ETAPA 6] Limpando addresses de stores (flag --clear-data ativo - todos os registros)...")
            logger.info("[ETAPA 6] Flag --clear-data ativo: deletando TODOS os addresses de stores")
        else:
            print("\n[ETAPA 6] Limpando addresses de stores...")
        self.delete_polymorphic_table('addresses', 'Store', 'addressable_type')
        
        print("[ETAPA 6] Carregando lookup Municipio (origem SQL Server)...")
        conn_mun = DatabaseConnection.get_sql_server_prd_connection()
        cur_mun = conn_mun.cursor()
        try:
            municipio_lookup = load_municipio_lookup(cur_mun)
        finally:
            cur_mun.close()
            conn_mun.close()
        
        # Buscar dados do SQL Server
        # Se houver limite, filtrar apenas estabelecimentos que foram migrados
        print("[ETAPA 6] Buscando dados do SQL Server...")
        query_params = []
        
        if self.store_id_map:
            # Buscar apenas estabelecimentos que foram migrados
            estabelecimentos_ids = list(self.store_id_map.keys())
            if estabelecimentos_ids:
                # ⚠️ IMPORTANTE: SQL Server tem limite de ~2100 parâmetros
                # Dividir em chunks de 2000 para evitar erro "COUNT field incorrect"
                MAX_PARAMS_PER_QUERY = 2000
                all_rows = []
                
                if len(estabelecimentos_ids) > MAX_PARAMS_PER_QUERY:
                    # Dividir em chunks
                    chunks = [estabelecimentos_ids[i:i + MAX_PARAMS_PER_QUERY] 
                             for i in range(0, len(estabelecimentos_ids), MAX_PARAMS_PER_QUERY)]
                    
                    print(f"[ETAPA 6] Processando {len(estabelecimentos_ids)} IdEstabelecimento em {len(chunks)} chunks de até {MAX_PARAMS_PER_QUERY} registros cada...")
                    logger.info(f"[ETAPA 6] Processando {len(estabelecimentos_ids)} IdEstabelecimento em {len(chunks)} chunks")
                    
                    conn_sql = DatabaseConnection.get_sql_server_prd_connection()
                    cursor_sql = conn_sql.cursor()
                    
                    for chunk_idx, chunk in enumerate(chunks, 1):
                        placeholders = ','.join(['?' for _ in chunk])
                        chunk_query = f"""
                        SELECT 
                            Id,
                            Endereco,
                            Numero,
                            Complemento,
                            Bairro,
                            CEP,
                            Cidade,
                            UF,
                            Latitude,
                            Longitude,
                            DataInclusao,
                            DataAlteracao
                        FROM Estabelecimento
                        WHERE Id IN ({placeholders})
                          AND Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != ''
                        ORDER BY Id
                        """
                        cursor_sql.execute(chunk_query, chunk)
                        chunk_rows = cursor_sql.fetchall()
                        all_rows.extend(chunk_rows)
                        print(f"[ETAPA 6] Chunk {chunk_idx}/{len(chunks)}: {len(chunk_rows)} registros carregados")
                        logger.info(f"[ETAPA 6] Chunk {chunk_idx}/{len(chunks)}: {len(chunk_rows)} registros carregados")
                    
                    cursor_sql.close()
                    conn_sql.close()
                    
                    print(f"[ETAPA 6] Total: {len(all_rows)} registros carregados de {len(estabelecimentos_ids)} IdEstabelecimento")
                    logger.info(f"[ETAPA 6] Total: {len(all_rows)} registros carregados")
                else:
                    # Menos de 2000 parâmetros, executar query normal
                    placeholders = ','.join(['?' for _ in estabelecimentos_ids])
                    sql_query = f"""
                    SELECT 
                        Id,
                        Endereco,
                        Numero,
                        Complemento,
                        Bairro,
                        CEP,
                        Cidade,
                        UF,
                        Latitude,
                        Longitude,
                        DataInclusao,
                        DataAlteracao
                    FROM Estabelecimento
                    WHERE Id IN ({placeholders})
                      AND Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != ''
                    ORDER BY Id
                    """
                    query_params.extend(estabelecimentos_ids)
                    
                    # Aplicar limite se especificado
                    if self.limit_rows > 0:
                        sql_query = f"""
                        SELECT 
                            Id,
                            Endereco,
                            Numero,
                            Complemento,
                            Bairro,
                            CEP,
                            Cidade,
                            UF,
                            Latitude,
                            Longitude,
                            DataInclusao,
                            DataAlteracao
                        FROM Estabelecimento
                        WHERE Id IN ({placeholders})
                          AND Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != ''
                        ORDER BY Id
                        OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY
                        """
                    
                    conn_sql = DatabaseConnection.get_sql_server_prd_connection()
                    cursor_sql = conn_sql.cursor()
                    cursor_sql.execute(sql_query, query_params)
                    all_rows = cursor_sql.fetchall()
                    cursor_sql.close()
                    conn_sql.close()
                    print(f"[ETAPA 6] {len(all_rows)} registros carregados. Processando conversões...")
            else:
                # Se não há estabelecimentos migrados, retornar query vazia
                all_rows = []
        else:
            sql_query = """
            SELECT 
                Id,
                Endereco,
                Numero,
                Complemento,
                Bairro,
                CEP,
                Cidade,
                UF,
                Latitude,
                Longitude,
                DataInclusao,
                DataAlteracao
            FROM Estabelecimento
            WHERE Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != ''
            ORDER BY Id
            """
            # Adicionar LIMIT se especificado e não houver mapeamento
            if self.limit_rows > 0:
                sql_query = sql_query.replace("ORDER BY Id", f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
            
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            cursor_sql.execute(sql_query)
            all_rows = cursor_sql.fetchall()
            cursor_sql.close()
            conn_sql.close()
            print(f"[ETAPA 6] {len(all_rows)} registros carregados. Processando conversões...")
        
        # Processar conversões em massa usando DataFrame (vetorizado)
        include_legacy = self.should_include_legacy_id()
        
        # Criar DataFrame usando from_records que é mais adequado para dados de banco
        df = pd.DataFrame.from_records(all_rows, columns=['Id', 'Endereco', 'Numero', 'Complemento', 'Bairro', 'CEP', 'Cidade', 'UF', 'Latitude', 'Longitude', 'DataInclusao', 'DataAlteracao'])
        
        # Lookup store_id (vetorizado)
        df['store_id'] = df['Id'].map(self.store_id_map)
        
        # Remover linhas onde store_id não foi encontrado ou Endereco está vazio
        df = df[df['store_id'].notna() & df['Endereco'].notna()]
        df = df[df['Endereco'].astype(str).str.strip() != '']
        
        # Aplicar transformações vetorizadas
        df['legacy_id'] = df['Id']
        
        # CEP: remover formatação e substituir zeros por padrão
        df['cep'] = df['CEP'].astype(str).str.replace(r'[^\d]', '', regex=True)
        df['cep'] = df['cep'].replace('nan', '')
        df['cep'] = df['cep'].apply(lambda x: '00000000' if not x or x == '0' * len(x) else x)
        
        # Limpar strings
        df['street'] = self.clean_string_vectorized(df['Endereco'], max_length=500).fillna('')
        df['number'] = self.clean_string_vectorized(df['Numero'], max_length=20)
        df['number'] = df['number'].fillna('S/N')
        df['address_line_2'] = self.clean_string_vectorized(df['Complemento'], max_length=200)
        df['neighborhood'] = self.clean_string_vectorized(df['Bairro'], max_length=100).fillna('')
        df['city'] = self.clean_string_vectorized(df['Cidade'], max_length=100).fillna('')
        df['state'] = self.clean_string_vectorized(df['UF'], max_length=2).fillna('')
        df['zone'] = self.clean_string_vectorized(df['Bairro'], max_length=100).fillna('')
        df['region'] = ''
        
        # Converter latitude/longitude (vetorizado)
        df['lat'] = pd.to_numeric(df['Latitude'].astype(str).str.replace(',', '.'), errors='coerce')
        df['lon'] = pd.to_numeric(df['Longitude'].astype(str).str.replace(',', '.'), errors='coerce')
        
        # Datas
        df['data_inclusao'] = df['DataInclusao']
        df['data_alteracao'] = df['DataAlteracao'].fillna(df['DataInclusao'])
        
        # Converter store_id para string
        df['store_id'] = df['store_id'].astype(str)
        
        municipal_code_list = []
        if len(df) > 0:
            df['municipal_code'] = df.apply(
                lambda r: municipal_code_from_origem(
                    None, r['UF'], r['Cidade'], municipio_lookup=municipio_lookup
                ),
                axis=1,
            )
            municipal_code_list = df['municipal_code'].tolist()
        
        # Converter DataFrame diretamente para lista de tuplas (otimizado)
        addressable_type_default = ['Store'] * len(df)
        type_default = ['main'] * len(df)
        
        if include_legacy:
            processed_tuples = list(zip(
                df['legacy_id'].tolist(),
                df['store_id'].tolist(),
                addressable_type_default,
                type_default,
                df['cep'].tolist(),
                df['street'].tolist(),
                df['number'].tolist(),
                df['address_line_2'].tolist(),
                df['neighborhood'].tolist(),
                df['city'].tolist(),
                df['state'].tolist(),
                municipal_code_list,
                df['lat'].tolist(),
                df['lon'].tolist(),
                df['zone'].tolist(),
                df['region'].tolist(),
                df['data_inclusao'].tolist(),
                df['data_alteracao'].tolist()
            ))
        else:
            processed_tuples = list(zip(
                df['store_id'].tolist(),
                addressable_type_default,
                type_default,
                df['cep'].tolist(),
                df['street'].tolist(),
                df['number'].tolist(),
                df['address_line_2'].tolist(),
                df['neighborhood'].tolist(),
                df['city'].tolist(),
                df['state'].tolist(),
                municipal_code_list,
                df['lat'].tolist(),
                df['lon'].tolist(),
                df['zone'].tolist(),
                df['region'].tolist(),
                df['data_inclusao'].tolist(),
                df['data_alteracao'].tolist()
            ))
        
        print(f"[ETAPA 6] {len(processed_tuples)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Query de insert usando gen_random_uuid() - formato para execute_values
        if include_legacy:
            insert_query = f"""
            INSERT INTO {schema}.addresses (
                id, legacy_id, addressable_id, addressable_type, type,
                postal_code, street, number, address_line_2, neighborhood,
                city, state, municipal_code, latitude, longitude, zone, region,
                created_at, updated_at
            ) VALUES %s
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        else:
            insert_query = f"""
            INSERT INTO {schema}.addresses (
                id, addressable_id, addressable_type, type,
                postal_code, street, number, address_line_2, neighborhood,
                city, state, municipal_code, latitude, longitude, zone, region,
                created_at, updated_at
            ) VALUES %s
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        
        chunk_num = 0
        total_processed = 0
        
        try:
            for i in range(0, len(processed_tuples), CHUNK_SIZE):
                chunk = processed_tuples[i:i + CHUNK_SIZE]
                chunk_num += 1
                print(f"[ETAPA 6] Processando chunk {chunk_num} ({len(chunk)} registros)...")
                
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
                        self.stats['addresses'] += len(chunk)
                    except Exception as batch_error:
                        error_msg = f"Erro ao inserir batch de addresses: {batch_error}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
                
                try:
                    conn_pg.commit()
                    print(f"[ETAPA 6] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} enderecos inseridos")
                    logger.info(f"[ETAPA 6] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} enderecos inseridos")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            print(f"\n[ETAPA 6] CONCLUIDA! Total de addresses migrados: {self.stats['addresses']}")
            logger.info(f"ETAPA 6 concluida: {self.stats['addresses']} registros")
            
            self.validate_step6_addresses()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 6: {e}")
            raise
        finally:
            try:
                if 'cursor_pg' in locals() and cursor_pg:
                    cursor_pg.close()
            except:
                pass
            try:
                if 'conn_pg' in locals() and conn_pg:
                    conn_pg.close()
            except:
                pass
    
    # ========================================================================
    # ETAPA 7: CONTACTS (polimórfica para stores)
    # ========================================================================
    
    def validate_step7_contacts(self):
        """Validação e relatório de qualidade - ETAPA 7"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 7: CONTACTS (STORES)")
        print("-"*80)
        
        try:
            # Contar origem - contatos de estabelecimentos (aplicando limite se especificado)
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            # Se houver mapeamento de stores, usar apenas os estabelecimentos migrados
            if self.store_id_map:
                estabelecimentos_ids = list(self.store_id_map.keys())
                if estabelecimentos_ids:
                    placeholders = ','.join(['?' for _ in estabelecimentos_ids])
                    query_email = f"""
                        SELECT COUNT(*) 
                        FROM Estabelecimento
                        WHERE Id IN ({placeholders})
                          AND Email IS NOT NULL AND LTRIM(RTRIM(Email)) != ''
                    """
                    query_phone = f"""
                        SELECT COUNT(*) 
                        FROM Estabelecimento
                        WHERE Id IN ({placeholders})
                          AND Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != ''
                    """
                    query_cellphone = f"""
                        SELECT COUNT(*) 
                        FROM Estabelecimento
                        WHERE Id IN ({placeholders})
                          AND CelularGerente IS NOT NULL AND LTRIM(RTRIM(CelularGerente)) != ''
                    """
                else:
                    query_email = "SELECT COUNT(*) FROM Estabelecimento WHERE 1=0"
                    query_phone = "SELECT COUNT(*) FROM Estabelecimento WHERE 1=0"
                    query_cellphone = "SELECT COUNT(*) FROM Estabelecimento WHERE 1=0"
            else:
                # Sem limite, não precisa de subquery nem ORDER BY
                if self.limit_rows > 0:
                    # Com limite, usar TOP para aplicar o limite
                    query_email = f"""
                        SELECT COUNT(*) 
                        FROM (
                            SELECT TOP {self.limit_rows} Id, Email, Telefone, CelularGerente
                            FROM Estabelecimento
                            WHERE (Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
                               OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
                               OR (CelularGerente IS NOT NULL AND LTRIM(RTRIM(CelularGerente)) != '')
                            ORDER BY Id
                        ) AS limited
                        WHERE Email IS NOT NULL AND LTRIM(RTRIM(Email)) != ''
                    """
                    query_phone = f"""
                        SELECT COUNT(*) 
                        FROM (
                            SELECT TOP {self.limit_rows} Id, Email, Telefone, CelularGerente
                            FROM Estabelecimento
                            WHERE (Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
                               OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
                               OR (CelularGerente IS NOT NULL AND LTRIM(RTRIM(CelularGerente)) != '')
                            ORDER BY Id
                        ) AS limited
                        WHERE Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != ''
                    """
                    query_cellphone = f"""
                        SELECT COUNT(*) 
                        FROM (
                            SELECT TOP {self.limit_rows} Id, Email, Telefone, CelularGerente
                            FROM Estabelecimento
                            WHERE (Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
                               OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
                               OR (CelularGerente IS NOT NULL AND LTRIM(RTRIM(CelularGerente)) != '')
                            ORDER BY Id
                        ) AS limited
                        WHERE CelularGerente IS NOT NULL AND LTRIM(RTRIM(CelularGerente)) != ''
                    """
                else:
                    # Sem limite, query direta sem subquery
                    query_email = """
                        SELECT COUNT(*) 
                        FROM Estabelecimento
                        WHERE Email IS NOT NULL AND LTRIM(RTRIM(Email)) != ''
                    """
                    query_phone = """
                        SELECT COUNT(*) 
                        FROM Estabelecimento
                        WHERE Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != ''
                    """
                    query_cellphone = """
                        SELECT COUNT(*) 
                        FROM Estabelecimento
                        WHERE CelularGerente IS NOT NULL AND LTRIM(RTRIM(CelularGerente)) != ''
                    """
            
            # Executar queries com parâmetros se necessário
            if self.store_id_map:
                estabelecimentos_ids = list(self.store_id_map.keys())
                if estabelecimentos_ids:
                    # ⚠️ IMPORTANTE: SQL Server tem limite de ~2100 parâmetros
                    # Dividir em chunks de 2000 para evitar erro "COUNT field incorrect"
                    MAX_PARAMS_PER_QUERY = 2000
                    origem_email = 0
                    origem_phone = 0
                    origem_cellphone = 0
                    
                    if len(estabelecimentos_ids) > MAX_PARAMS_PER_QUERY:
                        # Dividir em chunks
                        chunks = [estabelecimentos_ids[i:i + MAX_PARAMS_PER_QUERY] 
                                 for i in range(0, len(estabelecimentos_ids), MAX_PARAMS_PER_QUERY)]
                        for chunk in chunks:
                            placeholders = ','.join(['?' for _ in chunk])
                            query_email_chunk = f"""
                                SELECT COUNT(*) 
                                FROM Estabelecimento
                                WHERE Id IN ({placeholders})
                                  AND Email IS NOT NULL AND LTRIM(RTRIM(Email)) != ''
                            """
                            query_phone_chunk = f"""
                                SELECT COUNT(*) 
                                FROM Estabelecimento
                                WHERE Id IN ({placeholders})
                                  AND Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != ''
                            """
                            query_cellphone_chunk = f"""
                                SELECT COUNT(*) 
                                FROM Estabelecimento
                                WHERE Id IN ({placeholders})
                                  AND CelularGerente IS NOT NULL AND LTRIM(RTRIM(CelularGerente)) != ''
                            """
                            cursor_sql.execute(query_email_chunk, chunk)
                            origem_email += cursor_sql.fetchone()[0]
                            
                            cursor_sql.execute(query_phone_chunk, chunk)
                            origem_phone += cursor_sql.fetchone()[0]
                            
                            cursor_sql.execute(query_cellphone_chunk, chunk)
                            origem_cellphone += cursor_sql.fetchone()[0]
                    else:
                        # Menos de 2000 parâmetros, executar queries normais
                        cursor_sql.execute(query_email, estabelecimentos_ids)
                        origem_email = cursor_sql.fetchone()[0]
                        
                        cursor_sql.execute(query_phone, estabelecimentos_ids)
                        origem_phone = cursor_sql.fetchone()[0]
                        
                        cursor_sql.execute(query_cellphone, estabelecimentos_ids)
                        origem_cellphone = cursor_sql.fetchone()[0]
                else:
                    origem_email = 0
                    origem_phone = 0
                    origem_cellphone = 0
            else:
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
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contacts WHERE contactable_type = 'Store'")
            destino_total = cursor_pg.fetchone()[0]
            
            # Contar por tipo
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contacts WHERE contactable_type = 'Store' AND type = 'email'")
            destino_email = cursor_pg.fetchone()[0]
            
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contacts WHERE contactable_type = 'Store' AND type = 'phone'")
            destino_phone = cursor_pg.fetchone()[0]
            
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contacts WHERE contactable_type = 'Store' AND type = 'cellphone'")
            destino_cellphone = cursor_pg.fetchone()[0]
            
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - Estabelecimento):")
            print(f"  Emails: {origem_email}")
            print(f"  Telefones: {origem_phone}")
            print(f"  Celulares Gerente: {origem_cellphone}")
            print(f"  Total esperado: {origem_total}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.contacts):")
            print(f"  Emails (type='email'): {destino_email}")
            print(f"  Telefones (type='phone'): {destino_phone}")
            print(f"  Celulares (type='cellphone'): {destino_cellphone}")
            print(f"  Total inserido: {destino_total}")
            
            diferenca = origem_total - destino_total
            
            if diferenca == 0:
                print(f"\nOK - Todos os contacts foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 7: OK - Origem: {origem_total}, Destino: {destino_total}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} contacts")
                print(f"  {abs(diferenca)} contacts {'faltando' if diferenca > 0 else 'extras'} no destino")
                logger.warning(f"VALIDACAO ETAPA 7: Diferenca - Origem: {origem_total}, Destino: {destino_total}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 7: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step7_migrate_contacts(self):
        """ETAPA 7: Migrar contacts para stores"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 7: MIGRANDO CONTACTS (STORES)")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 7: Migrando contacts para stores")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        # Delete apenas contacts de stores (tabela polimórfica)
        # ⚠️ IMPORTANTE: Se clear_data=True, deleta TODOS os contacts de stores (não apenas filtrados)
        if self.clear_data:
            print("\n[ETAPA 7] Limpando contacts de stores (flag --clear-data ativo - todos os registros)...")
            logger.info("[ETAPA 7] Flag --clear-data ativo: deletando TODOS os contacts de stores")
        else:
            print("\n[ETAPA 7] Limpando contacts de stores...")
        self.delete_polymorphic_table('contacts', 'Store', 'contactable_type')
        
        # Buscar dados do SQL Server
        # Se houver limite, filtrar apenas estabelecimentos que foram migrados
        print("[ETAPA 7] Buscando dados do SQL Server...")
        query_params = []
        
        if self.store_id_map:
            # Buscar apenas estabelecimentos que foram migrados
            estabelecimentos_ids = list(self.store_id_map.keys())
            if estabelecimentos_ids:
                # ⚠️ IMPORTANTE: SQL Server tem limite de ~2100 parâmetros
                # Dividir em chunks de 2000 para evitar erro "COUNT field incorrect"
                MAX_PARAMS_PER_QUERY = 2000
                all_rows = []
                
                if len(estabelecimentos_ids) > MAX_PARAMS_PER_QUERY:
                    # Dividir em chunks
                    chunks = [estabelecimentos_ids[i:i + MAX_PARAMS_PER_QUERY] 
                             for i in range(0, len(estabelecimentos_ids), MAX_PARAMS_PER_QUERY)]
                    
                    print(f"[ETAPA 7] Processando {len(estabelecimentos_ids)} IdEstabelecimento em {len(chunks)} chunks de até {MAX_PARAMS_PER_QUERY} registros cada...")
                    logger.info(f"[ETAPA 7] Processando {len(estabelecimentos_ids)} IdEstabelecimento em {len(chunks)} chunks")
                    
                    conn_sql = DatabaseConnection.get_sql_server_prd_connection()
                    cursor_sql = conn_sql.cursor()
                    
                    for chunk_idx, chunk in enumerate(chunks, 1):
                        placeholders = ','.join(['?' for _ in chunk])
                        chunk_query = f"""
                        SELECT 
                            Id,
                            Email,
                            Telefone,
                            CelularGerente,
                            DataInclusao,
                            DataAlteracao
                        FROM Estabelecimento
                        WHERE Id IN ({placeholders})
                          AND ((Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
                           OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
                           OR (CelularGerente IS NOT NULL AND LTRIM(RTRIM(CelularGerente)) != ''))
                        ORDER BY Id
                        """
                        cursor_sql.execute(chunk_query, chunk)
                        chunk_rows = cursor_sql.fetchall()
                        all_rows.extend(chunk_rows)
                        print(f"[ETAPA 7] Chunk {chunk_idx}/{len(chunks)}: {len(chunk_rows)} registros carregados")
                        logger.info(f"[ETAPA 7] Chunk {chunk_idx}/{len(chunks)}: {len(chunk_rows)} registros carregados")
                    
                    cursor_sql.close()
                    conn_sql.close()
                    
                    print(f"[ETAPA 7] Total: {len(all_rows)} registros carregados de {len(estabelecimentos_ids)} IdEstabelecimento")
                    logger.info(f"[ETAPA 7] Total: {len(all_rows)} registros carregados")
                else:
                    # Menos de 2000 parâmetros, executar query normal
                    placeholders = ','.join(['?' for _ in estabelecimentos_ids])
                    sql_query = f"""
                    SELECT 
                        Id,
                        Email,
                        Telefone,
                        CelularGerente,
                        DataInclusao,
                        DataAlteracao
                    FROM Estabelecimento
                    WHERE Id IN ({placeholders})
                      AND ((Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
                       OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
                       OR (CelularGerente IS NOT NULL AND LTRIM(RTRIM(CelularGerente)) != ''))
                    ORDER BY Id
                    """
                    query_params.extend(estabelecimentos_ids)
                    
                    # Aplicar limite se especificado
                    if self.limit_rows > 0:
                        sql_query = f"""
                        SELECT 
                            Id,
                            Email,
                            Telefone,
                            CelularGerente,
                            DataInclusao,
                            DataAlteracao
                        FROM Estabelecimento
                        WHERE Id IN ({placeholders})
                          AND ((Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
                           OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
                           OR (CelularGerente IS NOT NULL AND LTRIM(RTRIM(CelularGerente)) != ''))
                        ORDER BY Id
                        OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY
                        """
                    
                    conn_sql = DatabaseConnection.get_sql_server_prd_connection()
                    cursor_sql = conn_sql.cursor()
                    cursor_sql.execute(sql_query, query_params)
                    all_rows = cursor_sql.fetchall()
                    cursor_sql.close()
                    conn_sql.close()
                    print(f"[ETAPA 7] {len(all_rows)} registros carregados. Processando conversões...")
            else:
                # Se não há estabelecimentos migrados, retornar query vazia
                all_rows = []
        else:
            sql_query = """
            SELECT 
                Id,
                Email,
                Telefone,
                CelularGerente,
                DataInclusao,
                DataAlteracao
            FROM Estabelecimento
            WHERE (Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
               OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
               OR (CelularGerente IS NOT NULL AND LTRIM(RTRIM(CelularGerente)) != '')
            ORDER BY Id
            """
            # Adicionar LIMIT se especificado e não houver mapeamento
            if self.limit_rows > 0:
                sql_query = sql_query.replace("ORDER BY Id", f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
            
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            cursor_sql.execute(sql_query)
            all_rows = cursor_sql.fetchall()
            cursor_sql.close()
            conn_sql.close()
            print(f"[ETAPA 7] {len(all_rows)} registros carregados. Processando conversões...")
        
        print(f"[ETAPA 7] {len(all_rows)} registros carregados. Processando conversões em massa (vetorizado)...")
        
        # Processar conversões em massa usando DataFrame (vetorizado)
        # Criar DataFrame usando from_records que é mais adequado para dados de banco
        df = pd.DataFrame.from_records(all_rows, columns=['Id', 'Email', 'Telefone', 'CelularGerente', 'DataInclusao', 'DataAlteracao'])
        
        # Lookup store_id (vetorizado)
        df['store_id'] = df['Id'].map(self.store_id_map)
        
        # Remover linhas onde store_id não foi encontrado
        df = df[df['store_id'].notna()]
        
        # Preparar datas
        df['data_inclusao'] = df['DataInclusao']
        df['data_alteracao'] = df['DataAlteracao'].fillna(df['DataInclusao'])
        
        # Converter store_id para string
        df['store_id'] = df['store_id'].astype(str)
        
        # Criar listas de registros para cada tipo de contato e converter para tuplas diretamente
        processed_tuples = []
        addressable_type_default = 'Store'  # Primeira maiúscula e singular (mesma lógica de addressable_type)
        
        # Email (vetorizado)
        email_mask = df['Email'].notna() & (df['Email'].astype(str).str.strip() != '')
        email_df = df[email_mask].copy()
        if len(email_df) > 0:
            email_values = email_df['Email'].astype(str).str.strip().str.lower().tolist()
            processed_tuples.extend(list(zip(
                email_df['store_id'].tolist(),
                [addressable_type_default] * len(email_df),
                ['email'] * len(email_df),
                email_values,
                email_df['data_inclusao'].tolist(),
                email_df['data_alteracao'].tolist()
            )))
        
        # Telefone (vetorizado)
        phone_mask = df['Telefone'].notna() & (df['Telefone'].astype(str).str.strip() != '')
        phone_df = df[phone_mask].copy()
        if len(phone_df) > 0:
            phone_values = phone_df['Telefone'].astype(str).str.replace(r'[^\d]', '', regex=True)
            phone_df = phone_df[phone_values != '']
            if len(phone_df) > 0:
                phone_values_clean = phone_values[phone_values != ''].tolist()
                processed_tuples.extend(list(zip(
                    phone_df['store_id'].tolist(),
                    [addressable_type_default] * len(phone_df),
                    ['phone'] * len(phone_df),
                    phone_values_clean,
                    phone_df['data_inclusao'].tolist(),
                    phone_df['data_alteracao'].tolist()
                )))
        
        # Celular Gerente (vetorizado)
        cellphone_mask = df['CelularGerente'].notna() & (df['CelularGerente'].astype(str).str.strip() != '')
        cellphone_df = df[cellphone_mask].copy()
        if len(cellphone_df) > 0:
            cellphone_values = cellphone_df['CelularGerente'].astype(str).str.replace(r'[^\d]', '', regex=True)
            cellphone_df = cellphone_df[cellphone_values != '']
            if len(cellphone_df) > 0:
                cellphone_values_clean = cellphone_values[cellphone_values != ''].tolist()
                processed_tuples.extend(list(zip(
                    cellphone_df['store_id'].tolist(),
                    [addressable_type_default] * len(cellphone_df),
                    ['cellphone'] * len(cellphone_df),
                    cellphone_values_clean,
                    cellphone_df['data_inclusao'].tolist(),
                    cellphone_df['data_alteracao'].tolist()
                )))
        
        print(f"[ETAPA 7] {len(processed_tuples)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL
        schema = get_schema_atual()
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Query de insert usando gen_random_uuid() - formato para execute_values
        insert_query = f"""
        INSERT INTO {schema}.contacts (
            id, contactable_id, contactable_type, type, value,
            created_at, updated_at
        ) VALUES %s
        """
        insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s)"
        
        chunk_num = 0
        total_processed = 0
        
        try:
            for i in range(0, len(processed_tuples), CHUNK_SIZE):
                chunk = processed_tuples[i:i + CHUNK_SIZE]
                chunk_num += 1
                print(f"[ETAPA 7] Processando chunk {chunk_num} ({len(chunk)} registros)...")
                
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
                        self.stats['contacts'] += len(chunk)
                    except Exception as batch_error:
                        error_msg = f"Erro ao inserir batch de contacts: {batch_error}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
                
                try:
                    conn_pg.commit()
                    print(f"[ETAPA 7] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} contacts inseridos")
                    logger.info(f"[ETAPA 7] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} contacts inseridos")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            print(f"\n[ETAPA 7] CONCLUIDA! Total de contacts migrados: {self.stats['contacts']}")
            logger.info(f"ETAPA 7 concluida: {self.stats['contacts']} registros")
            
            self.validate_step7_contacts()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 7: {e}")
            raise
        finally:
            try:
                if 'cursor_pg' in locals() and cursor_pg:
                    cursor_pg.close()
            except:
                pass
            try:
                if 'conn_pg' in locals() and conn_pg:
                    conn_pg.close()
            except:
                pass
    
    def run(self):
        """Executa a migração completa"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print(f"INICIANDO MIGRACAO: SQL Server PRD -> PostgreSQL {destino} ({schema})")
        print("="*80)
        print(f"Data/Hora: {datetime.now()}")
        print("="*80)
        
        # Log de configuracoes no inicio
        logger.info("="*80)
        logger.info("INICIANDO MIGRACAO COMPLETA - STORES")
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
            # ETAPA 1: Store Segments (primeiro, pois pode ser referenciado)
            self.step1_migrate_store_segments()
            
            # ETAPA 2: Retail Chains (segundo, pois pode ser referenciado)
            self.step2_migrate_retail_chains()
            
            # ETAPA 3: Store Brands (terceiro, pois stores referencia store_brands)
            self.step3_migrate_store_brands()
            
            # ETAPA 4: Stores (quarto, pois outras tabelas referenciam stores)
            self.step4_migrate_stores()
            
            # ETAPA 5: Store CNPJs (quinto, referencia stores)
            self.step5_migrate_store_cnpjs()
            
            # ETAPA 6: Addresses (sexto, polimórfica para stores)
            self.step6_migrate_addresses()
            
            # ETAPA 7: Contacts (sétimo, polimórfica para stores)
            self.step7_migrate_contacts()
            
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
            print(f"  Store Segments: {self.stats['store_segments']}")
            print(f"  Retail Chains: {self.stats['retail_chains']}")
            print(f"  Store Brands: {self.stats['store_brands']}")
            print(f"  Stores: {self.stats['stores']}")
            print(f"  Store CNPJs: {self.stats['store_cnpjs']}")
            print(f"  Addresses: {self.stats['addresses']}")
            print(f"  Contacts: {self.stats['contacts']}")
            print(f"  Erros: {len(self.stats['errors'])}")
            
            logger.info(f"Duracao: {duration}")
            logger.info(f"Store Segments: {self.stats['store_segments']}")
            logger.info(f"Retail Chains: {self.stats['retail_chains']}")
            logger.info(f"Store Brands: {self.stats['store_brands']}")
            logger.info(f"Stores: {self.stats['stores']}")
            logger.info(f"Store CNPJs: {self.stats['store_cnpjs']}")
            logger.info(f"Addresses: {self.stats['addresses']}")
            logger.info(f"Contacts: {self.stats['contacts']}")
            logger.info(f"Erros: {len(self.stats['errors'])}")
            
            if self.stats['errors']:
                print(f"\nAVISO - Total de erros: {len(self.stats['errors'])}")
                print("Verifique o arquivo log_execution.txt para detalhes")
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
    
    migration = StoresMigration(limit_rows=limit_rows)
    migration.run()

