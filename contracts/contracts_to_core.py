"""
Script de migração de dados: SQL Server PRD -> PostgreSQL Destino
Migra dados das tabelas: contracts, contract_scenarios, contract_scenario_stores, 
contract_sellers, contract_team_members, contract_contacts, contract_partners, 
contract_additional_charges; contract_scenarios_brands é populada no destino (etapa 10).
"""

import sys
import os
import re
import uuid
import json
import logging
import pandas as pd
from datetime import date, datetime
from typing import Any, List, Dict, Optional, Tuple, Set
from psycopg2.extras import execute_values

# Raiz do projeto (para import billing.billings_to_core)
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
# Adicionar diretório utils ao path
utils_path = os.path.join(os.path.dirname(__file__), '..', 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)
# ⚠️ CRÍTICO: Importar usando o mesmo caminho do orchestrator para garantir mesma referência
from utils.database_connection import DatabaseConnection
from billing.billings_to_core import (
    XLSX_DEFAULT,
    _load_xlsx_index,
    contract_parameters_billing_type_from_xlsx_row,
    contract_parameters_thirteenth_salary_type_from_xlsx_row,
    farmer_hunter_logins_from_xlsx_row,
)

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


def _sql_server_to_charge_start_date(*candidates) -> date:
    """Primeiro datetime/date não nulo (Orcamento); senão hoje (coluna start_date NOT NULL)."""
    for val in candidates:
        if val is None:
            continue
        if isinstance(val, datetime):
            return val.date()
        if isinstance(val, date):
            return val
    return date.today()


def _scalar_to_float_for_hour_value(val, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        if pd.isna(val):
            return default
    except TypeError:
        pass
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def compute_scenario_hour_value(valor_negociado, frequencia, horas) -> float:
    """
    contract_dictionary: Round(ValorNegociado / (Frequencia * Horas * 4), 2).
    """
    vn = _scalar_to_float_for_hour_value(valor_negociado, 0.0)
    fq = _scalar_to_float_for_hour_value(frequencia, 0.0)
    h = _scalar_to_float_for_hour_value(horas, 0.0)
    denom = fq * h * 4.0
    if denom == 0.0:
        return 0.0
    return round(vn / denom, 2)


def format_hour_value_key(hour_value) -> str:
    """String estável para chave de cenário (2 decimais), alinhada ao hour_value gravado."""
    try:
        if hour_value is None:
            return "0.00"
        if pd.isna(hour_value):
            return "0.00"
        return f"{float(hour_value):.2f}"
    except (TypeError, ValueError):
        return "0.00"


def _normalize_br_cpf_cnpj_digits(digits: str) -> Optional[str]:
    """Chave estável para CPF/CNPJ (só dígitos). Origem usa zeros à esquerda (ex.: 07599886842) → zfill(11); CNPJ zfill(14)."""
    if not digits:
        return None
    L = len(digits)
    if L <= 11:
        return digits.zfill(11)
    if L <= 14:
        return digits.zfill(14)
    return digits


# Expressão T-SQL: mesmo resultado que compute_scenario_hour_value (ViewOrcamentosLojas v)
SQL_VIEW_HOUR_VALUE_CALC = """
CASE
    WHEN ISNULL(CAST(v.Frequencia AS FLOAT), 0) * ISNULL(TRY_CAST(v.Horas AS FLOAT), 0) * 4.0 = 0
        OR (ISNULL(CAST(v.Frequencia AS FLOAT), 0) * ISNULL(TRY_CAST(v.Horas AS FLOAT), 0) * 4.0) IS NULL
        THEN CAST(0 AS DECIMAL(18, 2))
    ELSE CAST(
        ROUND(
            ISNULL(v.ValorNegociado, 0) / NULLIF(
                CAST(v.Frequencia AS FLOAT) * ISNULL(TRY_CAST(v.Horas AS FLOAT), 0) * 4.0, 0
            ),
            2
        ) AS DECIMAL(18, 2))
END
""".replace("\n", " ").strip()

SQL_LIMITED_HOUR_VALUE_CALC = SQL_VIEW_HOUR_VALUE_CALC.replace("v.", "limited.")


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


def _int_from_contratos_xlsx(val, default: int = 1) -> int:
    """Converte célula numérica do XLSX (dia_faturamento / dia_vencimento) para int."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


# ============================================================================
# FUNÇÕES COMPARTILHADAS PARA PROMOTER_TASKS (UNIFICADAS)
# ============================================================================

def normalize_promoter_task_name(nome_tarefa, nome_cliente=None):
    """
    Normaliza e constrói o name de promoter_task de forma consistente.
    Esta função DEVE ser usada tanto no step9 quanto no step2.
    
    Args:
        nome_tarefa: str ou None da ViewOrcamentosLojas.NomeTarefa
        nome_cliente: str ou None da ViewOrcamentosLojas.NomeCliente (não utilizado, mantido para compatibilidade)
    
    Returns:
        tuple: (name_normalizado, nome_tarefa_clean) ou (None, None) se NomeTarefa inválido
    """
    # 1. Normalizar NomeTarefa
    if nome_tarefa:
        nome_tarefa_clean = str(nome_tarefa).strip().upper()
        if not nome_tarefa_clean or nome_tarefa_clean in ['NULL', 'NONE', '']:
            nome_tarefa_clean = None
    else:
        nome_tarefa_clean = None
    
    # 2. Validar NomeTarefa (obrigatório)
    if not nome_tarefa_clean:
        # Se não tem tarefa, não pode criar promoter_task
        return None, None
    
    # 3. Construir name apenas com o nome da tarefa (sem cliente)
    name = f"{nome_tarefa_clean}"
    
    # 4. Remover espaços extras e normalizar
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
                 data_inicio_operacao_max=None, status_pedido_filter=None, clear_data=False):
        """
        Args:
            limit_rows: 0 = todos, > 0 = limitar quantidade
            id_orcamento_filter: Lista de IdOrcamento para filtrar (ex: [6192, 6193])
            data_aviso_previo_min: Data mínima para DataAvisoPrevio (datetime ou string 'YYYY-MM-DD')
            data_inicio_operacao_max: Data máxima para DataInicioOperacao (datetime ou string 'YYYY-MM-DD')
            status_pedido_filter: Lista de StatusPedido para filtrar (ex: [6, 7, 8])
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
            'contract_scenarios_brands': 0,
            'promoter_tasks': 0,
            'billings': 0,
            'customers_total_destino': None,
            'customers_total_legacy_ids_nos_grupos': None,
            'customers_indicador_mesclados': None,
            'errors': []
        }
        self.contract_id_map = {}      # Map: legado_id (IdOrcamento) -> uuid
        self.contract_billing_map = {}  # Map: contract_id (uuid) -> billing_id (uuid) - placeholder até migração de billings
        self.customer_id_map = {}      # Map: legado_id -> uuid (carregado de customers)
        self.store_id_map = {}         # Map: legado_id -> uuid (carregado de stores)
        self.scenario_id_map = {}      # Map: legado_id (IdOrcamentoLoja) -> uuid
        self.promoter_task_map = {}    # Map: (IdTarefa, NomeTarefa_normalizado) -> uuid (promoter_tasks.id)
        self.limit_rows = limit_rows   # 0 = todos, > 0 = limitar quantidade
        self._xlsx_by_orcamento_cache = None  # lazy: contratos_ativos.xlsx (id_orcamento -> linha)
        
        # Filtros opcionais
        self.id_orcamento_filter = id_orcamento_filter if id_orcamento_filter else []
        self.data_aviso_previo_min = data_aviso_previo_min
        self.data_inicio_operacao_max = data_inicio_operacao_max
        self.status_pedido_filter = status_pedido_filter if status_pedido_filter else []
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
                len(self.status_pedido_filter) > 0 or
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
                    'status_pedido': self.status_pedido_filter if self.status_pedido_filter else None,
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
    
    def _collect_destino_stats_snapshot(self):
        """
        Contagens no PostgreSQL destino: billings e customers (dedup / legacy_ids).
        Preenche self.stats para o resumo final.
        """
        destino = DatabaseConnection.get_destino()
        schema_core = "gmcore" if destino == "HML" else "core"
        schema_fin = "gmfinancial" if destino == "HML" else "financial"
        try:
            conn = DatabaseConnection.get_postgresql_destino_connection()
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {schema_fin}.billings")
            self.stats["billings"] = cur.fetchone()[0]
            cur.execute(
                f"""
                SELECT
                    COUNT(*)::bigint,
                    COALESCE(
                        SUM(jsonb_array_length(legacy_ids)) FILTER (
                            WHERE legacy_ids IS NOT NULL AND jsonb_array_length(legacy_ids) > 0
                        ),
                        0
                    )::bigint
                FROM {schema_core}.customers
                """
            )
            row = cur.fetchone()
            n_dest = int(row[0] or 0)
            n_leg = int(row[1] or 0)
            self.stats["customers_total_destino"] = n_dest
            self.stats["customers_total_legacy_ids_nos_grupos"] = n_leg
            self.stats["customers_indicador_mesclados"] = max(0, n_leg - n_dest)
            cur.close()
            conn.close()
        except Exception as e:
            logger.warning(f"[STATS] Snapshot destino (billings/customers): {e}")

    def save_migration_execution_stats_json(self, duration_seconds: Optional[float] = None):
        """
        Grava/atualiza docs/migracao/migration_execution_stats.json com o último run e histórico.
        """
        path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "docs", "migracao", "migration_execution_stats.json")
        )
        destino = DatabaseConnection.get_destino()
        filt = self.load_filter_json() or {}
        agg = (filt.get("aggregated_ids") or {}) if isinstance(filt, dict) else {}
        entry = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "destino": destino,
            "duracao_segundos": duration_seconds,
            "filtro_escopo": {
                "id_orcamento_no_json": len(agg.get("IdOrcamento") or []),
                "id_cliente_no_json": len(agg.get("IdCliente") or []),
            },
            "estatisticas": {
                "contracts": self.stats["contracts"],
                "billings_total_destino": self.stats.get("billings"),
                "promoter_tasks": self.stats["promoter_tasks"],
                "contract_scenarios": self.stats["contract_scenarios"],
                "contract_scenario_stores": self.stats["contract_scenario_stores"],
                "contract_sellers": self.stats["contract_sellers"],
                "contract_team_members": self.stats["contract_team_members"],
                "contract_contacts": self.stats["contract_contacts"],
                "contract_partners": self.stats["contract_partners"],
                "contract_additional_charges": self.stats["contract_additional_charges"],
                "contract_scenarios_brands": self.stats["contract_scenarios_brands"],
                "customers": {
                    "total_customers_deduplicados": self.stats.get("customers_total_destino"),
                    "total_customers_duplicados_legacy_extras": self.stats.get(
                        "customers_indicador_mesclados"
                    ),
                    "total_legacy_ids_referenciados_nos_grupos": self.stats.get(
                        "customers_total_legacy_ids_nos_grupos"
                    ),
                },
                "contract_billing_map_entradas": len(self.contract_billing_map),
                "erros_count": len(self.stats["errors"]),
            },
        }
        historico = []
        if os.path.isfile(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    prev = json.load(f)
                historico = prev.get("historico") or []
            except Exception:
                historico = []
        historico.insert(0, entry)
        historico = historico[:20]
        out = {
            "meta": {
                "titulo": "Estatísticas de execução — migração contracts (e snapshot destino)",
                "ultima_atualizacao": entry["timestamp"],
                "arquivo": "docs/migracao/migration_execution_stats.json",
                "notas": {
                    "total_customers_deduplicados": "COUNT(*) em gmcore.customers (HML) ou core.customers (PRD).",
                    "total_legacy_ids_referenciados_nos_grupos": "Soma de jsonb_array_length(legacy_ids) em todas as linhas.",
                    "total_customers_duplicados_legacy_extras": "max(0, soma_legacy_ids - linhas): quantidade de IDs legados extras agrupados no mesmo registro (dedup por CNPJ).",
                },
            },
            "ultima_execucao": entry,
            "historico": historico,
        }
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(out, f, indent=2, ensure_ascii=False)
            print(f"\n[STATS] Estatísticas gravadas em: {path}")
            logger.info(f"[STATS] migration_execution_stats.json atualizado")
        except Exception as e:
            logger.warning(f"[STATS] Não foi possível gravar migration_execution_stats.json: {e}")

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

    def _ensure_contratos_xlsx_loaded(self) -> None:
        if self._xlsx_by_orcamento_cache is not None:
            return
        path = os.environ.get("CONTRATOS_ATIVOS_FILE", XLSX_DEFAULT)
        _, self._xlsx_by_orcamento_cache = _load_xlsx_index(path)

    def get_xlsx_id_orcamento_set(self) -> Set[int]:
        self._ensure_contratos_xlsx_loaded()
        if not self._xlsx_by_orcamento_cache:
            return set()
        return set(self._xlsx_by_orcamento_cache.keys())

    def _effective_id_orcamento_list_xlsx_intersection(self, ids: Optional[List[int]]) -> List[int]:
        """(ids ou escopo amplo) ∩ IdOrcamento do contratos_ativos.xlsx (linhas com orcamento_ativo)."""
        self._ensure_contratos_xlsx_loaded()
        xlsx_ids = self.get_xlsx_id_orcamento_set()
        if not xlsx_ids:
            raise ValueError(
                "contratos_ativos.xlsx sem IdOrcamento ou arquivo ausente — "
                "verifique CONTRATOS_ATIVOS_FILE e contratos_ativos.xlsx na raiz do projeto"
            )
        if not ids:
            return sorted(xlsx_ids)
        return sorted(set(ids) & xlsx_ids)

    def _load_id_orcamento_from_destino_contracts_only(self) -> List[int]:
        schema = get_schema_atual()
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        out: List[int] = []
        try:
            if self.should_include_legacy_id():
                cursor_pg.execute(
                    f"SELECT DISTINCT legacy_id FROM {schema}.contracts WHERE legacy_id IS NOT NULL"
                )
                out = [row[0] for row in cursor_pg.fetchall()]
        finally:
            cursor_pg.close()
            conn_pg.close()
        return out

    def _resolve_id_orcamento_list_for_steps(self) -> List[int]:
        """
        IdOrcamento efetivos para etapas 2–9, 10 e billings: (JSON ou CLI ou destino) ∩ XLSX ativos.
        """
        filter_data = self.load_filter_json()
        ids: List[int] = []
        if filter_data and filter_data.get("aggregated_ids"):
            ids = list(filter_data["aggregated_ids"].get("IdOrcamento") or [])
        if ids:
            return self._effective_id_orcamento_list_xlsx_intersection(ids)
        if self.id_orcamento_filter:
            return self._effective_id_orcamento_list_xlsx_intersection(list(self.id_orcamento_filter))
        dest = self._load_id_orcamento_from_destino_contracts_only()
        if dest:
            return self._effective_id_orcamento_list_xlsx_intersection(dest)
        return self._effective_id_orcamento_list_xlsx_intersection(None)

    def _apply_xlsx_scope_intersection(self) -> None:
        """Restringe self.id_orcamento_filter à interseção com contratos_ativos.xlsx (antes do preview)."""
        if not self.get_xlsx_id_orcamento_set():
            raise ValueError(
                "contratos_ativos.xlsx sem IdOrcamento (orcamento_ativo) ou arquivo ausente — "
                "verifique CONTRATOS_ATIVOS_FILE."
            )
        before = list(self.id_orcamento_filter) if self.id_orcamento_filter else []
        self.id_orcamento_filter = self._effective_id_orcamento_list_xlsx_intersection(
            before if before else None
        )
        if not self.id_orcamento_filter:
            raise ValueError("Interseção IdOrcamento × XLSX vazia — ajuste filtros ou planilha.")
        nx = len(self.get_xlsx_id_orcamento_set())
        print(
            f"[ETAPA 1] Escopo ∩ contratos_ativos.xlsx: {len(self.id_orcamento_filter)} IdOrcamento "
            f"(planilha: {nx}; filtro CLI antes: {len(before)})"
        )
        logger.info(f"[ETAPA 1] id_orcamento_filter após ∩ XLSX: {len(self.id_orcamento_filter)}")
    
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
            
            # Filtro StatusPedido
            if len(self.status_pedido_filter) > 0:
                placeholders = ','.join(['?' for _ in self.status_pedido_filter])
                where_conditions.append(f"v.StatusPedido IN ({placeholders})")
                query_params.extend(self.status_pedido_filter)
            
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
        
        self._apply_xlsx_scope_intersection()
        
        # Buscar dados do SQL Server primeiro para identificar quais customers são necessários
        # ⚠️ IMPORTANTE: Usar SELECT DISTINCT para garantir que todos os IdOrcamento únicos sejam coletados
        print("[ETAPA 1] Buscando dados do SQL Server para identificar customers necessários...")
        sql_query_preview = """
        SELECT DISTINCT
            v.IdOrcamento,
            v.IdCliente
        FROM ViewOrcamentosLojas v
        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
        """
        
        # Construir WHERE clause com filtros opcionais (mesma lógica do query principal)
        where_conditions_preview = []
        query_params_preview = []
        
        # ⚠️ CRÍTICO: Log dos filtros aplicados na query preview
        print(f"[ETAPA 1] Preview - Filtros recebidos:")
        print(f"  - id_orcamento_filter: {self.id_orcamento_filter}")
        print(f"  - data_aviso_previo_min: {self.data_aviso_previo_min}")
        print(f"  - data_inicio_operacao_max: {self.data_inicio_operacao_max}")
        print(f"  - status_pedido_filter: {self.status_pedido_filter}")
        logger.info(f"[ETAPA 1] Preview - Filtros: id_orcamento={self.id_orcamento_filter}, "
                   f"data_aviso={self.data_aviso_previo_min}, data_inicio={self.data_inicio_operacao_max}, "
                   f"status_pedido={self.status_pedido_filter}")
        
        if len(self.id_orcamento_filter) > 0:
            placeholders = ','.join(['?' for _ in self.id_orcamento_filter])
            where_conditions_preview.append(f"v.IdOrcamento IN ({placeholders})")
            query_params_preview.extend(self.id_orcamento_filter)
            print(f"[ETAPA 1] Preview - Aplicando filtro IdOrcamento: {self.id_orcamento_filter}")
        else:
            print(f"[ETAPA 1] Preview - SEM filtro IdOrcamento (buscando TODOS os orçamentos que atendem aos outros filtros)")
        
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
        
        # ⚠️ IMPORTANTE: Aplicar filtro StatusPedido também na query preview
        if len(self.status_pedido_filter) > 0:
            placeholders = ','.join(['?' for _ in self.status_pedido_filter])
            where_conditions_preview.append(f"v.StatusPedido IN ({placeholders})")
            query_params_preview.extend(self.status_pedido_filter)
            print(f"[ETAPA 1] Preview - Aplicando filtro StatusPedido: {self.status_pedido_filter}")
        else:
            print(f"[ETAPA 1] Preview - SEM filtro StatusPedido")
        
        # Filtro IdCliente (obrigatório conforme query base)
        where_conditions_preview.append("v.IdCliente IS NOT NULL")
        
        if where_conditions_preview:
            sql_query_preview += " WHERE " + " AND ".join(where_conditions_preview)
        
        print(f"[ETAPA 1] Preview - Query WHERE clause: {where_conditions_preview}")
        print(f"[ETAPA 1] Preview - Query params: {query_params_preview}")
        logger.info(f"[ETAPA 1] Preview - WHERE conditions: {where_conditions_preview}")
        logger.info(f"[ETAPA 1] Preview - Query params: {query_params_preview}")
        
        # ⚠️ IMPORTANTE: Remover GROUP BY já que estamos usando SELECT DISTINCT
        # GROUP BY não é necessário com SELECT DISTINCT e pode causar problemas
        
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
        
        print(f"[ETAPA 1] Preview: Encontrados {len(id_orcamento_preview)} IdOrcamento únicos na query preview")
        logger.info(f"[ETAPA 1] Preview: Encontrados {len(id_orcamento_preview)} IdOrcamento únicos na query preview")
        if len(id_orcamento_preview) > 0 and len(id_orcamento_preview) <= 10:
            print(f"[ETAPA 1] Preview: IdOrcamento encontrados: {id_orcamento_preview}")
        elif len(id_orcamento_preview) > 10:
            print(f"[ETAPA 1] Preview: IdOrcamento encontrados (primeiros 10): {id_orcamento_preview[:10]}...")
        
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
                'status_pedido': self.status_pedido_filter if self.status_pedido_filter else None,
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
        cursor_customers.execute(f"SELECT id, legacy_ids FROM {schema_customers}.customers WHERE legacy_ids IS NOT NULL AND jsonb_array_length(legacy_ids) > 0")
        for row in cursor_customers.fetchall():
            uuid_row, leg_ids = row[0], row[1]
            if leg_ids:
                for leg_id in leg_ids:
                    self.customer_id_map[leg_id] = uuid_row
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
                cursor_customers.execute(f"SELECT id, legacy_ids FROM {schema_customers}.customers WHERE legacy_ids IS NOT NULL AND jsonb_array_length(legacy_ids) > 0")
                for row in cursor_customers.fetchall():
                    uuid_row, leg_ids = row[0], row[1]
                    if leg_ids:
                        for leg_id in leg_ids:
                            self.customer_id_map[leg_id] = uuid_row
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
            MIN(v.IdCliente) AS IdCliente,
            MAX(v.NomeCliente) AS NomeCliente,
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
        
        # Filtro StatusPedido
        if len(self.status_pedido_filter) > 0:
            placeholders = ','.join(['?' for _ in self.status_pedido_filter])
            where_conditions.append(f"v.StatusPedido IN ({placeholders})")
            query_params.extend(self.status_pedido_filter)
        
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
                       f"StatusPedido={self.status_pedido_filter}, "
                       f"limit_rows={self.limit_rows}, clear_data={self.clear_data}")
            print(f"[ETAPA 1] Filtros aplicados: IdOrcamento={self.id_orcamento_filter}, "
                  f"DataAvisoPrevio_min={self.data_aviso_previo_min}, "
                  f"DataInicioOperacao_max={self.data_inicio_operacao_max}, "
                  f"StatusPedido={self.status_pedido_filter}")
        
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
                
                # Filtro StatusPedido
                if len(self.status_pedido_filter) > 0:
                    placeholders = ','.join(['?' for _ in self.status_pedido_filter])
                    where_conditions_ids.append(f"v.StatusPedido IN ({placeholders})")
                    query_params_ids.extend(self.status_pedido_filter)
                
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
        
        # contratos_ativos.xlsx (cache já carregado em _apply_xlsx_scope_intersection): dia_faturamento / dia_vencimento
        self._ensure_contratos_xlsx_loaded()
        xlsx_by_orcamento = self._xlsx_by_orcamento_cache or {}
        if xlsx_by_orcamento:
            print(
                f"[ETAPA 1] XLSX contratos: {len(xlsx_by_orcamento)} IdOrcamento — "
                "contract_parameters_billing_day/due_day de dia_faturamento/dia_vencimento quando houver linha."
            )
        else:
            print(
                "[ETAPA 1] AVISO: XLSX sem linhas — billing_day/due_day usam ViewOrcamentosLojas."
            )
            logger.warning("[ETAPA 1] XLSX vazio; billing_day/due_day da view SQL")
        
        # Processar tudo em memória e preparar batch_values
        # customer_id = UUID gmcore.customers.id (via customer_id_map[IdCliente])
        # legacy_id = IdOrcamento (legado_id)
        batch_values = []
        legacy_ids_list = []
        
        for row in all_rows:
            try:
                legado_id = row[0]  # IdOrcamento
                code = row[0]  # IdOrcamento
                customer_legacy_id = row[1]  # IdCliente (MIN por grupo — alinhado CUST-001)
                nome_cliente_view = row[2]  # NomeCliente (MAX por grupo — CONTRACTS_LEGACY_TITLE)
                # Índices após NomeCliente: 3 DiaFaturamento, 4 DiaVencimento, 5 StatusPedido, ...
                
                # Mapear customer_id
                customer_uuid = self.customer_id_map.get(customer_legacy_id)
                if not customer_uuid:
                    # Não logar individualmente para não prejudicar performance
                    # Apenas contar o erro e continuar
                    self.stats['errors'].append(f"Customer nao encontrado: IdCliente={customer_legacy_id} para Orcamento Id={legado_id}")
                    continue
                
                # Preparar valores
                # contract_parameters_*: prioridade contratos_ativos.xlsx (dia_faturamento, dia_vencimento); senão VIEW
                xlsx_row = xlsx_by_orcamento.get(legado_id) if xlsx_by_orcamento else None
                fallback_billing_type = self.map_billing_type(row[9])  # ModoFaturamento (fallback)
                fallback_thirteenth = self.map_thirteenth_salary_type(row[11])  # TipoCalculoDecimoTerceiro (fallback)
                if xlsx_row is not None:
                    billing_day = _int_from_contratos_xlsx(xlsx_row.get("dia_faturamento"), 1)
                    due_day = _int_from_contratos_xlsx(xlsx_row.get("dia_vencimento"), 1)
                    billing_type = contract_parameters_billing_type_from_xlsx_row(
                        xlsx_row, fallback_billing_type
                    )
                    thirteenth_salary_type = contract_parameters_thirteenth_salary_type_from_xlsx_row(
                        xlsx_row, fallback_thirteenth
                    )
                else:
                    billing_day = row[3] if row[3] is not None else 1
                    due_day = row[4] if row[4] is not None else 1
                    billing_type = fallback_billing_type
                    thirteenth_salary_type = fallback_thirteenth
                operation_type = self.map_operation_type(row[10])  # TipoOrcamento
                trade_type = self.map_trade_type(row[12])  # TradeMarketing
                start_date = row[6] if row[6] is not None else row[7]  # DataInicioOperacao ou DataInclusaoOrcamento como fallback
                if start_date is None:
                    start_date = datetime.now()  # Se ainda for None, usar data atual
                # anterior
                # status = self.convert_status_pedido(row[4])  # StatusPedido
                status = 'active'
                created_at = row[7] if row[7] else datetime.now()  # DataInclusaoOrcamento
                updated_at = row[8] if row[8] else datetime.now()  # DataAlteracaoOrcamento
                title = self.clean_string(nome_cliente_view, max_length=255)
                
                if include_legacy:
                    # contracts (PRD) estrutura atual:
                    # id, legacy_id, code, customer_id,
                    # contract_parameters_billing_day, contract_parameters_due_day,
                    # contract_parameters_billing_type, operation_type,
                    # contract_parameters_thirteenth_salary_type, trade_type,
                    # start_date, end_date, status, deleted_at,
                    # observations, expected_amount, created_at, updated_at,
                    # title, legacy_customer_id
                    batch_values.append((
                        legado_id,           # legacy_id
                        code,                # code
                        str(customer_uuid),  # customer_id
                        billing_day,         # contract_parameters_billing_day
                        due_day,             # contract_parameters_due_day
                        billing_type,        # contract_parameters_billing_type
                        operation_type,
                        thirteenth_salary_type,  # contract_parameters_thirteenth_salary_type
                        trade_type,
                        start_date,
                        None,                # end_date
                        status,
                        None,                # deleted_at
                        None,                # observations
                        0,                   # expected_amount (por enquanto)
                        created_at,
                        updated_at,
                        title,               # title = NomeCliente (view)
                        customer_legacy_id   # legacy_customer_id = IdCliente (não IdOrcamento)
                    ))
                    legacy_ids_list.append(legado_id)
                else:
                    batch_values.append((
                        code,                # code (sem legacy_id)
                        str(customer_uuid),
                        billing_day,
                        due_day,
                        billing_type,
                        operation_type,
                        thirteenth_salary_type,
                        trade_type,
                        start_date,
                        None,                # end_date
                        status,
                        None,                # deleted_at
                        None,                # observations
                        0,                   # expected_amount
                        created_at,
                        updated_at,
                        title,               # title
                        customer_legacy_id   # legacy_customer_id = IdCliente
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
        # Estrutura atual da tabela commercial.contracts:
        # id, legacy_id, code, customer_id,
        # contract_parameters_billing_day, contract_parameters_due_day,
        # contract_parameters_billing_type, operation_type,
        # contract_parameters_thirteenth_salary_type, trade_type,
        # start_date, end_date, status, deleted_at,
        # observations, expected_amount, created_at, updated_at,
        # title, legacy_customer_id
        if include_legacy:
            insert_query = f"""
            INSERT INTO {schema}.contracts (
                id, legacy_id, code, customer_id,
                contract_parameters_billing_day, contract_parameters_due_day,
                contract_parameters_billing_type, operation_type,
                contract_parameters_thirteenth_salary_type, trade_type,
                start_date, end_date, status, deleted_at,
                observations, expected_amount, created_at, updated_at,
                title, legacy_customer_id
            ) VALUES %s
            """
            insert_template = (
                "(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
        else:
            insert_query = f"""
            INSERT INTO {schema}.contracts (
                id, code, customer_id,
                contract_parameters_billing_day, contract_parameters_due_day,
                contract_parameters_billing_type, operation_type,
                contract_parameters_thirteenth_salary_type, trade_type,
                start_date, end_date, status, deleted_at,
                observations, expected_amount, created_at, updated_at,
                title, legacy_customer_id
            ) VALUES %s
            """
            insert_template = (
                "(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
        
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
            
            # Contar origem aplicando os mesmos filtros do step1 e step2
            # ⚠️ IMPORTANTE: Contar CENÁRIOS ÚNICOS usando DISTINCT (mesma lógica do step2)
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            # Construir WHERE clause com filtros opcionais (mesmo padrão do step2)
            where_conditions = []
            query_params = []
            
            if id_orcamento_list:
                placeholders = ','.join(['?' for _ in id_orcamento_list])
                where_conditions.append(f"v.IdOrcamento IN ({placeholders})")
                query_params.extend(id_orcamento_list)
            
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
            
            # Filtro StatusPedido
            if len(self.status_pedido_filter) > 0:
                placeholders = ','.join(['?' for _ in self.status_pedido_filter])
                where_conditions.append(f"v.StatusPedido IN ({placeholders})")
                query_params.extend(self.status_pedido_filter)
            
            # Filtro IdCliente (obrigatório conforme query base)
            where_conditions.append("v.IdCliente IS NOT NULL")
            
            # Construir query de contagem com DISTINCT (hour_value = fórmula contract_dictionary)
            hour_key_sql = (
                "ISNULL(CAST((" + SQL_VIEW_HOUR_VALUE_CALC + ") AS VARCHAR(50)), '0.00')"
            )
            if where_conditions:
                count_query = f"""
                SELECT COUNT(DISTINCT 
                    CAST(v.IdOrcamento AS VARCHAR) + '|' + 
                    ISNULL(CAST(v.Frequencia AS VARCHAR), '') + '|' + 
                    ISNULL(CAST(v.Horas AS VARCHAR), '') + '|' + 
                    {hour_key_sql} + '|' + 
                    ISNULL(CONVERT(VARCHAR, v.DataInicioOperacao, 120), '') + '|' + 
                    ISNULL(CONVERT(VARCHAR, v.DataAvisoPrevio, 120), '')
                )
                FROM ViewOrcamentosLojas v
                INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                WHERE {' AND '.join(where_conditions)}
                """
                cursor_sql.execute(count_query, query_params)
            elif self.limit_rows > 0:
                # Contar cenários únicos com LIMIT (hour_key com alias limited)
                hour_key_limited = (
                    "ISNULL(CAST((" + SQL_LIMITED_HOUR_VALUE_CALC + ") AS VARCHAR(50)), '0.00')"
                )
                count_query = f"""
                SELECT COUNT(DISTINCT 
                    CAST(limited.IdOrcamento AS VARCHAR) + '|' + 
                    ISNULL(CAST(limited.Frequencia AS VARCHAR), '') + '|' + 
                    ISNULL(CAST(limited.Horas AS VARCHAR), '') + '|' + 
                    {hour_key_limited} + '|' + 
                    ISNULL(CONVERT(VARCHAR, limited.DataInicioOperacao, 120), '') + '|' + 
                    ISNULL(CONVERT(VARCHAR, limited.DataAvisoPrevio, 120), '')
                )
                FROM (
                    SELECT DISTINCT TOP {self.limit_rows}
                        v.IdOrcamento, v.Frequencia, v.Horas, v.ValorNegociado,
                        v.DataInicioOperacao, v.DataAvisoPrevio
                    FROM ViewOrcamentosLojas v
                    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                    WHERE v.IdCliente IS NOT NULL
                    ORDER BY v.IdOrcamento, v.Frequencia, v.Horas, v.ValorNegociado, v.DataInicioOperacao, v.DataAvisoPrevio
                ) AS limited
                """
                cursor_sql.execute(count_query)
            else:
                # Contar todos os cenários únicos
                count_query = f"""
                SELECT COUNT(DISTINCT 
                    CAST(v.IdOrcamento AS VARCHAR) + '|' + 
                    ISNULL(CAST(v.Frequencia AS VARCHAR), '') + '|' + 
                    ISNULL(CAST(v.Horas AS VARCHAR), '') + '|' + 
                    {hour_key_sql} + '|' + 
                    ISNULL(CONVERT(VARCHAR, v.DataInicioOperacao, 120), '') + '|' + 
                    ISNULL(CONVERT(VARCHAR, v.DataAvisoPrevio, 120), '')
                )
                FROM ViewOrcamentosLojas v
                INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                WHERE v.IdCliente IS NOT NULL
                """
                cursor_sql.execute(count_query)
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
            
            print(f"\nORIGEM (SQL Server PRD - ViewOrcamentosLojas - CENÁRIOS ÚNICOS):")
            print(f"  Total de cenários únicos: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.contract_scenarios):")
            print(f"  Total de registros: {destino_count}")
            
            diferenca = origem_count - destino_count
            
            if diferenca == 0:
                print(f"\nOK - Todos os cenários únicos foram migrados com sucesso!")
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
            schema_pdv = get_schema_pdv()

            # Verificar se já existe no banco (PRD: pdv.*, HML: gmpdv.*)
            cursor_pg.execute(f"""
                SELECT id FROM {schema_pdv}.promoter_tasks WHERE name = %s
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
                
                cursor_pg.execute(f"""
                    INSERT INTO {schema_pdv}.promoter_tasks 
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
        
        filter_data = self.load_filter_json()
        try:
            id_orcamento_filter_list = self._resolve_id_orcamento_list_for_steps()
        except ValueError as e:
            logger.error(f"[ETAPA 9] {e}")
            print(f"ERRO - {e}")
            return
        logger.info(f"[ETAPA 9] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")
        print(f"[ETAPA 9] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")
        
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
        
        # Construir query para coletar tarefas únicas da ViewOrcamentosLojas (sem NomeCliente)
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
            
            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)
            
            # Query para coletar NomeTarefa únicos (sem NomeCliente)
            query_tarefas = f"""
            SELECT DISTINCT
                v.NomeTarefa
            FROM ViewOrcamentosLojas v
            INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
            {where_clause}
            AND v.NomeTarefa IS NOT NULL
            AND LTRIM(RTRIM(v.NomeTarefa)) != ''
            """
            
            if self.limit_rows > 0:
                query_tarefas = query_tarefas.replace("SELECT DISTINCT", f"SELECT DISTINCT TOP {self.limit_rows}")
            
            print(f"[ETAPA 9] Coletando tarefas únicas da ViewOrcamentosLojas...")
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
            
            print(f"[ETAPA 9] Encontradas {len(tarefas_view)} tarefas únicas")
            logger.info(f"[ETAPA 9] Encontradas {len(tarefas_view)} tarefas únicas")
            
            # Conectar ao PostgreSQL
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            # Preparar dados para inserção
            records_to_insert = []
            now = datetime.now()
            seen_names = set()  # Para evitar duplicatas por name
            
            for row in tarefas_view:
                nome_tarefa_raw = row[0]
                
                # Usar função compartilhada para normalização (sem NomeCliente)
                name, nome_tarefa_clean = normalize_promoter_task_name(nome_tarefa_raw)
                
                # Se name é None, pular (NomeTarefa obrigatório)
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
    
    # =========================================================================
    # BILLINGS (financial / gmfinancial) + fallback placeholder
    # =========================================================================
    def _reload_customer_id_map_from_destino(self):
        """Recarrega customer_id_map a partir de gmcore/core.customers (legacy_ids). Usado após step1 isolado."""
        destino = DatabaseConnection.get_destino()
        schema_customers = "gmcore" if destino == "HML" else "core"
        if destino == "PRD":
            conn = DatabaseConnection.get_postgresql_prd_destino_connection()
        else:
            conn = DatabaseConnection.get_postgresql_hml_destino_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT id, legacy_ids FROM {schema_customers}.customers
            WHERE legacy_ids IS NOT NULL AND jsonb_array_length(legacy_ids) > 0
            """
        )
        self.customer_id_map.clear()
        for uuid_row, leg_ids in cursor.fetchall():
            if leg_ids:
                for leg_id in leg_ids:
                    self.customer_id_map[leg_id] = uuid_row
        cursor.close()
        conn.close()
        logger.info(
            f"[BILLINGS] customer_id_map recarregado: {len(self.customer_id_map)} chaves"
        )

    def _reload_contract_id_map_from_destino(self):
        """Recarrega contract_id_map (legacy_id IdOrcamento -> UUID) a partir do destino."""
        schema = get_schema_atual()
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        self.contract_id_map.clear()
        if self.should_include_legacy_id():
            cursor_pg.execute(
                f"SELECT id, legacy_id FROM {schema}.contracts WHERE legacy_id IS NOT NULL"
            )
            for row in cursor_pg.fetchall():
                if row[1] is not None:
                    self.contract_id_map[row[1]] = row[0]
        cursor_pg.close()
        conn_pg.close()
        logger.info(
            f"[BILLINGS] contract_id_map recarregado: {len(self.contract_id_map)} contratos"
        )

    def ensure_billings_after_step1(self):
        """
        Migração real em billing/billings_to_core (escopo JSON ∩ XLSX) ou placeholders
        se mapa continuar vazio. Preenche contract_billing_map.
        Chamado pelo run() completo e pelo orchestrator_tasks (steps 2 e 8).
        """
        if not self.customer_id_map:
            self._reload_customer_id_map_from_destino()
        if not self.contract_id_map:
            self._reload_contract_id_map_from_destino()
        n_bill_map = 0
        try:
            from billing.billings_to_core import migrate_billings_for_contracts

            n_bill_map = migrate_billings_for_contracts(self)
        except Exception as e:
            logger.exception(f"[BILLINGS] Migração financeira falhou: {e}")
        if n_bill_map == 0:
            self._ensure_billing_placeholders_for_contracts()

    def _ensure_billing_placeholders_for_contracts(self):
        """Cria um billing placeholder por customer dos contracts migrados. Preenche contract_billing_map."""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        schema_financial = 'gmfinancial' if destino == 'HML' else 'financial'
        
        print("\n[BILLING PLACEHOLDER] Garantindo billings para customers dos contracts...")
        logger.info("[BILLING PLACEHOLDER] Iniciando criação de placeholders")
        
        try:
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            try:
                id_orcamento_filter_list = self._resolve_id_orcamento_list_for_steps()
            except ValueError as e:
                logger.error(f"[BILLING PLACEHOLDER] {e}")
                id_orcamento_filter_list = []
            
            if id_orcamento_filter_list:
                placeholders = ','.join(['%s' for _ in id_orcamento_filter_list])
                cursor_pg.execute(f"""
                    SELECT id, customer_id FROM {schema}.contracts
                    WHERE legacy_id IN ({placeholders})
                """, id_orcamento_filter_list)
            else:
                cursor_pg.execute(f"SELECT id, customer_id FROM {schema}.contracts")
            
            contract_rows = cursor_pg.fetchall()
            if not contract_rows:
                print("[BILLING PLACEHOLDER] AVISO: Nenhum contract encontrado (tabela vazia ou filtro sem match)")
                logger.warning("[BILLING PLACEHOLDER] Nenhum contract encontrado")
            customer_billing_map = {}  # customer_id -> billing_id
            
            for contract_uuid, customer_uuid in contract_rows:
                if not customer_uuid:
                    continue
                cid = str(customer_uuid)
                if cid in customer_billing_map:
                    self.contract_billing_map[str(contract_uuid)] = customer_billing_map[cid]
                    continue
                
                # Verificar se já existe billing para este customer
                cursor_pg.execute(
                    f"SELECT id FROM {schema_financial}.billings WHERE customer_id = %s LIMIT 1",
                    (cid,)
                )
                row = cursor_pg.fetchone()
                if row:
                    billing_id = str(row[0])
                else:
                    # Inserir placeholder
                    now = datetime.now()
                    cursor_pg.execute(f"""
                        INSERT INTO {schema_financial}.billings (
                            id, customer_id, name, observations, deleted_at, is_active,
                            created_at, updated_at, calculation_type,
                            contract_parameters_billing_day, contract_parameters_billing_type,
                            contract_parameters_due_day, contract_parameters_thirteenth_salary_type
                        ) VALUES (
                            gen_random_uuid(), %s, %s, NULL, NULL, true,
                            %s, %s, '', 0, '', 0, ''
                        )
                        RETURNING id
                    """, (cid, 'Placeholder - Migração', now, now))
                    billing_id = str(cursor_pg.fetchone()[0])
                
                customer_billing_map[cid] = billing_id
                self.contract_billing_map[str(contract_uuid)] = billing_id
            
            conn_pg.commit()
            cursor_pg.close()
            conn_pg.close()
            
            print(f"[BILLING PLACEHOLDER] OK - {len(customer_billing_map)} billings (contract_billing_map: {len(self.contract_billing_map)} contratos)")
            logger.info(f"[BILLING PLACEHOLDER] {len(customer_billing_map)} billings criados/encontrados")
        except Exception as e:
            logger.error(f"[BILLING PLACEHOLDER] Erro: {e}")
            raise
    # =========================================================================
    # FIM BLOCO TEMPORÁRIO - BILLING PLACEHOLDERS
    # =========================================================================
    
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
        
        # ⚠️ IMPORTANTE: store_id foi removido da tabela contract_scenarios
        # Não precisamos mais carregar mapeamento de stores para step2
        # (será usado apenas no step3 para contract_scenario_stores)
        
        try:
            id_orcamento_filter_list = self._resolve_id_orcamento_list_for_steps()
        except ValueError as e:
            logger.error(f"[ETAPA 2] {e}")
            print(f"ERRO - {e}")
            raise
        logger.info(f"[ETAPA 2] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")
        print(f"[ETAPA 2] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")
        
        # Limpar tabela (TRUNCATE ou DELETE baseado em filtros)
        if not id_orcamento_filter_list or self.clear_data:
            print("\n[ETAPA 2] Limpando tabela contract_scenarios...")
            self.clean_table('contract_scenarios')
        else:
            # Buscar IdOrcamentoLoja que serão deletados primeiro
            print("\n[ETAPA 2] Preparando limpeza de contract_scenarios...")
        
        # Buscar dados do SQL Server usando ViewOrcamentosLojas
        # CENÁRIOS ÚNICOS: IdOrcamento, Frequencia, Horas, ValorNegociado, datas (+ hour_value derivado da fórmula contract_dictionary)
        # MAX() para DataInclusao/DataAlteracao quando várias lojas compartilham o mesmo cenário
        print("[ETAPA 2] Buscando dados do SQL Server para criar cenários únicos...")
        sql_query = f"""
        SELECT 
            v.IdOrcamento,
            v.Frequencia,
            v.Horas,
            v.ValorNegociado,
            MAX({SQL_VIEW_HOUR_VALUE_CALC}) AS hour_value_calc,
            CONVERT(DATE,v.DataInicioOperacao) AS DataInicioOperacao,
            CONVERT(DATE,v.DataAvisoPrevio) AS DataAvisoPrevio,
            v.NomeTarefa,
            v.NomeCliente,
            v.StatusPedido,
            MAX(v.DataInclusaoOrcamentoLojas) AS DataInclusaoOrcamentoLojas,
            MAX(v.DataAlteracaoOrcamentoLojas) AS DataAlteracaoOrcamentoLojas,
            v.IdTarefa,
            v.IdCliente
        FROM ViewOrcamentosLojas v
        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
        """
        
        # Construir WHERE clause com filtros opcionais (mesmo padrão do step1)
        where_conditions = []
        query_params = []
        
        # Filtro IdOrcamento
        if id_orcamento_filter_list:
            placeholders = ','.join(['?' for _ in id_orcamento_filter_list])
            where_conditions.append(f"v.IdOrcamento IN ({placeholders})")
            query_params.extend(id_orcamento_filter_list)
        
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
        
        # Filtro StatusPedido
        if len(self.status_pedido_filter) > 0:
            placeholders = ','.join(['?' for _ in self.status_pedido_filter])
            where_conditions.append(f"v.StatusPedido IN ({placeholders})")
            query_params.extend(self.status_pedido_filter)
        
        # Filtro IdCliente (obrigatório conforme query base)
        where_conditions.append("v.IdCliente IS NOT NULL")
        
        # Adicionar WHERE se houver condições
        if where_conditions:
            sql_query += " WHERE " + " AND ".join(where_conditions)
        
        # ⚠️ IMPORTANTE: Adicionar GROUP BY com todas as colunas não agregadas
        # Isso garante que apenas cenários únicos sejam retornados, usando MAX() para as datas datetime
        sql_query += f"""
        GROUP BY 
            v.IdOrcamento,
            v.Frequencia,
            v.Horas,
            v.ValorNegociado,
            CONVERT(DATE,v.DataInicioOperacao),
            CONVERT(DATE,v.DataAvisoPrevio),
            v.NomeTarefa,
            v.NomeCliente,
            v.StatusPedido,
            v.IdTarefa,
            v.IdCliente
        ORDER BY 
            v.IdOrcamento, 
            v.Frequencia, 
            v.Horas, 
            v.ValorNegociado, 
            CONVERT(DATE,v.DataInicioOperacao), 
            CONVERT(DATE,v.DataAvisoPrevio)
        """
        
        # Adicionar LIMIT se especificado
        if self.limit_rows > 0:
            sql_query = sql_query.replace(
                "ORDER BY \n            v.IdOrcamento, \n            v.Frequencia, \n            v.Horas, \n            v.ValorNegociado, \n            CONVERT(DATE,v.DataInicioOperacao), \n            CONVERT(DATE,v.DataAvisoPrevio)\n        ",
                f"ORDER BY \n            v.IdOrcamento, \n            v.Frequencia, \n            v.Horas, \n            v.ValorNegociado, \n            CONVERT(DATE,v.DataInicioOperacao), \n            CONVERT(DATE,v.DataAvisoPrevio)\n        OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY",
            )
        
        # Log da query completa para debug/teste no SSMS
        print("\n" + "="*80)
        print("[ETAPA 2] QUERY SQL COMPLETA PARA TESTE NO SSMS")
        print("="*80)
        # Substituir placeholders pelos valores reais para facilitar teste
        query_for_log = sql_query
        if query_params:
            for i, param in enumerate(query_params):
                if isinstance(param, (int, float)):
                    query_for_log = query_for_log.replace('?', str(param), 1)
                else:
                    query_for_log = query_for_log.replace('?', f"'{param}'", 1)
        print(query_for_log)
        print("="*80 + "\n")
        logger.info(f"[ETAPA 2] Query SQL completa: {sql_query}")
        logger.info(f"[ETAPA 2] Parâmetros: {query_params}")
        
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
            # Buscar contract_id que serão deletados (baseado nos IdOrcamento filtrados)
            id_orcamento_to_delete = list(set([row[0] for row in all_rows]))  # IdOrcamento únicos
            if id_orcamento_to_delete:
                print("\n[ETAPA 2] Limpando registros filtrados da tabela contract_scenarios...")
                # Buscar contract_id correspondentes aos IdOrcamento
                contract_ids_to_delete = []
                for id_orc in id_orcamento_to_delete:
                    if id_orc in self.contract_id_map:
                        contract_ids_to_delete.append(str(self.contract_id_map[id_orc]))
                if contract_ids_to_delete:
                    try:
                        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
                        cursor_pg = conn_pg.cursor()
                        placeholders = ','.join(['%s' for _ in contract_ids_to_delete])
                        delete_query = f"DELETE FROM {schema}.contract_scenarios WHERE contract_id IN ({placeholders})"
                        cursor_pg.execute(delete_query, contract_ids_to_delete)
                        deleted_count = cursor_pg.rowcount
                        conn_pg.commit()
                        cursor_pg.close()
                        conn_pg.close()
                        print(f"[ETAPA 2] {deleted_count} registros deletados de contract_scenarios")
                        logger.info(f"[ETAPA 2] {deleted_count} registros deletados de contract_scenarios")
                    except Exception as e:
                        logger.error(f"[ETAPA 2] Erro ao deletar contract_scenarios: {e}")
                        print(f"[ETAPA 2] Erro ao deletar contract_scenarios: {e}")
        
        print(f"[ETAPA 2] {len(all_rows)} registros carregados. Processando conversões em massa (vetorizado)...")
        
        # Processar conversões em massa usando DataFrame (vetorizado)
        try:
            df = pd.DataFrame.from_records(all_rows, columns=[
                'IdOrcamento', 'Frequencia', 'Horas', 'ValorNegociado', 'hour_value_calc',
                'DataInicioOperacao', 'DataAvisoPrevio',
                'NomeTarefa', 'NomeCliente', 'StatusPedido', 'DataInclusaoOrcamentoLojas',
                'DataAlteracaoOrcamentoLojas', 'IdTarefa', 'IdCliente'
            ])
            
            # ⚠️ IMPORTANTE: Garantir que apenas CENÁRIOS ÚNICOS sejam processados
            # Fazer DISTINCT no DataFrame baseado na combinação única (mesma lógica do SQL)
            # Isso garante que mesmo se o SQL DISTINCT não funcionar perfeitamente, teremos apenas únicos
            print(f"[ETAPA 2] Registros carregados do SQL: {len(df)}")
            print(f"[ETAPA 2] Aplicando DISTINCT no DataFrame para garantir cenários únicos...")
            logger.info(f"[ETAPA 2] Registros carregados do SQL: {len(df)}")
            
            # Colunas para fazer DISTINCT (combinação única)
            distinct_cols = [
                'IdOrcamento', 'Frequencia', 'Horas', 'ValorNegociado', 'hour_value_calc',
                'DataInicioOperacao', 'DataAvisoPrevio',
            ]
            
            # Normalizar valores antes de fazer DISTINCT para garantir match correto
            # Criar colunas temporárias normalizadas para comparação (no próprio df)
            total_before = len(df)
            for col in distinct_cols:
                if col in ['DataInicioOperacao', 'DataAvisoPrevio']:
                    # Para datas, converter para string no formato YYYY-MM-DD (tratando NULL)
                    df[f'{col}_norm'] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                    df[f'{col}_norm'] = df[f'{col}_norm'].fillna('')
                elif col == 'ValorNegociado':
                    df[f'{col}_norm'] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype(float).astype(str)
                elif col == 'hour_value_calc':
                    df[f'{col}_norm'] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype(float).astype(str)
                elif col == 'Horas':
                    # Para Horas, converter para string (pode vir como nvarchar)
                    df[f'{col}_norm'] = df[col].astype(str).fillna('')
                else:
                    # Para outros campos, converter para string
                    df[f'{col}_norm'] = df[col].astype(str).fillna('')
            
            # Criar lista de colunas normalizadas para DISTINCT
            distinct_cols_norm = [f'{col}_norm' for col in distinct_cols]
            
            # Fazer DISTINCT baseado nas colunas normalizadas da combinação única
            # Manter o primeiro registro de cada combinação única
            df = df.drop_duplicates(subset=distinct_cols_norm, keep='first')
            
            # Remover colunas temporárias normalizadas
            df = df.drop(columns=distinct_cols_norm, errors='ignore')
            
            print(f"[ETAPA 2] Registros únicos após DISTINCT: {len(df)} (de {total_before} carregados)")
            logger.info(f"[ETAPA 2] Registros únicos após DISTINCT: {len(df)} (de {total_before} carregados)")
            
            # ⚠️ IMPORTANTE: Criar cópia do DataFrame original ANTES das conversões para criar o mapa depois
            df_original = df.copy()
            
            # Log dos tipos de dados inferidos pelo pandas
            print(f"[ETAPA 2] Tipos de dados inferidos pelo pandas:")
            print(f"  IdTarefa: {df['IdTarefa'].dtype} (NULLs: {df['IdTarefa'].isna().sum()})")
            print(f"  Frequencia: {df['Frequencia'].dtype} (NULLs: {df['Frequencia'].isna().sum()})")
            print(f"  ValorNegociado: {df['ValorNegociado'].dtype} (NULLs: {df['ValorNegociado'].isna().sum()})")
            print(f"  hour_value_calc: {df['hour_value_calc'].dtype} (NULLs: {df['hour_value_calc'].isna().sum()})")
            print(f"  StatusPedido: {df['StatusPedido'].dtype} (NULLs: {df['StatusPedido'].isna().sum()})")
            print(f"  DataAvisoPrevio: {df['DataAvisoPrevio'].dtype} (NULLs: {df['DataAvisoPrevio'].isna().sum()})")
            logger.info(
                f"[ETAPA 2] Tipos inferidos - IdTarefa: {df['IdTarefa'].dtype}, Frequencia: {df['Frequencia'].dtype}, "
                f"ValorNegociado/hour_value_calc: {df['ValorNegociado'].dtype}, StatusPedido: {df['StatusPedido'].dtype}, "
                f"DataAvisoPrevio: {df['DataAvisoPrevio'].dtype}"
            )
        except Exception as e:
            logger.error(f"[ETAPA 2] ERRO ao criar DataFrame: {e}")
            print(f"[ETAPA 2] ERRO ao criar DataFrame: {e}")
            raise
        
        # Aplicar transformações vetorizadas
        # ⚠️ IMPORTANTE: legacy_id sempre preenchido com 0 (conforme dictionary)
        df['legacy_id'] = 0
        
        # Mapear contract_id usando .map() (vetorizado)
        # ⚠️ IMPORTANTE: Não usar store_id mais (foi removido da tabela)
        df['contract_id'] = df['IdOrcamento'].map(self.contract_id_map)
        
        # Filtrar linhas onde contract_id é None (coletar warnings em batch)
        mask_valid = df['contract_id'].notna()
        missing_contracts = df[~mask_valid & df['contract_id'].isna()]['IdOrcamento'].unique()
        
        if len(missing_contracts) > 0:
            logger.warning(f"Contract nao encontrado para {len(missing_contracts)} IdOrcamento(s): {missing_contracts[:10].tolist()}{'...' if len(missing_contracts) > 10 else ''}")
        
        # Filtrar apenas linhas válidas
        df = df[mask_valid].copy()
        
        # Preparar promoter_task_id usando função compartilhada (mesma lógica do step9)
        print("[ETAPA 2] Normalizando e mapeando promoter_task_id usando função compartilhada...")
        logger.info("[ETAPA 2] Normalizando promoter_task names usando função compartilhada")
        
        # Usar função compartilhada normalize_promoter_task_name para garantir consistência com step9
        # Verificar se a coluna NomeTarefa existe
        if 'NomeTarefa' not in df.columns:
            logger.error(f"Coluna 'NomeTarefa' não encontrada no DataFrame. Colunas disponíveis: {df.columns.tolist()}")
            raise KeyError(f"Coluna 'NomeTarefa' não encontrada no DataFrame")
        
        # Aplicar função compartilhada normalize_promoter_task_name para cada linha
        # Criar colunas 'name' e 'nome_tarefa_clean' diretamente
        try:
            # Criar listas para armazenar os resultados
            names_list = []
            nome_tarefa_clean_list = []
            
            # Aplicar função para cada valor de NomeTarefa
            for idx, nome_tarefa in df['NomeTarefa'].items():
                try:
                    nome_tarefa_val = nome_tarefa if pd.notna(nome_tarefa) else None
                    name, nome_tarefa_clean = normalize_promoter_task_name(nome_tarefa_val)
                    names_list.append(name)
                    nome_tarefa_clean_list.append(nome_tarefa_clean)
                except Exception as e:
                    logger.error(f"Erro ao normalizar nome_tarefa na linha {idx} ('{nome_tarefa}'): {e}")
                    names_list.append(None)
                    nome_tarefa_clean_list.append(None)
            
            # Criar colunas no DataFrame
            df['name'] = names_list
            df['nome_tarefa_clean'] = nome_tarefa_clean_list
            
            # Verificar se as colunas foram criadas
            if 'name' not in df.columns:
                logger.error(f"Coluna 'name' não foi criada. Colunas disponíveis: {df.columns.tolist()}")
                raise KeyError("Coluna 'name' não foi criada")
            if 'nome_tarefa_clean' not in df.columns:
                logger.error(f"Coluna 'nome_tarefa_clean' não foi criada. Colunas disponíveis: {df.columns.tolist()}")
                raise KeyError("Coluna 'nome_tarefa_clean' não foi criada")
                
        except KeyError as e:
            logger.error(f"Erro KeyError ao aplicar normalização: {e}")
            raise
        except Exception as e:
            logger.error(f"Erro ao aplicar normalização: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
        
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
        
        # Garantir que não há valores None antes de converter para string
        if df['promoter_task_id'].isna().any():
            remaining_none = df['promoter_task_id'].isna().sum()
            logger.warning(f"[ETAPA 2] Ainda há {remaining_none} valores None após preenchimento. Preenchendo com tarefa padrão...")
            df.loc[df['promoter_task_id'].isna(), 'promoter_task_id'] = default_task_id
        
        # Converter para string (garantindo que todos os valores são válidos)
        df['promoter_task_id'] = df['promoter_task_id'].astype(str)
        
        # Remover colunas temporárias
        df = df.drop(columns=['name', 'nome_tarefa_clean'], errors='ignore')
        
        print(f"[ETAPA 2] Mapeamento de promoter_task_id concluído. {missing_count} registros usaram tarefa padrão.")
        logger.info(f"[ETAPA 2] Mapeamento concluído: {missing_count} registros usaram tarefa padrão de {len(df)} total")
        
        # Verificar se há valores None (erro crítico - não deveria acontecer)
        if df['promoter_task_id'].isna().any():
            missing_count = df['promoter_task_id'].isna().sum()
            logger.error(f"[ETAPA 2] ERRO CRÍTICO: {missing_count} registros sem promoter_task_id válido mesmo com tarefa padrão")
            print(f"[ETAPA 2] ERRO CRÍTICO: {missing_count} registros sem promoter_task_id válido")
            raise Exception(f"Não foi possível atribuir promoter_task_id para {missing_count} registros")
        
        # Mapear frequency de inteiro para varchar (conversão em lote vetorizada)
        # Mapa de frequência incluindo valores decimais
        frequency_map = {
            # Valores inteiros
            1: 'once_per_week',
            2: 'twice_per_week',
            3: 'three_times_per_week',
            4: 'four_times_per_week',
            5: 'five_times_per_week',
            6: 'six_times_per_week',
            7: 'seven_times_per_week',
            15: 'every_15_days',
            30: 'once_per_month',
            # Valores decimais
            0.25: 'once_per_month',
            0.5: 'every_15_days',
            1.5: 'once_per_week',
            2.5: 'twice_per_week'
        }
        # Converter Frequencia mantendo valores decimais quando necessário
        try:
            print("[ETAPA 2] Convertendo Frequencia...")
            # Converter para numérico primeiro, tratar NaN
            frequencia_numeric = pd.to_numeric(df['Frequencia'], errors='coerce')
            print(f"[ETAPA 2] Frequencia após to_numeric: tipo={frequencia_numeric.dtype}, NULLs={frequencia_numeric.isna().sum()}")
            
            # Mapear diretamente os valores numéricos (incluindo decimais) para o frequency_map
            # Usar função que tenta mapear o valor exato primeiro, depois tenta como inteiro arredondado
            def map_frequency(value):
                if pd.isna(value):
                    return pd.NA
                try:
                    value_float = float(value)
                    # Tentar mapear o valor exato primeiro (inclui decimais)
                    if value_float in frequency_map:
                        return frequency_map[value_float]
                    # Se não encontrar, tentar como inteiro arredondado
                    value_int = int(round(value_float))
                    if value_int in frequency_map:
                        return frequency_map[value_int]
                    # Se não encontrar, retornar None
                    return None
                except (ValueError, TypeError):
                    return None
            
            df['frequency'] = frequencia_numeric.apply(map_frequency)
            # Preencher valores não mapeados com string vazia
            df['frequency'] = df['frequency'].fillna('')
            
            # Log de valores únicos mapeados
            valores_unicos = frequencia_numeric.dropna().unique()
            print(f"[ETAPA 2] Valores únicos de Frequencia encontrados: {sorted(valores_unicos)}")
            print(f"[ETAPA 2] Frequencia após conversão: tipo={df['frequency'].dtype}, NULLs/vazios={(df['frequency'] == '').sum()}")
            print("[ETAPA 2] Frequencia convertida com sucesso")
        except Exception as e:
            logger.error(f"[ETAPA 2] ERRO ao converter Frequencia: {e}")
            print(f"[ETAPA 2] ERRO ao converter Frequencia: {e}")
            print(f"[ETAPA 2] Tipo original: {df['Frequencia'].dtype}")
            print(f"[ETAPA 2] Valores únicos (primeiros 10): {df['Frequencia'].unique()[:10]}")
            raise
        df['hours'] = self.convert_hours_to_float_vectorized(df['Horas'])
        
        # hour_value: Round(ValorNegociado / (Frequencia * Horas * 4), 2) — contract_dictionary
        try:
            print("[ETAPA 2] Calculando hour_value (ValorNegociado / (Frequencia*Horas*4), 2 dec.)...")
            df['hour_value'] = df.apply(
                lambda r: compute_scenario_hour_value(
                    r['ValorNegociado'], r['Frequencia'], r['Horas']
                ),
                axis=1,
            ).astype(float)
            print(f"[ETAPA 2] hour_value OK: tipo={df['hour_value'].dtype}, NULLs={df['hour_value'].isna().sum()}")
            logger.info(f"[ETAPA 2] hour_value - tipo: {df['hour_value'].dtype}, NULLs: {df['hour_value'].isna().sum()}")
        except Exception as e:
            logger.error(f"[ETAPA 2] ERRO ao calcular hour_value: {e}")
            print(f"[ETAPA 2] ERRO ao calcular hour_value: {e}")
            df['hour_value'] = 0.0
            print("[ETAPA 2] Usando valor padrão 0.0 para hour_value devido ao erro")
        
        df['start_date'] = df['DataInicioOperacao'].fillna(df['DataInclusaoOrcamentoLojas'])
        df.loc[df['start_date'].isna(), 'start_date'] = datetime.now()
        
        # ⚠️ IMPORTANTE: end_date mapeado de DataAvisoPrevio (pode ser NULL)
        df['end_date'] = pd.to_datetime(df['DataAvisoPrevio'], errors='coerce')
        # Converter para date apenas onde não é NaT/None
        mask_not_null = df['end_date'].notna()
        df.loc[mask_not_null, 'end_date'] = df.loc[mask_not_null, 'end_date'].dt.date
        df.loc[~mask_not_null, 'end_date'] = None
        
        # Status: usar StatusPedido (conforme dictionary: mapear StatusPedido ou Ativo para status)
        # Converter status de inteiro: 11 → 0 (closed), 0 → 0 (inactive), qualquer outro → 1 (active)
        try:
            print("[ETAPA 2] Convertendo StatusPedido...")
            status_pedido_numeric = pd.to_numeric(df['StatusPedido'], errors='coerce')
            print(f"[ETAPA 2] StatusPedido após to_numeric: tipo={status_pedido_numeric.dtype}, NULLs={status_pedido_numeric.isna().sum()}")
            status_temp = status_pedido_numeric.fillna(0)
            # Converter para int usando apply para evitar problemas de conversão direta
            status_temp = status_temp.apply(lambda x: int(x) if pd.notna(x) else 0)
            print(f"[ETAPA 2] StatusPedido convertido com sucesso: tipo={status_temp.dtype}")
        except Exception as e:
            logger.error(f"[ETAPA 2] ERRO ao converter StatusPedido: {e}")
            print(f"[ETAPA 2] ERRO ao converter StatusPedido: {e}")
            print(f"[ETAPA 2] Tipo original: {df['StatusPedido'].dtype}")
            print(f"[ETAPA 2] Valores únicos: {df['StatusPedido'].unique()}")
            # Em caso de erro, usar valor padrão 'active' (1)
            status_temp = pd.Series(1, index=df.index, dtype=int)
            print("[ETAPA 2] Usando valor padrão 'active' (1) para status devido ao erro")
        # Mapear em lote vetorizado: 1 para 'active', 0 para qualquer outro valor (int)
        df['status'] = pd.Series(1, index=df.index, dtype=int)  # 1 = active por padrão
        # Se status_temp == 11 (closed) OU status_temp == 0 (inactive) → status = 0
        df.loc[(status_temp == 11) | (status_temp == 0), 'status'] = 0
        
        df['created_at'] = df['DataInclusaoOrcamentoLojas'].fillna(datetime.now())
        df['updated_at'] = df['DataAlteracaoOrcamentoLojas'].fillna(df['created_at'])
        
        # Converter UUIDs para string
        df['contract_id'] = df['contract_id'].astype(str)
        df['promoter_task_id'] = df['promoter_task_id'].astype(str)
        
        # Filtrar apenas rows com billing_id (contract_billing_map do placeholder - BLOCO TEMPORÁRIO)
        df = df[df['contract_id'].isin(self.contract_billing_map)].copy()
        df_original = df_original.loc[df.index].copy() if len(df) > 0 else df_original.iloc[0:0]
        if len(df) == 0:
            print("[ETAPA 2] AVISO: Nenhum registro com billing_id. Verifique contract_billing_map.")
            logger.warning("[ETAPA 2] Nenhum registro com billing_id após filtro")
            return
        
        # Converter end_date (pode ser None) para lista tratando NaT/NaN
        def convert_nat_to_none(val):
            if val is None:
                return None
            if pd.isna(val) or val is pd.NaT or str(val) == 'NaT':
                return None
            # Se for objeto date do Python, manter como está
            return val
        
        end_date_list = [convert_nat_to_none(x) for x in df['end_date']]
        
        # Converter DataFrame diretamente para lista de tuplas (otimizado)
        # ⚠️ IMPORTANTE: estrutura atual de commercial.contract_scenarios (PRD):
        # id, legacy_id, contract_id, promoter_task_id, frequency, hours, hour_value,
        # start_date, end_date, status, deleted_at, expected_amount, created_at, updated_at, billing_id
        # billing_id do contract_billing_map (placeholder - BLOCO TEMPORÁRIO)
        expected_amount_series = df['hours'] * df['hour_value']
        expected_amount_series = expected_amount_series.fillna(0.0)
        expected_amount_list = expected_amount_series.tolist()
        billing_id_list = [self.contract_billing_map[cid] for cid in df['contract_id']]
        if include_legacy:
            processed_tuples = list(zip(
                df['legacy_id'].tolist(),
                df['contract_id'].tolist(),
                df['promoter_task_id'].tolist(),
                df['frequency'].tolist(),
                df['hours'].tolist(),
                df['hour_value'].tolist(),
                df['start_date'].tolist(),
                end_date_list,
                df['status'].tolist(),
                [None] * len(df),            # deleted_at
                expected_amount_list,        # expected_amount
                df['created_at'].tolist(),
                df['updated_at'].tolist(),
                billing_id_list              # billing_id (um por customer - BLOCO TEMPORÁRIO)
            ))
        else:
            processed_tuples = list(zip(
                df['contract_id'].tolist(),
                df['promoter_task_id'].tolist(),
                df['frequency'].tolist(),
                df['hours'].tolist(),
                df['hour_value'].tolist(),
                df['start_date'].tolist(),
                end_date_list,
                df['status'].tolist(),
                [None] * len(df),            # deleted_at
                expected_amount_list,        # expected_amount
                df['created_at'].tolist(),
                df['updated_at'].tolist(),
                billing_id_list              # billing_id (um por customer - BLOCO TEMPORÁRIO)
            ))
        legacy_ids_list = df['legacy_id'].tolist()
        
        print(f"[ETAPA 2] {len(processed_tuples)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL e inserir em bulk
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Query de insert usando gen_random_uuid() - formato para execute_values
        # ⚠️ IMPORTANTE: estrutura atual de commercial.contract_scenarios (PRD):
        # id, legacy_id, contract_id, promoter_task_id, frequency, hours, hour_value,
        # start_date, end_date, status, deleted_at, expected_amount, created_at, updated_at, billing_id
        if include_legacy:
            insert_query = f"""
            INSERT INTO {schema}.contract_scenarios (
                id, legacy_id, contract_id, promoter_task_id, frequency, hours, hour_value,
                start_date, end_date, status, deleted_at, expected_amount, created_at, updated_at, billing_id
            ) VALUES %s
            """
            insert_template = (
                "(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s, %s)"
            )
        else:
            insert_query = f"""
            INSERT INTO {schema}.contract_scenarios (
                id, contract_id, promoter_task_id, frequency, hours, hour_value,
                start_date, end_date, status, deleted_at, expected_amount, created_at, updated_at, billing_id
            ) VALUES %s
            """
            insert_template = (
                "(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s)"
            )
        
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
            
            # ⚠️ IMPORTANTE: Criar mapa de cenários baseado na combinação única
            # Chave: (IdOrcamento, Frequencia, Horas, hour_value calculado, DataInicioOperacao, DataAvisoPrevio)
            # Valor: UUID do scenario
            # ⚠️ IMPORTANTE: Usar os valores brutos do DataFrame original (mesma normalização do step3)
            print(f"[ETAPA 2] Criando mapa de cenários baseado na combinação única...")
            logger.info(f"[ETAPA 2] Criando mapa de cenários para step3...")
            
            # Função auxiliar para normalizar frequência de forma consistente (usada em step2 e step3)
            def normalize_frequency(freq_value):
                """
                Normaliza frequência para string de forma consistente.
                Garante que valores decimais (0.25, 0.5, 1.5, 2.5) tenham formatação consistente.
                Usada tanto na criação de chaves quanto na reconstrução a partir do banco.
                """
                # Verificar se é None, NaN ou string vazia
                if freq_value is None or freq_value == '':
                    return ''
                try:
                    # Tentar usar pd.isna() se for valor do pandas, senão verificar None diretamente
                    if hasattr(freq_value, '__iter__') and not isinstance(freq_value, str):
                        if pd.isna(freq_value):
                            return ''
                    freq_float = float(freq_value)
                    # Usar formatação controlada para evitar problemas de precisão
                    # Para valores decimais conhecidos, garantir formato exato
                    if freq_float == 0.25:
                        return '0.25'
                    elif freq_float == 0.5:
                        return '0.5'
                    elif freq_float == 1.5:
                        return '1.5'
                    elif freq_float == 2.5:
                        return '2.5'
                    else:
                        # Para outros valores, converter normalmente
                        return str(int(freq_float)) if freq_float.is_integer() else str(freq_float)
                except (ValueError, TypeError):
                    return str(freq_value) if freq_value else ''
            
            # Criar chave única para cada linha do DataFrame original (valores brutos)
            def create_scenario_key_from_df(row):
                """Cria chave única baseada nos valores brutos (mesma normalização do step3)"""
                # Normalização idêntica ao step3
                # ⚠️ IMPORTANTE: Usar função auxiliar para normalizar Frequencia de forma consistente
                freq_str = normalize_frequency(row['Frequencia'])
                # ⚠️ CRÍTICO: Converter Horas para float primeiro, depois para string (mesmo do step3)
                # Isso garante que '1' e 1.0 virem ambos '1.0'
                try:
                    horas_float = float(row['Horas']) if pd.notna(row['Horas']) else 0.0
                    horas_str = str(horas_float)
                except (ValueError, TypeError):
                    horas_str = ''
                hv = compute_scenario_hour_value(
                    row['ValorNegociado'], row['Frequencia'], row['Horas']
                )
                valor_hora_str = format_hour_value_key(hv)
                start_date_str = row['DataInicioOperacao'].strftime('%Y-%m-%d') if pd.notna(row['DataInicioOperacao']) else ''
                # ⚠️ IMPORTANTE: end_date pode ser None (mesmo do step3)
                end_date_str = row['DataAvisoPrevio'].strftime('%Y-%m-%d') if pd.notna(row['DataAvisoPrevio']) else None
                
                return (
                    int(row['IdOrcamento']),
                    freq_str,
                    horas_str,
                    valor_hora_str,
                    start_date_str,
                    end_date_str
                )
            
            # Criar chaves únicas usando o DataFrame original (valores brutos)
            scenario_keys_list = df_original.apply(create_scenario_key_from_df, axis=1).tolist()
            
            print(f"[ETAPA 2] Criando mapa de scenarios: {len(scenario_keys_list)} chaves únicas")
            logger.info(f"[ETAPA 2] Criando mapa de scenarios: {len(scenario_keys_list)} chaves únicas")
            
            # ⚠️ IMPORTANTE: Buscar UUIDs do banco usando os valores reais para garantir correspondência exata
            # Em vez de confiar apenas na ordem, vamos buscar cada scenario individualmente usando os valores
            # Primeiro, vamos buscar todos os scenarios recém-inseridos ordenados por created_at
            
            # Mapa reverso: frequency enum -> lista de valores possíveis (inteiros e decimais como string)
            # Um enum pode ter múltiplos valores de origem (ex: once_per_week pode vir de 1 ou 1.5)
            frequency_enum_to_values = {
                'once_per_week': ['1', '1.5'],
                'twice_per_week': ['2', '2.5'],
                'three_times_per_week': ['3'],
                'four_times_per_week': ['4'],
                'five_times_per_week': ['5'],
                'six_times_per_week': ['6'],
                'seven_times_per_week': ['7'],
                'every_15_days': ['15', '0.5'],
                'once_per_month': ['30', '0.25']
            }
            
            cursor_pg.execute(f"""
                SELECT 
                    id,
                    frequency,
                    hours,
                    hour_value,
                    start_date,
                    end_date,
                    contract_id
                FROM {schema}.contract_scenarios
                ORDER BY created_at DESC
                LIMIT %s
            """, (len(scenario_keys_list),))
            
            scenario_rows = cursor_pg.fetchall()
            
            # Criar mapa reverso: contract_id -> IdOrcamento
            contract_to_id_orcamento = {}
            for id_orc, contract_uuid in self.contract_id_map.items():
                contract_to_id_orcamento[str(contract_uuid)] = id_orc
            
            # Mapear cada scenario do banco para sua chave única
            scenarios_mapped = 0
            for scenario_uuid, frequency, hours, hour_value, start_date, end_date, contract_uuid in scenario_rows:
                # Buscar IdOrcamento do contract_id
                contract_uuid_str = str(contract_uuid) if contract_uuid else None
                id_orcamento = contract_to_id_orcamento.get(contract_uuid_str)
                
                if not id_orcamento:
                    logger.warning(f"[ETAPA 2] Nao foi possivel encontrar IdOrcamento para contract_id={contract_uuid_str}")
                    continue
                
                # Criar chave única baseada nos valores do banco (mesma normalização do create_scenario_key_from_df)
                # ⚠️ IMPORTANTE: Converter frequency de enum para lista de valores possíveis
                # Um enum pode ter múltiplos valores de origem (ex: once_per_week pode vir de 1 ou 1.5)
                freq_enum_str = str(frequency) if frequency else ''
                freq_values_list = frequency_enum_to_values.get(freq_enum_str, [freq_enum_str])
                
                # ⚠️ CRÍTICO: Converter hours para float primeiro, depois para string (mesmo do create_scenario_key_from_df)
                # Isso garante que 1.0 vire '1.0' (mesmo formato que será usado no step3)
                try:
                    horas_float = float(hours) if hours is not None else 0.0
                    horas_str = str(horas_float)
                except (ValueError, TypeError):
                    horas_str = ''
                valor_hora_str = format_hour_value_key(hour_value)
                start_date_str = start_date.strftime('%Y-%m-%d') if start_date else ''
                # ⚠️ IMPORTANTE: end_date pode ser None (mesmo do create_scenario_key_from_df)
                end_date_str = end_date.strftime('%Y-%m-%d') if end_date else None
                
                # Tentar mapear para cada valor de frequência possível
                # Se encontrar match com qualquer valor, adicionar ao mapa
                # ⚠️ IMPORTANTE: Normalizar cada valor da lista usando a mesma função de normalização
                mapped = False
                for freq_value in freq_values_list:
                    # Normalizar frequência usando a mesma função auxiliar
                    freq_str_normalized = normalize_frequency(freq_value)
                    scenario_key = (
                        int(id_orcamento),
                        freq_str_normalized,
                        horas_str,
                        valor_hora_str,
                        start_date_str,
                        end_date_str
                    )
                    
                    # Verificar se esta chave está na lista de chaves esperadas
                    if scenario_key in scenario_keys_list:
                        self.scenario_id_map[scenario_key] = str(scenario_uuid)
                        scenarios_mapped += 1
                        mapped = True
                        break  # Encontrou match, não precisa tentar outros valores
                
                if not mapped:
                    logger.warning(
                        f"[ETAPA 2] Scenario do banco nao corresponde a nenhuma chave esperada: "
                        f"scenario_id={scenario_uuid}, IdOrcamento={id_orcamento}, "
                        f"frequency={freq_enum_str}, freq_values_list={freq_values_list}, hours={horas_str}, hour_value={valor_hora_str}, "
                        f"start_date={start_date_str}, end_date={end_date_str}"
                    )
            
            print(f"[ETAPA 2] {scenarios_mapped} cenários mapeados para step3 (de {len(scenario_keys_list)} esperados)")
            logger.info(f"[ETAPA 2] {scenarios_mapped} cenários mapeados no scenario_id_map (de {len(scenario_keys_list)} esperados)")
            
            # Verificar se há chaves não mapeadas
            unmapped_keys = [key for key in scenario_keys_list if key not in self.scenario_id_map]
            if unmapped_keys:
                logger.warning(f"[ETAPA 2] AVISO: {len(unmapped_keys)} chaves nao foram mapeadas para scenarios")
                print(f"[ETAPA 2] AVISO: {len(unmapped_keys)} chaves nao foram mapeadas para scenarios")
                # Logar algumas chaves não mapeadas para debug
                for key in unmapped_keys[:5]:
                    logger.warning(f"[ETAPA 2] Chave nao mapeada: {key}")
            
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
            
            # Construir WHERE clause com filtros opcionais (mesmo padrão do step3)
            where_conditions = []
            query_params = []
            
            if id_orcamento_list:
                placeholders = ','.join(['?' for _ in id_orcamento_list])
                where_conditions.append(f"v.IdOrcamento IN ({placeholders})")
                query_params.extend(id_orcamento_list)
            
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
            
            # Filtro StatusPedido
            if len(self.status_pedido_filter) > 0:
                placeholders = ','.join(['?' for _ in self.status_pedido_filter])
                where_conditions.append(f"v.StatusPedido IN ({placeholders})")
                query_params.extend(self.status_pedido_filter)
            
            # Filtro IdCliente (obrigatório conforme query base)
            where_conditions.append("v.IdCliente IS NOT NULL")
            
            # Contar origem: todos os registros de ViewOrcamentosLojas (não DISTINCT, pois cada registro vira uma linha em contract_scenario_stores)
            if where_conditions:
                count_query = f"""
                SELECT COUNT(*)
                FROM ViewOrcamentosLojas v
                INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
                WHERE {' AND '.join(where_conditions)}
                """
                cursor_sql.execute(count_query, query_params)
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
        # ⚠️ IMPORTANTE: scenario_id_map já foi criado no step2 baseado na combinação única
        # Se não existe, criar agora buscando do banco
        print("[ETAPA 3] Verificando mapeamento de scenarios...")
        if not self.scenario_id_map:
            logger.warning("[ETAPA 3] scenario_id_map vazio. Buscando cenários do banco...")
            try:
                schema_scenarios = schema
                # ⚠️ CRÍTICO: Usar conexão PRD diretamente quando destino for PRD
                if destino == 'PRD':
                    conn_scenarios = DatabaseConnection.get_postgresql_prd_destino_connection()
                else:
                    conn_scenarios = DatabaseConnection.get_postgresql_hml_destino_connection()
                cursor_scenarios = conn_scenarios.cursor()
                
                # Buscar cenários com suas combinações únicas
                cursor_scenarios.execute(f"""
                    SELECT 
                        id,
                        contract_id,
                        frequency,
                        hours,
                        hour_value,
                        start_date,
                        end_date
                    FROM {schema_scenarios}.contract_scenarios
                """)
                
                # Criar mapa: buscar contract_id -> IdOrcamento primeiro
                # ⚠️ IMPORTANTE: contract_id_map armazena UUID como string, mas pode vir como UUID do banco
                contract_to_id_orcamento = {}
                for id_orc, contract_uuid in self.contract_id_map.items():
                    # Garantir que ambos sejam strings para comparação
                    contract_to_id_orcamento[str(contract_uuid)] = id_orc
                
                scenario_rows = cursor_scenarios.fetchall()
                for scenario_uuid, contract_uuid, frequency, hours, hour_value, start_date, end_date in scenario_rows:
                    # Buscar IdOrcamento do contract_id (contract_uuid pode vir como UUID ou string)
                    contract_uuid_str = str(contract_uuid) if contract_uuid else None
                    id_orcamento = contract_to_id_orcamento.get(contract_uuid_str)
                    if id_orcamento:
                        # Criar chave única baseada na combinação
                        freq_str = str(frequency) if frequency else ''
                        horas_str = str(hours) if hours else ''
                        valor_hora_str = format_hour_value_key(hour_value)
                        start_date_str = start_date.strftime('%Y-%m-%d') if start_date else ''
                        end_date_str = end_date.strftime('%Y-%m-%d') if end_date else None
                        
                        scenario_key = (
                            int(id_orcamento),
                            freq_str,
                            horas_str,
                            valor_hora_str,
                            start_date_str,
                            end_date_str
                        )
                        self.scenario_id_map[scenario_key] = str(scenario_uuid)
                
                cursor_scenarios.close()
                conn_scenarios.close()
                print(f"OK - {len(self.scenario_id_map)} scenarios carregados")
                logger.info(f"Carregados {len(self.scenario_id_map)} scenarios para mapeamento")
            except Exception as e:
                logger.warning(f"Erro ao carregar scenarios: {e}")
                print(f"AVISO - Nao foi possivel carregar scenarios: {e}")
        else:
            print(f"OK - {len(self.scenario_id_map)} scenarios já mapeados do step2")
            logger.info(f"Usando {len(self.scenario_id_map)} scenarios já mapeados do step2")
        
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
        
        try:
            id_orcamento_filter_list = self._resolve_id_orcamento_list_for_steps()
        except ValueError as e:
            logger.error(f"[ETAPA 3] {e}")
            print(f"ERRO - {e}")
            raise
        logger.info(f"[ETAPA 3] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")
        print(f"[ETAPA 3] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")
        
        # Limpar tabela (TRUNCATE ou DELETE baseado em filtros)
        if not id_orcamento_filter_list or self.clear_data:
            print("\n[ETAPA 3] Limpando tabela contract_scenario_stores...")
            self.truncate_table('contract_scenario_stores')
        else:
            print("\n[ETAPA 3] Preparando limpeza de contract_scenario_stores...")
        
        # Buscar dados do SQL Server usando ViewOrcamentosLojas + OrcamentoLojas
        # ⚠️ IMPORTANTE: Precisamos buscar todos os campos para criar a chave única e buscar o scenario_id
        print("[ETAPA 3] Buscando dados do SQL Server...")
        sql_query = """
        SELECT 
            v.IdOrcamentoLoja,
            v.IdOrcamento,
            v.IdEstabelecimento,
            v.Frequencia,
            v.Horas,
            v.ValorNegociado,
            v.DataInicioOperacao,
            v.DataAvisoPrevio,
            v.DataInclusaoOrcamentoLojas,
            v.DataAlteracaoOrcamentoLojas,
            ol.Ativo
        FROM ViewOrcamentosLojas v
        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
        LEFT JOIN OrcamentoLojas ol ON ol.Id = v.IdOrcamentoLoja
        """
        
        # Construir WHERE clause com filtros opcionais (mesmo padrão do step1 e step2)
        where_conditions = []
        query_params = []
        
        # Filtro IdOrcamento
        if id_orcamento_filter_list:
            placeholders = ','.join(['?' for _ in id_orcamento_filter_list])
            where_conditions.append(f"v.IdOrcamento IN ({placeholders})")
            query_params.extend(id_orcamento_filter_list)
        
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
        
        # Filtro StatusPedido
        if len(self.status_pedido_filter) > 0:
            placeholders = ','.join(['?' for _ in self.status_pedido_filter])
            where_conditions.append(f"v.StatusPedido IN ({placeholders})")
            query_params.extend(self.status_pedido_filter)
        
        # Filtro IdCliente (obrigatório conforme query base)
        where_conditions.append("v.IdCliente IS NOT NULL")
        
        # Adicionar WHERE se houver condições
        if where_conditions:
            sql_query += " WHERE " + " AND ".join(where_conditions)
        
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
        print("[ETAPA 3] Carregando dados na memória...")
        all_rows = cursor_sql.fetchall()
        cursor_sql.close()
        conn_sql.close()
        
        print(f"[ETAPA 3] {len(all_rows)} registros carregados. Processando conversões em massa (vetorizado)...")
        
        # Processar conversões em massa usando DataFrame (vetorizado)
        df = pd.DataFrame.from_records(all_rows, columns=[
            'IdOrcamentoLoja', 'IdOrcamento', 'IdEstabelecimento', 'Frequencia', 'Horas',
            'ValorNegociado', 'DataInicioOperacao', 'DataAvisoPrevio',
            'DataInclusaoOrcamentoLojas', 'DataAlteracaoOrcamentoLojas', 'Ativo'
        ])
        
        # Aplicar transformações vetorizadas
        df['legacy_id'] = df['IdOrcamentoLoja']
        
        # Função auxiliar para normalizar frequência de forma consistente (mesma do step2)
        def normalize_frequency(freq_value):
            """
            Normaliza frequência para string de forma consistente.
            Garante que valores decimais (0.25, 0.5, 1.5, 2.5) tenham formatação consistente.
            """
            # Verificar se é None, NaN ou string vazia
            if freq_value is None or freq_value == '':
                return ''
            try:
                # Tentar usar pd.isna() se for valor do pandas, senão verificar None diretamente
                if hasattr(freq_value, '__iter__') and not isinstance(freq_value, str):
                    if pd.isna(freq_value):
                        return ''
                freq_float = float(freq_value)
                # Usar formatação controlada para evitar problemas de precisão
                # Para valores decimais conhecidos, garantir formato exato
                if freq_float == 0.25:
                    return '0.25'
                elif freq_float == 0.5:
                    return '0.5'
                elif freq_float == 1.5:
                    return '1.5'
                elif freq_float == 2.5:
                    return '2.5'
                else:
                    # Para outros valores, converter normalmente
                    return str(int(freq_float)) if freq_float.is_integer() else str(freq_float)
            except (ValueError, TypeError):
                return str(freq_value) if freq_value else ''
        
        # ⚠️ IMPORTANTE: Buscar scenario_id baseado na combinação única
        # Criar chave única para cada registro baseada na combinação
        # ⚠️ CRÍTICO: Usar EXATAMENTE a mesma normalização do step2 (create_scenario_key_from_df)
        def create_scenario_key(row):
            """Cria chave única baseada na combinação de campos (mesma normalização do step2)"""
            # Normalização idêntica ao step2
            # ⚠️ IMPORTANTE: Usar função auxiliar para normalizar Frequencia de forma consistente
            freq_str = normalize_frequency(row['Frequencia'])
            # ⚠️ CRÍTICO: Converter Horas para float primeiro, depois para string (mesmo do step2)
            # Isso garante que '1' e 1.0 virem ambos '1.0'
            try:
                horas_float = float(row['Horas']) if pd.notna(row['Horas']) else 0.0
                horas_str = str(horas_float)
            except (ValueError, TypeError):
                horas_str = ''
            hv = compute_scenario_hour_value(
                row['ValorNegociado'], row['Frequencia'], row['Horas']
            )
            valor_hora_str = format_hour_value_key(hv)
            start_date_str = row['DataInicioOperacao'].strftime('%Y-%m-%d') if pd.notna(row['DataInicioOperacao']) else ''
            # ⚠️ IMPORTANTE: end_date pode ser None (mesmo do step2)
            end_date_str = row['DataAvisoPrevio'].strftime('%Y-%m-%d') if pd.notna(row['DataAvisoPrevio']) else None
            
            return (
                int(row['IdOrcamento']),
                freq_str,
                horas_str,
                valor_hora_str,
                start_date_str,
                end_date_str
            )
        
        # Criar chaves únicas para cada registro
        df['scenario_key'] = df.apply(create_scenario_key, axis=1)
        
        # Log detalhado: mostrar alguns exemplos de scenario_key criados
        if len(df) > 0:
            sample_keys = df['scenario_key'].head(5).tolist()
            print(f"[ETAPA 3] Exemplos de scenario_key criados (primeiros 5): {sample_keys}")
            logger.info(f"[ETAPA 3] Total de scenario_keys únicos criados: {df['scenario_key'].nunique()}")
            logger.info(f"[ETAPA 3] Total de scenario_keys no mapa: {len(self.scenario_id_map)}")
        
        # Mapear scenario_id usando a chave única
        df['scenario_id'] = df['scenario_key'].map(self.scenario_id_map)
        df['store_id'] = df['IdEstabelecimento'].map(self.store_id_map)
        
        # ⚠️ VALIDAÇÃO CRÍTICA: Verificar se o scenario_id encontrado realmente corresponde aos valores
        # Buscar os scenarios do banco para validar correspondência exata
        print("[ETAPA 3] Validando correspondência exata dos scenarios...")
        schema_scenarios = schema
        conn_validate = DatabaseConnection.get_postgresql_destino_connection()
        cursor_validate = conn_validate.cursor()
        
        # Mapa reverso: frequency enum -> lista de valores originais possíveis (inteiros e decimais como string)
        # Usado para comparar com Frequencia da origem durante validação
        # Um enum pode ter múltiplos valores de origem (ex: once_per_week pode vir de 1 ou 1.5)
        frequency_enum_to_values = {
            'once_per_week': ['1', '1.5'],
            'twice_per_week': ['2', '2.5'],
            'three_times_per_week': ['3'],
            'four_times_per_week': ['4'],
            'five_times_per_week': ['5'],
            'six_times_per_week': ['6'],
            'seven_times_per_week': ['7'],
            'every_15_days': ['15', '0.5'],
            'once_per_month': ['30', '0.25']
        }
        
        # Buscar todos os scenarios com seus valores para validação
        # ⚠️ IMPORTANTE: Incluir legacy_id (IdOrcamento) através do JOIN com contracts
        cursor_validate.execute(f"""
            SELECT 
                cs.id,
                cs.frequency,
                cs.hours,
                cs.hour_value,
                cs.start_date,
                cs.end_date,
                c.legacy_id
            FROM {schema_scenarios}.contract_scenarios cs
            INNER JOIN {schema_scenarios}.contracts c ON c.id = cs.contract_id
        """)
        
        scenario_validation_map = {}
        for row in cursor_validate.fetchall():
            scenario_uuid, frequency, hours, hour_value, start_date, end_date, legacy_id = row
            # Normalizar valores para comparação (mesma normalização do create_scenario_key)
            # ⚠️ IMPORTANTE: Converter frequency de enum para lista de valores possíveis (inteiros e decimais)
            # Um enum pode ter múltiplos valores de origem (ex: once_per_week pode vir de 1 ou 1.5)
            freq_enum_str = str(frequency) if frequency else ''
            freq_values_list = frequency_enum_to_values.get(freq_enum_str, [freq_enum_str])  # Lista de valores possíveis
            
            # ⚠️ CRÍTICO: Converter hours para float primeiro, depois para string (mesmo do create_scenario_key)
            # Isso garante que 1.0 vire '1.0' (mesmo formato que será usado na validação)
            try:
                hours_float = float(hours) if hours is not None else 0.0
                hours_val = str(hours_float)
            except (ValueError, TypeError):
                hours_val = ''
            hour_value_val = format_hour_value_key(hour_value)
            start_date_val = start_date.strftime('%Y-%m-%d') if start_date else ''
            # ⚠️ IMPORTANTE: end_date pode ser None (mesmo do create_scenario_key)
            end_date_val = end_date.strftime('%Y-%m-%d') if end_date else None
            
            scenario_validation_map[str(scenario_uuid)] = {
                'legacy_id': legacy_id,  # IdOrcamento para validação
                'frequency': freq_values_list,  # Lista de valores possíveis para comparação flexível
                'hours': hours_val,
                'hour_value': hour_value_val,
                'start_date': start_date_val,
                'end_date': end_date_val
            }
        
        cursor_validate.close()
        conn_validate.close()
        
        # Validar cada registro que tem scenario_id
        def validate_scenario_match(row):
            """Valida se o scenario_id realmente corresponde aos valores do registro"""
            if pd.isna(row['scenario_id']):
                return False
            
            scenario_uuid = str(row['scenario_id'])
            if scenario_uuid not in scenario_validation_map:
                return False
            
            scenario_data = scenario_validation_map[scenario_uuid]
            
            # Normalizar valores do registro para comparação (mesma normalização do create_scenario_key)
            # ⚠️ IMPORTANTE: Usar função auxiliar para normalizar Frequencia de forma consistente
            freq_reg = normalize_frequency(row['Frequencia'])
            # ⚠️ CRÍTICO: Converter Horas para float primeiro, depois para string (mesmo do create_scenario_key)
            # Isso garante que '1' e 1.0 virem ambos '1.0'
            try:
                horas_float_reg = float(row['Horas']) if pd.notna(row['Horas']) else 0.0
                horas_reg = str(horas_float_reg)
            except (ValueError, TypeError):
                horas_reg = ''
            hv_reg = compute_scenario_hour_value(
                row['ValorNegociado'], row['Frequencia'], row['Horas']
            )
            valor_hora_reg = format_hour_value_key(hv_reg)
            start_date_reg = row['DataInicioOperacao'].strftime('%Y-%m-%d') if pd.notna(row['DataInicioOperacao']) else ''
            # ⚠️ IMPORTANTE: end_date pode ser None (mesmo do create_scenario_key)
            end_date_reg = row['DataAvisoPrevio'].strftime('%Y-%m-%d') if pd.notna(row['DataAvisoPrevio']) else None
            
            # Comparar valores normalizados
            # ⚠️ IMPORTANTE: Comparar end_date considerando None e string vazia como equivalentes
            end_date_match = (
                end_date_reg == scenario_data['end_date'] or
                (end_date_reg == '' and scenario_data['end_date'] is None) or
                (end_date_reg is None and scenario_data['end_date'] == '')
            )
            
            # ⚠️ IMPORTANTE: Comparar frequency considerando múltiplos valores possíveis (inteiros e decimais)
            # scenario_data['frequency'] agora é uma lista de valores possíveis
            freq_match = freq_reg in scenario_data['frequency'] if isinstance(scenario_data['frequency'], list) else freq_reg == scenario_data['frequency']
            
            # ⚠️ IMPORTANTE: Validar legacy_id (IdOrcamento) para garantir que o scenario pertence ao mesmo contrato
            legacy_id_match = int(row['IdOrcamento']) == scenario_data['legacy_id'] if scenario_data.get('legacy_id') is not None else False
            
            match = (
                legacy_id_match and
                freq_match and
                horas_reg == scenario_data['hours'] and
                valor_hora_reg == scenario_data['hour_value'] and
                start_date_reg == scenario_data['start_date'] and
                end_date_match
            )
            
            if not match:
                logger.warning(
                    f"[ETAPA 3] VALIDACAO FALHOU para IdOrcamentoLoja={row['IdOrcamentoLoja']}, "
                    f"IdOrcamento={row['IdOrcamento']}, scenario_id={scenario_uuid}. "
                    f"Registro: IdOrcamento={row['IdOrcamento']}, Frequencia={freq_reg}, Horas={horas_reg}, hour_value={valor_hora_reg}, "
                    f"DataInicio={start_date_reg}, DataAviso={end_date_reg}. "
                    f"Scenario: legacy_id={scenario_data.get('legacy_id')}, frequency={scenario_data['frequency']}, hours={scenario_data['hours']}, "
                    f"hour_value={scenario_data['hour_value']}, start_date={scenario_data['start_date']}, "
                    f"end_date={scenario_data['end_date']}"
                )
            
            return match
        
        # Aplicar validação
        df['scenario_valid'] = df.apply(validate_scenario_match, axis=1)
        
        # Filtrar linhas onde scenario_id ou store_id são None OU onde a validação falhou
        mask_valid = df['scenario_id'].notna() & df['store_id'].notna() & df['scenario_valid']
        
        # Coletar informações sobre registros filtrados
        missing_scenarios = df[~mask_valid & df['scenario_id'].isna()]['IdOrcamentoLoja'].unique()
        missing_stores = df[~mask_valid & df['store_id'].isna()]['IdEstabelecimento'].unique()
        invalid_scenarios = df[~mask_valid & df['scenario_valid'] == False]['IdOrcamentoLoja'].unique()
        
        if len(missing_scenarios) > 0:
            logger.warning(f"[ETAPA 3] Scenario nao encontrado para {len(missing_scenarios)} IdOrcamentoLoja(s): {missing_scenarios[:10].tolist()}{'...' if len(missing_scenarios) > 10 else ''}")
            print(f"[ETAPA 3] AVISO: {len(missing_scenarios)} registros sem scenario_id correspondente")
        
        if len(missing_stores) > 0:
            logger.warning(f"[ETAPA 3] Store nao encontrado para {len(missing_stores)} IdEstabelecimento(s): {missing_stores[:10].tolist()}{'...' if len(missing_stores) > 10 else ''}")
            print(f"[ETAPA 3] AVISO: {len(missing_stores)} registros sem store_id correspondente")
        
        if len(invalid_scenarios) > 0:
            logger.warning(f"[ETAPA 3] VALIDACAO FALHOU para {len(invalid_scenarios)} IdOrcamentoLoja(s): {invalid_scenarios[:10].tolist()}{'...' if len(invalid_scenarios) > 10 else ''}")
            print(f"[ETAPA 3] AVISO CRITICO: {len(invalid_scenarios)} registros com scenario_id que nao corresponde aos valores!")
            print(f"[ETAPA 3] Esses registros serao FILTRADOS e nao serao inseridos.")
        
        # Filtrar apenas linhas válidas (com scenario_id, store_id E validação passada)
        df = df[mask_valid].copy()
        
        # Remover coluna temporária de validação
        df = df.drop(columns=['scenario_valid'], errors='ignore')
        
        print(f"[ETAPA 3] Apos validacao: {len(df)} registros validos de {len(all_rows)} carregados")
        logger.info(f"[ETAPA 3] Apos validacao: {len(df)} registros validos de {len(all_rows)} carregados")
        
        # Preparar valores com transformações vetorizadas
        df['start_date'] = df['DataInicioOperacao'].fillna(df['DataInclusaoOrcamentoLojas'])
        df.loc[df['start_date'].isna(), 'start_date'] = datetime.now()
        
        # ⚠️ IMPORTANTE: status deve ser VARCHAR(50), não INTEGER
        # Converter Ativo para VARCHAR: True/1 → 'active', False/0 → 'inactive'
        df['status'] = df['Ativo'].fillna(False).astype(bool)
        df['status'] = df['status'].map({True: 'active', False: 'inactive'})
        
        # ⚠️ IMPORTANTE: closed_at sempre preenchido com NULL (conforme dictionary)
        df['closed_at'] = None
        df['created_at'] = df['DataInclusaoOrcamentoLojas'].fillna(datetime.now())
        df['updated_at'] = df['DataAlteracaoOrcamentoLojas'].fillna(df['created_at'])
        
        # Converter UUIDs para string
        df['scenario_id'] = df['scenario_id'].astype(str)
        df['store_id'] = df['store_id'].astype(str)
        
        # Remover coluna temporária scenario_key
        df = df.drop(columns=['scenario_key'], errors='ignore')
        
        # Converter DataFrame diretamente para lista de tuplas (otimizado)
        # ⚠️ IMPORTANTE: closed_at sempre NULL, não precisa converter NaT
        closed_at_list = [None] * len(df)
        
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
            
            # Contar origem: Orcamento (IdUsuarioVendedor) [+ FaturamentoOrcamentoComissao desativado na migração]
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
            
            # # Contar FaturamentoOrcamentoComissao (desativado: seller_type commission não existe no destino)
            # if id_orcamento_list:
            #     placeholders = ','.join(['?' for _ in id_orcamento_list])
            #     cursor_sql.execute(f"""
            #         SELECT COUNT(*)
            #         FROM FaturamentoOrcamentoComissao
            #         WHERE IdOrcamento IN ({placeholders})
            #     """, id_orcamento_list)
            # elif self.limit_rows > 0:
            #     cursor_sql.execute(f"""
            #         SELECT COUNT(*)
            #         FROM (
            #             SELECT TOP {self.limit_rows} Id
            #             FROM FaturamentoOrcamentoComissao
            #             ORDER BY Id
            #         ) AS limited
            #     """)
            # else:
            #     cursor_sql.execute("SELECT COUNT(*) FROM FaturamentoOrcamentoComissao")
            # origem_comissao = cursor_sql.fetchone()[0]
            origem_comissao = 0
            
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
            print(f"  Comissoes (migracao desativada): {origem_comissao}")
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
        """ETAPA 4: Migrar contract_sellers (Farmer/Hunter a partir de contratos_ativos.xlsx + users.login/user_name)."""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 4: MIGRANDO CONTRACT_SELLERS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 4: Migrando contract_sellers")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)

        schema_users = "gmcore" if destino == "HML" else "core"
        if destino == "PRD":
            conn_users = DatabaseConnection.get_postgresql_prd_destino_connection()
        else:
            conn_users = DatabaseConnection.get_postgresql_hml_destino_connection()
        cursor_users = conn_users.cursor()

        # Mapa login normalizado (minúsculo) -> user id; df_login_user_id = dicionário tabular para debug/join
        login_to_user_id: Dict[str, Any] = {}
        try:
            cursor_users.execute(
                f"""
                SELECT id, user_name, normalized_user_name, email
                FROM {schema_users}.users
                """
            )
            rows_users = cursor_users.fetchall()
            for uid, un, nun, em in rows_users:
                for cand in (un, nun, em):
                    if cand is None or (isinstance(cand, str) and not str(cand).strip()):
                        continue
                    k = str(cand).strip().lower()
                    if k and k not in login_to_user_id:
                        login_to_user_id[k] = uid
            df_login_user_id = pd.DataFrame(
                [{"login_norm": k, "user_id": v} for k, v in sorted(login_to_user_id.items())]
            )
            print(f"[ETAPA 4] Mapa login→user_id: {len(login_to_user_id)} chaves (df_login_user_id com {len(df_login_user_id)} linhas)")
            logger.info(f"[ETAPA 4] login_to_user_id keys: {len(login_to_user_id)}")
            self.df_login_user_id = df_login_user_id
        except Exception as e:
            logger.error(f"[ETAPA 4] Erro ao montar mapa de users: {e}")
            df_login_user_id = pd.DataFrame(columns=["login_norm", "user_id"])
            self.df_login_user_id = df_login_user_id
            rows_users = []

        # CPF na planilha (coluna "login"): Usuario.Cpf na origem → users.legacy_id = Usuario.Id
        legacy_id_to_user_id: Dict[int, Any] = {}
        cpf_digits_to_user_id: Dict[str, Any] = {}
        try:
            cursor_users.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = 'users' AND column_name = 'legacy_id'
                """,
                (schema_users,),
            )
            if cursor_users.fetchone():
                cursor_users.execute(
                    f"SELECT id, legacy_id FROM {schema_users}.users WHERE legacy_id IS NOT NULL"
                )
                for uid, leg in cursor_users.fetchall():
                    if leg is not None:
                        try:
                            legacy_id_to_user_id[int(leg)] = uid
                        except (TypeError, ValueError):
                            continue
                print(f"[ETAPA 4] Mapa legacy_id (Usuario.Id)→user_id: {len(legacy_id_to_user_id)} linhas")
        except Exception as e:
            logger.warning(f"[ETAPA 4] Nao foi possivel carregar legacy_id em users: {e}")

        if legacy_id_to_user_id:
            try:
                conn_sql_u = DatabaseConnection.get_sql_server_prd_connection()
                cur_sql_u = conn_sql_u.cursor()
                cur_sql_u.execute(
                    """
                    SELECT Id, Cpf
                    FROM Usuario
                    WHERE Cpf IS NOT NULL AND LTRIM(RTRIM(CAST(Cpf AS NVARCHAR(64)))) <> ''
                    """
                )
                for id_usuario, cpf in cur_sql_u.fetchall():
                    uid_pg = legacy_id_to_user_id.get(int(id_usuario))
                    if uid_pg is None:
                        continue
                    digits = re.sub(r"\D", "", str(cpf))
                    key = _normalize_br_cpf_cnpj_digits(digits)
                    if not key:
                        continue
                    if key not in cpf_digits_to_user_id:
                        cpf_digits_to_user_id[key] = uid_pg
                cur_sql_u.close()
                conn_sql_u.close()
                print(f"[ETAPA 4] Mapa CPF (digitos)→user_id (via Usuario): {len(cpf_digits_to_user_id)} chaves")
                logger.info(f"[ETAPA 4] cpf_digits_to_user_id: {len(cpf_digits_to_user_id)}")
            except Exception as e:
                logger.warning(f"[ETAPA 4] Nao foi possivel montar mapa CPF Usuario→user: {e}")

        # Usuário admin (fallback quando ambos logins ausentes/inválidos)
        admin_user_id = None
        try:
            cursor_users.execute(
                f"""
                SELECT id FROM {schema_users}.users
                WHERE LOWER(TRIM(user_name)) = LOWER(%s)
                   OR LOWER(TRIM(COALESCE(email, ''))) = LOWER(%s)
                ORDER BY id
                LIMIT 1
                """,
                ("sysadmin@superaholdings.com.br", "sysadmin@superaholdings.com.br"),
            )
            r = cursor_users.fetchone()
            if r:
                admin_user_id = r[0]
                print(f"[ETAPA 4] Fallback admin user_id: {admin_user_id}")
                logger.info(f"[ETAPA 4] admin_user_id (sysadmin): {admin_user_id}")
        except Exception as e:
            logger.warning(f"[ETAPA 4] Nao foi possivel resolver admin: {e}")

        if admin_user_id is None and rows_users:
            admin_user_id = rows_users[0][0]
            logger.warning(f"[ETAPA 4] Usando primeiro user da lista como fallback admin: {admin_user_id}")

        cursor_users.close()
        conn_users.close()

        def _lookup_user_by_login(login: Optional[str]):
            if login is None:
                return None
            try:
                if pd.isna(login):
                    return None
            except TypeError:
                pass
            if isinstance(login, float) and login == int(login):
                s = str(int(login))
            else:
                s = str(login).strip()
            if not s:
                return None
            r = login_to_user_id.get(s.lower())
            if r is not None:
                return r
            digits = re.sub(r"\D", "", s)
            if len(digits) < 10:
                return None
            key = _normalize_br_cpf_cnpj_digits(digits)
            if not key:
                return None
            return cpf_digits_to_user_id.get(key)

        print("\n[ETAPA 4] Limpando tabela contract_sellers...")
        self.truncate_table("contract_sellers")

        try:
            id_orcamento_filter_list = self._resolve_id_orcamento_list_for_steps()
        except ValueError as e:
            logger.error(f"[ETAPA 4] {e}")
            print(f"ERRO - {e}")
            raise
        logger.info(f"[ETAPA 4] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")
        print(f"[ETAPA 4] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")

        xlsx_path = os.environ.get("CONTRATOS_ATIVOS_FILE", XLSX_DEFAULT)
        _, xlsx_by_id = _load_xlsx_index(xlsx_path)
        print(f"[ETAPA 4] contratos_ativos.xlsx: {len(xlsx_by_id)} linhas no índice ({xlsx_path})")

        batch_values: List[Tuple[str, str, str, datetime, datetime]] = []
        missing_contracts: List[int] = []
        rows_debug: List[Dict[str, Any]] = []

        now = datetime.now()
        for oid in id_orcamento_filter_list:
            contract_uuid = self.contract_id_map.get(oid)
            if not contract_uuid:
                missing_contracts.append(oid)
                continue

            row = xlsx_by_id.get(oid)
            farmer_login, hunter_login = farmer_hunter_logins_from_xlsx_row(row)

            fu = _lookup_user_by_login(farmer_login)
            hu = _lookup_user_by_login(hunter_login)

            if fu is None and hu is not None:
                fu = hu
            elif hu is None and fu is not None:
                hu = fu
            if fu is None and hu is None:
                if admin_user_id is None:
                    logger.error(f"[ETAPA 4] IdOrcamento={oid}: sem login válido e sem admin; ignorando.")
                    self.stats["errors"].append(
                        f"contract_sellers: IdOrcamento={oid} sem user fallback admin"
                    )
                    continue
                fu = admin_user_id
                hu = admin_user_id
                logger.info(
                    f"[ETAPA 4] IdOrcamento={oid}: farmer/hunter sem login resolvido → admin"
                )

            rows_debug.append(
                {
                    "IdOrcamento": oid,
                    "farmer_login": farmer_login,
                    "hunter_login": hunter_login,
                    "farmer_user_id": str(fu),
                    "hunter_user_id": str(hu),
                }
            )

            batch_values.append((str(contract_uuid), str(fu), "farmer", now, now))
            batch_values.append((str(contract_uuid), str(hu), "hunter", now, now))

        if missing_contracts:
            logger.warning(f"[ETAPA 4] Contracts nao encontrados: IdOrcamento={sorted(set(missing_contracts))}")
            self.stats["errors"].extend(
                [f"Contract nao encontrado: IdOrcamento={x}" for x in sorted(set(missing_contracts))]
            )

        if rows_debug:
            df_sellers_resolve = pd.DataFrame(rows_debug)
            self.df_contract_sellers_xlsx = df_sellers_resolve
            print(f"[ETAPA 4] Prévia resolução (primeiras linhas):\n{df_sellers_resolve.head(8).to_string()}")
            logger.info(f"[ETAPA 4] contract_sellers resolvidos para {len(rows_debug)} orçamentos")
        else:
            self.df_contract_sellers_xlsx = pd.DataFrame()
        
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
        
        try:
            id_orcamento_filter_list = self._resolve_id_orcamento_list_for_steps()
        except ValueError as e:
            logger.error(f"[ETAPA 5] {e}")
            print(f"ERRO - {e}")
            raise
        logger.info(f"[ETAPA 5] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")
        print(f"[ETAPA 5] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")
        
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
        
        try:
            id_orcamento_filter_list = self._resolve_id_orcamento_list_for_steps()
        except ValueError as e:
            logger.error(f"[ETAPA 6] {e}")
            print(f"ERRO - {e}")
            raise
        logger.info(f"[ETAPA 6] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")
        print(f"[ETAPA 6] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")
        
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
        
        try:
            id_orcamento_filter_list = self._resolve_id_orcamento_list_for_steps()
        except ValueError as e:
            logger.error(f"[ETAPA 7] {e}")
            print(f"ERRO - {e}")
            raise
        logger.info(f"[ETAPA 7] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")
        print(f"[ETAPA 7] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")
        
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
        
        # Obter person_id válido da tabela people (FK obrigatória)
        schema_core = 'gmcore' if destino == 'HML' else 'core'
        person_uuid = None
        try:
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT id FROM {schema_core}.people LIMIT 1")
            row_person = cursor_pg.fetchone()
            cursor_pg.close()
            conn_pg.close()
            if row_person:
                person_uuid = str(row_person[0])
                logger.info(f"[ETAPA 7] person_id obtido de people: {person_uuid}")
            else:
                logger.warning("[ETAPA 7] Tabela people vazia - contract_partners nao sera migrado (FK person_id)")
                print("[ETAPA 7] AVISO: Tabela people vazia. Nenhum contract_partner sera inserido.")
        except Exception as e:
            logger.warning(f"[ETAPA 7] Erro ao obter person_id de people: {e} - contract_partners nao sera migrado")
            print(f"[ETAPA 7] AVISO: Erro ao obter person_id: {e}. Nenhum contract_partner sera inserido.")
        
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
                
                if not person_uuid:
                    continue
                
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
        """ETAPA 8: Migrar contract_additional_charges (depende de billings - migração de billing ainda não implementada)"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 8: MIGRANDO CONTRACT_ADDITIONAL_CHARGES")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 8: Migrando contract_additional_charges")
        logger.info(f"Ambiente: {destino} | Schema: {schema} | Limite: {'TODOS' if self.limit_rows == 0 else self.limit_rows}")
        logger.info("="*80)
        
        # Carregar billing placeholders se não estiver populado (ex: step 8 executado isolado)
        if not self.contract_billing_map:
            self._ensure_billing_placeholders_for_contracts()
        
        try:
            id_orcamento_filter_list = self._resolve_id_orcamento_list_for_steps()
        except ValueError as e:
            logger.error(f"[ETAPA 8] {e}")
            print(f"ERRO - {e}")
            raise
        logger.info(f"[ETAPA 8] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")
        print(f"[ETAPA 8] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")
        
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
        # billing_id do contract_billing_map (placeholder - BLOCO TEMPORÁRIO)
        batch_values = []
        missing_contracts = []  # Coletar IdOrcamento não encontrados
        
        for row in all_rows:
            try:
                legado_id_orcamento = row[0]
                
                contract_uuid = self.contract_id_map.get(legado_id_orcamento)
                if not contract_uuid:
                    missing_contracts.append(legado_id_orcamento)
                    continue
                
                billing_id_valid = self.contract_billing_map.get(str(contract_uuid))
                if not billing_id_valid:
                    continue
                
                created_at_base = row[9] if row[9] else datetime.now()
                updated_at_base = row[10] if row[10] else datetime.now()
                start_default = _sql_server_to_charge_start_date(row[9], row[10])
                start_epi = _sql_server_to_charge_start_date(row[5], row[9], row[10])
                
                # REGISTRO 1: EPI
                if (row[1] and row[1] > 0) or (row[4] is True):
                    # billing_model: se tem InicioCobrancaEPI, é recurring (cobrança recorrente), senão one_time
                    # # Anterior
                    # billing_model_epi = 'recurring' if row[5] else 'one_time'

                    #Novo = enum
                    billing_model_epi = 'monthly'

                    batch_values.append((
                        str(contract_uuid),
                        billing_id_valid,
                        row[1] if row[1] else 0.0,
                        'other',
                        billing_model_epi,
                        start_epi,
                        None,
                        created_at_base,
                        updated_at_base,
                    ))
                
                # REGISTRO 2: Trade Marketing
                if row[2] and row[2] > 0:
                    batch_values.append((
                        str(contract_uuid),
                        billing_id_valid,
                        row[2],
                        # anterior
                        # 'trade_marketing',
                        'other',
                        # anterior
                        # 'recurring',
                        'monthly',
                        start_default,
                        None,
                        created_at_base,
                        updated_at_base,
                    ))
                
                # REGISTRO 3: Outros
                if row[3] and row[3] > 0:
                    batch_values.append((
                        str(contract_uuid),
                        billing_id_valid,
                        row[3],
                        # anterior
                        # 'others',
                        'other',
                        # anterior
                        # 'one_time',
                        'monthly',
                        start_default,
                        None,
                        created_at_base,
                        updated_at_base,
                    ))
                
                # REGISTRO 4: Juros
                if row[6] and row[6] > 0:
                    batch_values.append((
                        str(contract_uuid),
                        billing_id_valid,
                        row[6],
                        # anterior
                        # 'interest',
                        'other',
                        # anterior
                        # 'recurring',
                        'monthly',
                        start_default,
                        None,
                        created_at_base,
                        updated_at_base,
                    ))
                
                # REGISTRO 5: Desconto
                if row[7] and row[7] > 0:
                    batch_values.append((
                        str(contract_uuid),
                        billing_id_valid,
                        row[7],
                        # anterior
                        # 'discount',
                        'other',
                        # anterior
                        # 'one_time',
                        'monthly',
                        start_default,
                        None,
                        created_at_base,
                        updated_at_base,
                    ))
                
                # REGISTRO 6: Multa
                if row[8] and row[8] > 0:
                    batch_values.append((
                        str(contract_uuid),
                        billing_id_valid,
                        row[8],
                        # anterior
                        # 'fine',
                        'other',
                        # anterior
                        # 'one_time',
                        'monthly',
                        start_default,
                        None,
                        created_at_base,
                        updated_at_base,
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
        # billing_id obrigatório em PRD (FK para financial.billings)
        insert_query = f"""
        INSERT INTO {schema}.contract_additional_charges (
            id, contract_id, billing_id, amount, charge_type, billing_model,
            start_date, end_date, created_at, updated_at
        ) VALUES %s
        """
        insert_template = (
            "(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        
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
    
    def step10_migrate_contract_scenarios_brands(self):
        """
        ETAPA 10: Popular contract_scenarios_brands no destino (sem SQL Server).
        TRUNCATE + INSERT DISTINCT (junção não polimórfica).
        scenario_id = contract_scenarios.id; customer_brand_id = customer_customer_brand.customer_brand_id.
        """
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        schema_core = "gmcore" if destino == "HML" else "core"
        print("\n" + "=" * 80)
        print("ETAPA 10: POPULANDO CONTRACT_SCENARIOS_BRANDS (DESTINO)")
        print("=" * 80)
        logger.info("=" * 80)
        logger.info("ETAPA 10: Populando contract_scenarios_brands")
        logger.info(f"Ambiente: {destino} | Schema commercial: {schema} | Schema core: {schema_core}")
        logger.info("=" * 80)
        
        try:
            id_orcamento_filter_list = self._resolve_id_orcamento_list_for_steps()
        except ValueError as e:
            logger.error(f"[ETAPA 10] {e}")
            print(f"ERRO - {e}")
            raise
        logger.info(f"[ETAPA 10] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")
        print(f"[ETAPA 10] {len(id_orcamento_filter_list)} IdOrcamento (escopo ∩ XLSX)")
        
        # Tabela de junção (não polimórfica): TRUNCATE + INSERT DISTINCT a partir do estado atual no destino.
        print("\n[ETAPA 10] TRUNCATE contract_scenarios_brands...")
        self.truncate_table("contract_scenarios_brands")
        
        insert_sql = f"""
        INSERT INTO {schema}.contract_scenarios_brands (scenario_id, customer_brand_id)
        SELECT DISTINCT cs.id, cb.customer_brand_id
        FROM {schema_core}.customer_customer_brand cb
        INNER JOIN {schema}.contracts c ON c.customer_id = cb.customer_id
        INNER JOIN {schema}.contract_scenarios cs ON cs.contract_id = c.id
        """
        
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        try:
            cursor_pg.execute(insert_sql)
            inserted = cursor_pg.rowcount if cursor_pg.rowcount is not None else 0
            conn_pg.commit()
            self.stats["contract_scenarios_brands"] = inserted
            print(f"\n[ETAPA 10] CONCLUIDA! contract_scenarios_brands inseridos: {inserted}")
            logger.info(f"[ETAPA 10] CONCLUIDA! contract_scenarios_brands inseridos: {inserted}")
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"[ETAPA 10] Erro ao inserir contract_scenarios_brands: {e}")
            print(f"ERRO - [ETAPA 10] {e}")
            raise
        finally:
            cursor_pg.close()
            conn_pg.close()
    
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
            self.ensure_billings_after_step1()
            self.step9_migrate_promoter_tasks()  # Executar antes do step2
            self.step2_migrate_contract_scenarios()
            self.step3_migrate_contract_scenario_stores()
            self.step4_migrate_contract_sellers()
            self.step5_migrate_contract_team_members()
            self.step6_migrate_contract_contacts()
            self.step7_migrate_contract_partners()
            self.step8_migrate_contract_additional_charges()
            self.step10_migrate_contract_scenarios_brands()
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            self._collect_destino_stats_snapshot()
            self.save_migration_execution_stats_json(duration_seconds=duration.total_seconds())
            
            print("\n" + "="*80)
            print("MIGRACAO CONCLUIDA COM SUCESSO!")
            print("="*80)
            logger.info("="*80)
            logger.info("MIGRACAO CONCLUIDA COM SUCESSO!")
            logger.info("="*80)
            
            cd = self.stats.get("customers_total_destino")
            cl = self.stats.get("customers_total_legacy_ids_nos_grupos")
            cm = self.stats.get("customers_indicador_mesclados")
            bl = self.stats.get("billings")
            
            print(f"\nDuracao total: {duration}")
            print(f"\nESTATISTICAS FINAIS:")
            print(f"  Contracts: {self.stats['contracts']}")
            print(f"  Billings (total destino {destino}): {bl}")
            print(f"  Promoter Tasks: {self.stats['promoter_tasks']}")
            print(f"  Contract Scenarios: {self.stats['contract_scenarios']}")
            print(f"  Contract Scenario Stores: {self.stats['contract_scenario_stores']}")
            print(f"  Contract Sellers: {self.stats['contract_sellers']}")
            print(f"  Contract Team Members: {self.stats['contract_team_members']}")
            print(f"  Contract Contacts: {self.stats['contract_contacts']}")
            print(f"  Contract Partners: {self.stats['contract_partners']}")
            print(f"  Contract Additional Charges: {self.stats['contract_additional_charges']}")
            print(f"  Contract Scenarios Brands: {self.stats['contract_scenarios_brands']}")
            print(f"  Total Customers Deduplicados (registros em {destino}): {cd}")
            print(f"  Total Customers Duplicados (legacy_ids extras agregados por CNPJ): {cm}")
            print(f"  Total legacy_ids referenciados (soma nos grupos): {cl}")
            print(f"  Erros: {len(self.stats['errors'])}")
            
            logger.info(f"Duracao: {duration}")
            logger.info(f"Contracts: {self.stats['contracts']}")
            logger.info(f"Billings (destino): {bl}")
            logger.info(f"Contract Scenarios: {self.stats['contract_scenarios']}")
            logger.info(f"Contract Scenario Stores: {self.stats['contract_scenario_stores']}")
            logger.info(f"Contract Sellers: {self.stats['contract_sellers']}")
            logger.info(f"Contract Team Members: {self.stats['contract_team_members']}")
            logger.info(f"Contract Contacts: {self.stats['contract_contacts']}")
            logger.info(f"Contract Partners: {self.stats['contract_partners']}")
            logger.info(f"Contract Additional Charges: {self.stats['contract_additional_charges']}")
            logger.info(f"Contract Scenarios Brands: {self.stats['contract_scenarios_brands']}")
            logger.info(f"Promoter Tasks: {self.stats['promoter_tasks']}")
            logger.info(
                f"Customers dedup/legacy: destino={cd}, legacy_ids_soma={cl}, extras_mesclados={cm}"
            )
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
