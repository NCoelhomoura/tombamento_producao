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
from datetime import datetime
from typing import List, Dict, Optional, Tuple

# Adicionar diretório utils ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database_connection import DatabaseConnection

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
CHUNK_SIZE = 1000


class StoresMigration:
    """Classe para executar a migração de dados de stores"""
    
    def __init__(self, limit_rows=0):
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
    
    def should_include_legacy_id(self):
        """Retorna True se deve incluir legacy_id (apenas em HML)"""
        destino = DatabaseConnection.get_destino()
        return destino == 'HML'
    
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
    
    def delete_polymorphic_table(self, table_name: str, entity_type: str, type_column: str, schema: str = None):
        """
        Faz DELETE em tabela polimórfica filtrando por tipo de entidade
        Exemplo: delete_polymorphic_table('contacts', 'stores', 'contactable_type')
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
            # Contar origem (aplicando limite se especificado)
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            if self.limit_rows > 0:
                cursor_sql.execute(f"SELECT COUNT(*) FROM (SELECT TOP {self.limit_rows} Id FROM CanalEstabelecimento ORDER BY Id) AS limited")
            else:
                cursor_sql.execute("SELECT COUNT(*) FROM CanalEstabelecimento")
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
        
        # Buscar dados do SQL Server
        print("[ETAPA 1] Buscando dados do SQL Server...")
        sql_query = """
        SELECT 
            Id,
            Nome,
            Ativo,
            DataInclusao,
            DataAlteracao
        FROM CanalEstabelecimento
        ORDER BY Id
        """
        
        # Adicionar LIMIT se especificado
        if self.limit_rows > 0:
            sql_query = sql_query.replace("ORDER BY Id", f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
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
                
                chunk_errors = 0
                for row in rows:
                    try:
                        segment_id = uuid.uuid4()
                        legado_id = row[0]
                        self.store_segment_id_map[legado_id] = segment_id
                        
                        # Construir INSERT condicionalmente (legacy_id apenas em HML)
                        include_legacy = self.should_include_legacy_id()
                        if include_legacy:
                            insert_query = f"""
                            INSERT INTO {schema}.store_segments (
                                id, name, is_active, created_at, updated_at, legacy_id
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                            """
                            cursor_pg.execute(insert_query, (
                                str(segment_id),
                                self.clean_string(row[1]),  # Nome
                                row[2] if row[2] is not None else False,  # Ativo
                                row[3],  # DataInclusao -> created_at
                                row[4] if row[4] else row[3],  # DataAlteracao -> updated_at
                                legado_id  # legacy_id
                            ))
                        else:
                            insert_query = f"""
                            INSERT INTO {schema}.store_segments (
                                id, name, is_active, created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s)
                            """
                            cursor_pg.execute(insert_query, (
                                str(segment_id),
                                self.clean_string(row[1]),  # Nome
                                row[2] if row[2] is not None else False,  # Ativo
                                row[3],  # DataInclusao -> created_at
                                row[4] if row[4] else row[3]  # DataAlteracao -> updated_at
                            ))
                        
                        self.stats['store_segments'] += 1
                        total_processed += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir store_segment Id={row[0]}: {e}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
                
                # Commit do chunk
                try:
                    conn_pg.commit()
                    print(f"[ETAPA 1] Chunk {chunk_num} processado: {total_processed} registros inseridos, {chunk_errors} erros")
                    logger.info(f"[ETAPA 1] Chunk {chunk_num} processado: {total_processed} registros inseridos, {chunk_errors} erros")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            print(f"\n[ETAPA 1] CONCLUIDA! Total de store_segments migrados: {self.stats['store_segments']}")
            logger.info(f"ETAPA 1 concluida: {self.stats['store_segments']} registros")
            
            # Validação e relatório de qualidade
            self.validate_step1_store_segments()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 1: {e}")
            raise
        finally:
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
    
    # ========================================================================
    # ETAPA 2: RETAIL_CHAINS
    # ========================================================================
    
    def validate_step2_retail_chains(self):
        """Validação e relatório de qualidade - ETAPA 2"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 2: RETAIL_CHAINS")
        print("-"*80)
        
        try:
            # Contar origem (aplicando limite se especificado)
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            if self.limit_rows > 0:
                cursor_sql.execute(f"SELECT COUNT(*) FROM (SELECT TOP {self.limit_rows} Id FROM Rede ORDER BY Id) AS limited")
            else:
                cursor_sql.execute("SELECT COUNT(*) FROM Rede")
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
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
        
        # Truncate
        print("\n[ETAPA 2] Limpando tabela retail_chains...")
        self.truncate_table('retail_chains')
        
        # Buscar dados do SQL Server
        print("[ETAPA 2] Buscando dados do SQL Server...")
        sql_query = """
        SELECT 
            Id,
            Nome,
            Codigo,
            Ativo,
            DataInclusao,
            DataAlteracao
        FROM Rede
        ORDER BY Id
        """
        
        # Adicionar LIMIT se especificado
        if self.limit_rows > 0:
            sql_query = sql_query.replace("ORDER BY Id", f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
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
                
                chunk_errors = 0
                for row in rows:
                    try:
                        retail_chain_id = uuid.uuid4()
                        legado_id = row[0]
                        self.retail_chain_id_map[legado_id] = retail_chain_id
                        
                        # Construir INSERT condicionalmente (legacy_id apenas em HML)
                        include_legacy = self.should_include_legacy_id()
                        if include_legacy:
                            insert_query = f"""
                            INSERT INTO {schema}.retail_chains (
                                id, name, description, is_active, created_at, updated_at, legacy_id
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor_pg.execute(insert_query, (
                                str(retail_chain_id),
                                self.clean_string(row[1]),  # Nome -> name
                                str(row[2]) if row[2] is not None else '',  # Codigo -> description (converter para string)
                                row[3] if row[3] is not None else False,  # Ativo -> is_active
                                row[4],  # DataInclusao -> created_at
                                row[5] if row[5] else row[4],  # DataAlteracao -> updated_at
                                legado_id  # legacy_id
                            ))
                        else:
                            insert_query = f"""
                            INSERT INTO {schema}.retail_chains (
                                id, name, description, is_active, created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                            """
                            cursor_pg.execute(insert_query, (
                                str(retail_chain_id),
                                self.clean_string(row[1]),  # Nome -> name
                                str(row[2]) if row[2] is not None else '',  # Codigo -> description (converter para string)
                                row[3] if row[3] is not None else False,  # Ativo -> is_active
                                row[4],  # DataInclusao -> created_at
                                row[5] if row[5] else row[4]  # DataAlteracao -> updated_at
                            ))
                        
                        self.stats['retail_chains'] += 1
                        total_processed += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir retail_chain Id={row[0]}: {e}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
                
                # Commit do chunk
                try:
                    conn_pg.commit()
                    print(f"[ETAPA 2] Chunk {chunk_num} processado: {total_processed} registros inseridos, {chunk_errors} erros")
                    logger.info(f"[ETAPA 2] Chunk {chunk_num} processado: {total_processed} registros inseridos, {chunk_errors} erros")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            print(f"\n[ETAPA 2] CONCLUIDA! Total de retail_chains migrados: {self.stats['retail_chains']}")
            logger.info(f"ETAPA 2 concluida: {self.stats['retail_chains']} registros")
            
            # Validação e relatório de qualidade
            self.validate_step2_retail_chains()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 2: {e}")
            raise
        finally:
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
    
    # ========================================================================
    # ETAPA 3: STORE_BRANDS
    # ========================================================================
    
    def validate_step3_store_brands(self):
        """Validação e relatório de qualidade - ETAPA 3"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 3: STORE_BRANDS")
        print("-"*80)
        
        try:
            # Contar origem (aplicando limite se especificado)
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            if self.limit_rows > 0:
                cursor_sql.execute(f"SELECT COUNT(*) FROM (SELECT TOP {self.limit_rows} Id FROM Bandeira ORDER BY Id) AS limited")
            else:
                cursor_sql.execute("SELECT COUNT(*) FROM Bandeira")
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
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
        
        # Truncate
        print("\n[ETAPA 3] Limpando tabela store_brands...")
        self.truncate_table('store_brands')
        
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
        
        # Buscar dados do SQL Server
        print("[ETAPA 3] Buscando dados do SQL Server...")
        sql_query = """
        SELECT 
            b.Id,
            b.NomeFantasia,
            b.Codigo,
            b.IdRede,
            b.Ativo,
            b.DataInclusao,
            b.DataAlteracao,
            (SELECT TOP 1 IdCanalEstabelecimento 
             FROM Estabelecimento 
             WHERE IdBandeira = b.Id AND IdCanalEstabelecimento IS NOT NULL) AS IdCanalEstabelecimento
        FROM Bandeira b
        ORDER BY b.Id
        """
        
        # Adicionar LIMIT se especificado
        if self.limit_rows > 0:
            sql_query = sql_query.replace("ORDER BY b.Id", f"ORDER BY b.Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
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
                
                chunk_errors = 0
                for row in rows:
                    try:
                        bandeira_id = row[0]
                        store_brand_id = uuid.uuid4()
                        self.store_brand_id_map[bandeira_id] = store_brand_id
                        
                        # Lookup retail_chain_id
                        retail_chain_id = retail_chain_map.get(row[3])  # IdRede
                        if not retail_chain_id:
                            raise ValueError(f"Retail chain não encontrado para IdRede={row[3]}")
                        
                        # Lookup store_segment_id
                        # Buscar o IdCanalEstabelecimento mais comum para esta bandeira
                        store_segment_id = None
                        if row[7]:  # IdCanalEstabelecimento da subquery
                            store_segment_id = store_segment_map.get(row[7])
                        
                        # Se não encontrou, buscar diretamente do Estabelecimento
                        if not store_segment_id:
                            cursor_sql_temp = conn_sql.cursor()
                            cursor_sql_temp.execute("""
                                SELECT TOP 1 IdCanalEstabelecimento 
                                FROM Estabelecimento 
                                WHERE IdBandeira = ? AND IdCanalEstabelecimento IS NOT NULL
                                ORDER BY Id
                            """, (bandeira_id,))
                            temp_row = cursor_sql_temp.fetchone()
                            cursor_sql_temp.close()
                            if temp_row and temp_row[0]:
                                store_segment_id = store_segment_map.get(temp_row[0])
                        
                        # Se ainda não encontrou, usar o primeiro store_segment disponível como padrão
                        # (isso pode não ser ideal, mas evita erro de NOT NULL)
                        if not store_segment_id and store_segment_map:
                            # Usar o primeiro segmento disponível como padrão
                            store_segment_id = list(store_segment_map.values())[0]
                            logger.warning(f"Store segment não encontrado para Bandeira Id={bandeira_id}, usando padrão: {store_segment_id}")
                        
                        # abras_code: usar Codigo se disponível, senão "empty"
                        abras_code = str(row[2]) if row[2] is not None else "empty"
                        
                        # Construir INSERT condicionalmente (legacy_id apenas em HML)
                        include_legacy = self.should_include_legacy_id()
                        if include_legacy:
                            insert_query = f"""
                            INSERT INTO {schema}.store_brands (
                                id, description, abras_code, retail_chain_id, store_segment_id,
                                is_active, created_at, updated_at, legacy_id
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor_pg.execute(insert_query, (
                                str(store_brand_id),
                                self.clean_string(row[1]),  # NomeFantasia -> description
                                abras_code,  # abras_code (não pode ser nulo)
                                str(retail_chain_id),  # retail_chain_id (lookup)
                                str(store_segment_id) if store_segment_id else None,  # store_segment_id (lookup)
                                row[4] if row[4] is not None else False,  # Ativo -> is_active
                                row[5],  # DataInclusao -> created_at
                                row[6] if row[6] else row[5],  # DataAlteracao -> updated_at
                                bandeira_id  # legacy_id
                            ))
                        else:
                            insert_query = f"""
                            INSERT INTO {schema}.store_brands (
                                id, description, abras_code, retail_chain_id, store_segment_id,
                                is_active, created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor_pg.execute(insert_query, (
                                str(store_brand_id),
                                self.clean_string(row[1]),  # NomeFantasia -> description
                                abras_code,  # abras_code (não pode ser nulo)
                                str(retail_chain_id),  # retail_chain_id (lookup)
                                str(store_segment_id) if store_segment_id else None,  # store_segment_id (lookup)
                                row[4] if row[4] is not None else False,  # Ativo -> is_active
                                row[5],  # DataInclusao -> created_at
                                row[6] if row[6] else row[5]  # DataAlteracao -> updated_at
                            ))
                        
                        self.stats['store_brands'] += 1
                        total_processed += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir store_brand Id={row[0]}: {e}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
                
                # Commit do chunk
                try:
                    conn_pg.commit()
                    print(f"[ETAPA 3] Chunk {chunk_num} processado: {total_processed} registros inseridos, {chunk_errors} erros")
                    logger.info(f"[ETAPA 3] Chunk {chunk_num} processado: {total_processed} registros inseridos, {chunk_errors} erros")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            print(f"\n[ETAPA 3] CONCLUIDA! Total de store_brands migrados: {self.stats['store_brands']}")
            logger.info(f"ETAPA 3 concluida: {self.stats['store_brands']} registros")
            
            # Validação e relatório de qualidade
            self.validate_step3_store_brands()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 3: {e}")
            raise
        finally:
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
    
    # ========================================================================
    # ETAPA 4: STORES
    # ========================================================================
    
    def validate_step4_stores(self):
        """Validação e relatório de qualidade - ETAPA 4"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 4: STORES")
        print("-"*80)
        
        try:
            # Contar origem (aplicando limite se especificado)
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            if self.limit_rows > 0:
                cursor_sql.execute(f"SELECT COUNT(*) FROM (SELECT TOP {self.limit_rows} Id FROM Estabelecimento ORDER BY Id) AS limited")
            else:
                cursor_sql.execute("SELECT COUNT(*) FROM Estabelecimento")
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            schema = get_schema_atual()
            destino_nome = DatabaseConnection.get_destino()
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
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
        
        # Truncate
        print("\n[ETAPA 4] Limpando tabela stores...")
        self.truncate_table('stores')
        
        # Carregar mapeamento de store_brands
        print("[ETAPA 4] Carregando mapeamento de store_brands...")
        include_legacy = self.should_include_legacy_id()
        if include_legacy:
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT id, legacy_id FROM {schema}.store_brands")
            store_brand_map = {}
            for row in cursor_pg.fetchall():
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
        
        # Buscar dados do SQL Server
        # Se houver limite, filtrar apenas estabelecimentos cujas bandeiras foram migradas
        print("[ETAPA 4] Buscando dados do SQL Server...")
        if self.limit_rows > 0:
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
                    e.DataAlteracao
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
                    DataAlteracao
                FROM Estabelecimento
                ORDER BY Id
                """
        else:
            sql_query = """
            SELECT 
                Id,
                NomeFantasia,
                IdBandeira,
                Ativo,
                DataInclusao,
                DataAlteracao
            FROM Estabelecimento
            ORDER BY Id
            """
        
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
                print(f"[ETAPA 4] Processando chunk {chunk_num} ({len(rows)} registros)...")
                logger.info(f"[ETAPA 4] Processando chunk {chunk_num} ({len(rows)} registros)...")
                
                chunk_errors = 0
                for row in rows:
                    try:
                        store_id = uuid.uuid4()
                        legado_id = row[0]
                        self.store_id_map[legado_id] = store_id
                        
                        # Lookup store_brand_id
                        store_brand_id = store_brand_map.get(row[2])  # IdBandeira
                        if not store_brand_id:
                            # Usar store_brand padrão quando não encontrado
                            logger.warning(f"Store brand não encontrado para IdBandeira={row[2]}. Usando store_brand padrão 'Teste store_brands'")
                            store_brand_id = default_store_brand_id
                        
                        # Construir INSERT condicionalmente (legacy_id apenas em HML)
                        if include_legacy:
                            insert_query = f"""
                            INSERT INTO {schema}.stores (
                                id, name, store_brand_id, is_active, created_at, updated_at, legacy_id
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor_pg.execute(insert_query, (
                                str(store_id),
                                self.clean_string(row[1], 255),  # NomeFantasia -> name
                                str(store_brand_id),  # store_brand_id (lookup)
                                row[3] if row[3] is not None else False,  # Ativo -> is_active
                                row[4],  # DataInclusao -> created_at
                                row[5] if row[5] else row[4],  # DataAlteracao -> updated_at
                                legado_id  # legacy_id
                            ))
                        else:
                            insert_query = f"""
                            INSERT INTO {schema}.stores (
                                id, name, store_brand_id, is_active, created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                            """
                            cursor_pg.execute(insert_query, (
                                str(store_id),
                                self.clean_string(row[1], 255),  # NomeFantasia -> name
                                str(store_brand_id),  # store_brand_id (lookup)
                                row[3] if row[3] is not None else False,  # Ativo -> is_active
                                row[4],  # DataInclusao -> created_at
                                row[5] if row[5] else row[4]  # DataAlteracao -> updated_at
                            ))
                        
                        self.stats['stores'] += 1
                        total_processed += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir store Id={row[0]}: {e}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
                
                # Commit do chunk
                try:
                    conn_pg.commit()
                    print(f"[ETAPA 4] Chunk {chunk_num} processado: {total_processed} registros inseridos, {chunk_errors} erros")
                    logger.info(f"[ETAPA 4] Chunk {chunk_num} processado: {total_processed} registros inseridos, {chunk_errors} erros")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            print(f"\n[ETAPA 4] CONCLUIDA! Total de stores migrados: {self.stats['stores']}")
            logger.info(f"ETAPA 4 concluida: {self.stats['stores']} registros")
            
            # Validação e relatório de qualidade
            self.validate_step4_stores()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 4: {e}")
            raise
        finally:
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
    
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
            
            # Se houver limite e mapeamento de stores, usar apenas os estabelecimentos migrados
            if self.limit_rows > 0 and self.store_id_map:
                estabelecimentos_ids = tuple(self.store_id_map.keys())
                cursor_sql.execute(f"""
                    SELECT COUNT(*) 
                    FROM Estabelecimento
                    WHERE Id IN {estabelecimentos_ids}
                      AND Cnpj IS NOT NULL AND LTRIM(RTRIM(Cnpj)) != ''
                """)
            elif self.limit_rows > 0:
                cursor_sql.execute(f"""
                    SELECT COUNT(*) 
                    FROM (SELECT TOP {self.limit_rows} Id FROM Estabelecimento WHERE Cnpj IS NOT NULL AND LTRIM(RTRIM(Cnpj)) != '' ORDER BY Id) AS limited
                """)
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
        
        # Truncate
        print("\n[ETAPA 5] Limpando tabela store_cnpjs...")
        self.truncate_table('store_cnpjs')
        
        # Buscar dados do SQL Server
        # Se houver limite, filtrar apenas estabelecimentos que foram migrados
        print("[ETAPA 5] Buscando dados do SQL Server...")
        if self.limit_rows > 0 and self.store_id_map:
            # Buscar apenas estabelecimentos que foram migrados
            estabelecimentos_ids = tuple(self.store_id_map.keys())
            sql_query = f"""
            SELECT 
                Id,
                Cnpj,
                Ativo,
                DataInclusao,
                DataAlteracao
            FROM Estabelecimento
            WHERE Id IN {estabelecimentos_ids}
              AND Cnpj IS NOT NULL AND LTRIM(RTRIM(Cnpj)) != ''
            ORDER BY Id
            """
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
                print(f"[ETAPA 5] Processando chunk {chunk_num} ({len(rows)} registros)...")
                logger.info(f"[ETAPA 5] Processando chunk {chunk_num} ({len(rows)} registros)...")
                
                chunk_errors = 0
                for row in rows:
                    try:
                        legado_id = row[0]
                        store_id = self.store_id_map.get(legado_id)
                        
                        if not store_id:
                            continue
                        
                        # Limpar CNPJ
                        cnpj = self.clean_cnpj(row[1])
                        if not cnpj:
                            continue  # Pular se não houver CNPJ válido
                        
                        insert_query = f"""
                        INSERT INTO {schema}.store_cnpjs (
                            id, cnpj, is_main, store_id, is_active, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        cursor_pg.execute(insert_query, (
                            str(uuid.uuid4()),  # id
                            cnpj,  # cnpj (limpo)
                            True,  # is_main (sempre true para CNPJ principal)
                            str(store_id),  # store_id (lookup)
                            row[2] if row[2] is not None else False,  # Ativo -> is_active
                            row[3],  # DataInclusao -> created_at
                            row[4] if row[4] else row[3]  # DataAlteracao -> updated_at
                        ))
                        
                        self.stats['store_cnpjs'] += 1
                        total_processed += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir store_cnpj para Estabelecimento Id={row[0]}: {e}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
                
                # Commit do chunk
                try:
                    conn_pg.commit()
                    print(f"[ETAPA 5] Chunk {chunk_num} processado: {total_processed} registros inseridos, {chunk_errors} erros")
                    logger.info(f"[ETAPA 5] Chunk {chunk_num} processado: {total_processed} registros inseridos, {chunk_errors} erros")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            print(f"\n[ETAPA 5] CONCLUIDA! Total de store_cnpjs migrados: {self.stats['store_cnpjs']}")
            logger.info(f"ETAPA 5 concluida: {self.stats['store_cnpjs']} registros")
            
            # Validação e relatório de qualidade
            self.validate_step5_store_cnpjs()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 5: {e}")
            raise
        finally:
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
    
    # ========================================================================
    # ETAPA 6: ADDRESSES (polimórfica para stores)
    # ========================================================================
    
    def validate_step6_addresses(self):
        """Validação e relatório de qualidade - ETAPA 6"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 6: ADDRESSES (STORES)")
        print("-"*80)
        
        try:
            # Aplicar limite se especificado
            limit_clause = ""
            if self.limit_rows > 0:
                limit_clause = f" OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY"
            
            # Contar origem - endereços de estabelecimentos (aplicando mesmo filtro e limite da migração)
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            query_main = f"""
                SELECT COUNT(*) 
                FROM (
                    SELECT Id, Endereco
                    FROM Estabelecimento
                    WHERE Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != ''
                    ORDER BY Id
                    {limit_clause}
                ) AS limited
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
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.addresses WHERE addressable_type = 'stores'")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - Estabelecimento):")
            print(f"  Total de enderecos: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL {destino_nome} - {schema}.addresses):")
            print(f"  Total inserido (addressable_type='stores'): {destino_count}")
            
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
        print("\n[ETAPA 6] Limpando addresses de stores...")
        self.delete_polymorphic_table('addresses', 'stores', 'addressable_type')
        
        # Buscar dados do SQL Server
        # Se houver limite, filtrar apenas estabelecimentos que foram migrados
        print("[ETAPA 6] Buscando dados do SQL Server...")
        if self.limit_rows > 0 and self.store_id_map:
            # Buscar apenas estabelecimentos que foram migrados
            estabelecimentos_ids = tuple(self.store_id_map.keys())
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
            WHERE Id IN {estabelecimentos_ids}
              AND Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != ''
            ORDER BY Id
            """
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
                print(f"[ETAPA 6] Processando chunk {chunk_num} ({len(rows)} registros)...")
                logger.info(f"[ETAPA 6] Processando chunk {chunk_num} ({len(rows)} registros)...")
                
                chunk_errors = 0
                for row in rows:
                    legado_id = row[0]
                    store_id = self.store_id_map.get(legado_id)
                    
                    if not store_id:
                        continue
                    
                    # Endereço Principal
                    if row[1] and str(row[1]).strip():  # Endereco
                        try:
                            cep = re.sub(r'[^\d]', '', str(row[5])) if row[5] else None
                            if not cep or cep == '0' * len(cep):
                                cep = '00000000'
                            
                            # Converter latitude/longitude se possível
                            lat = None
                            lon = None
                            if row[8]:  # Latitude
                                try:
                                    lat = float(str(row[8]).replace(',', '.'))
                                except:
                                    pass
                            if row[9]:  # Longitude
                                try:
                                    lon = float(str(row[9]).replace(',', '.'))
                                except:
                                    pass
                            
                            # Valores obrigatórios com defaults
                            zone_value = self.clean_string(row[4], 100) or ''
                            region_value = ''
                            number_value = self.clean_string(row[2], 20) if row[2] and str(row[2]).strip() else 'S/N'
                            city_value = self.clean_string(row[6], 100) if row[6] and str(row[6]).strip() else ''
                            state_value = self.clean_string(row[7], 2) if row[7] and str(row[7]).strip() else ''
                            street_value = self.clean_string(row[1], 500) or ''
                            neighborhood_value = self.clean_string(row[4], 100) or ''
                            
                            # Construir INSERT condicionalmente (legacy_id apenas em HML)
                            include_legacy = self.should_include_legacy_id()
                            if include_legacy:
                                insert_query = f"""
                                INSERT INTO {schema}.addresses (
                                    id, legacy_id, addressable_id, addressable_type, type,
                                    postal_code, street, number, address_line_2, neighborhood,
                                    city, state, municipal_code, latitude, longitude, zone, region,
                                    created_at, updated_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """
                                cursor_pg.execute(insert_query, (
                                    str(uuid.uuid4()),
                                    legado_id,
                                    str(store_id),
                                    'stores',
                                    'main',
                                    cep,
                                    street_value,
                                    number_value,
                                    self.clean_string(row[3], 200),
                                    neighborhood_value,
                                    city_value,
                                    state_value,
                                    0,  # municipal_code (padrão 0)
                                    lat,
                                    lon,
                                    zone_value,
                                    region_value,
                                    row[10],
                                    row[11] if row[11] else row[10]
                                ))
                            else:
                                insert_query = f"""
                                INSERT INTO {schema}.addresses (
                                    id, addressable_id, addressable_type, type,
                                    postal_code, street, number, address_line_2, neighborhood,
                                    city, state, municipal_code, latitude, longitude, zone, region,
                                    created_at, updated_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """
                                cursor_pg.execute(insert_query, (
                                    str(uuid.uuid4()),
                                    str(store_id),
                                    'stores',
                                    'main',
                                    cep,
                                    street_value,
                                    number_value,
                                    self.clean_string(row[3], 200),
                                    neighborhood_value,
                                    city_value,
                                    state_value,
                                    0,  # municipal_code (padrão 0)
                                    lat,
                                    lon,
                                    zone_value,
                                    region_value,
                                    row[10],
                                    row[11] if row[11] else row[10]
                                ))
                            
                            self.stats['addresses'] += 1
                            
                        except Exception as e:
                            error_msg = f"Erro ao inserir endereco para Estabelecimento Id={legado_id}: {e}"
                            logger.error(error_msg)
                            print(f"ERRO - {error_msg}")
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            try:
                                conn_pg.rollback()
                                cursor_pg = conn_pg.cursor()
                            except Exception as rollback_error:
                                logger.error(f"Erro ao fazer rollback: {rollback_error}")
                            continue
                
                # Commit do chunk
                try:
                    conn_pg.commit()
                    total_processed = self.stats['addresses']
                    print(f"[ETAPA 6] Chunk {chunk_num} processado: {total_processed} enderecos inseridos, {chunk_errors} erros")
                    logger.info(f"[ETAPA 6] Chunk {chunk_num} processado: {total_processed} enderecos inseridos, {chunk_errors} erros")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            print(f"\n[ETAPA 6] CONCLUIDA! Total de addresses migrados: {self.stats['addresses']}")
            logger.info(f"ETAPA 6 concluida: {self.stats['addresses']} registros")
            
            # Validação e relatório de qualidade
            self.validate_step6_addresses()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 6: {e}")
            raise
        finally:
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
    
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
            
            # Se houver limite e mapeamento de stores, usar apenas os estabelecimentos migrados
            if self.limit_rows > 0 and self.store_id_map:
                estabelecimentos_ids = tuple(self.store_id_map.keys())
                query_email = f"""
                    SELECT COUNT(*) 
                    FROM Estabelecimento
                    WHERE Id IN {estabelecimentos_ids}
                      AND Email IS NOT NULL AND LTRIM(RTRIM(Email)) != ''
                """
                query_phone = f"""
                    SELECT COUNT(*) 
                    FROM Estabelecimento
                    WHERE Id IN {estabelecimentos_ids}
                      AND Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != ''
                """
                query_cellphone = f"""
                    SELECT COUNT(*) 
                    FROM Estabelecimento
                    WHERE Id IN {estabelecimentos_ids}
                      AND CelularGerente IS NOT NULL AND LTRIM(RTRIM(CelularGerente)) != ''
                """
            else:
                limit_clause = ""
                if self.limit_rows > 0:
                    limit_clause = f" OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY"
                
                query_email = f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT Id, Email, Telefone, CelularGerente
                        FROM Estabelecimento
                        WHERE (Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
                           OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
                           OR (CelularGerente IS NOT NULL AND LTRIM(RTRIM(CelularGerente)) != '')
                        ORDER BY Id
                        {limit_clause}
                    ) AS limited
                    WHERE Email IS NOT NULL AND LTRIM(RTRIM(Email)) != ''
                """
                query_phone = f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT Id, Email, Telefone, CelularGerente
                        FROM Estabelecimento
                        WHERE (Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
                           OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
                           OR (CelularGerente IS NOT NULL AND LTRIM(RTRIM(CelularGerente)) != '')
                        ORDER BY Id
                        {limit_clause}
                    ) AS limited
                    WHERE Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != ''
                """
                query_cellphone = f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT Id, Email, Telefone, CelularGerente
                        FROM Estabelecimento
                        WHERE (Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
                           OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
                           OR (CelularGerente IS NOT NULL AND LTRIM(RTRIM(CelularGerente)) != '')
                        ORDER BY Id
                        {limit_clause}
                    ) AS limited
                    WHERE CelularGerente IS NOT NULL AND LTRIM(RTRIM(CelularGerente)) != ''
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
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contacts WHERE contactable_type = 'stores'")
            destino_total = cursor_pg.fetchone()[0]
            
            # Contar por tipo
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contacts WHERE contactable_type = 'stores' AND type = 'email'")
            destino_email = cursor_pg.fetchone()[0]
            
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contacts WHERE contactable_type = 'stores' AND type = 'phone'")
            destino_phone = cursor_pg.fetchone()[0]
            
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.contacts WHERE contactable_type = 'stores' AND type = 'cellphone'")
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
        print("\n[ETAPA 7] Limpando contacts de stores...")
        self.delete_polymorphic_table('contacts', 'stores', 'contactable_type')
        
        # Buscar dados do SQL Server
        # Se houver limite, filtrar apenas estabelecimentos que foram migrados
        print("[ETAPA 7] Buscando dados do SQL Server...")
        if self.limit_rows > 0 and self.store_id_map:
            # Buscar apenas estabelecimentos que foram migrados
            estabelecimentos_ids = tuple(self.store_id_map.keys())
            sql_query = f"""
            SELECT 
                Id,
                Email,
                Telefone,
                CelularGerente,
                DataInclusao,
                DataAlteracao
            FROM Estabelecimento
            WHERE Id IN {estabelecimentos_ids}
              AND ((Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
               OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
               OR (CelularGerente IS NOT NULL AND LTRIM(RTRIM(CelularGerente)) != ''))
            ORDER BY Id
            """
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
        
        # Processar em chunks
        schema = get_schema_atual()
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
                print(f"[ETAPA 7] Processando chunk {chunk_num} ({len(rows)} registros)...")
                logger.info(f"[ETAPA 7] Processando chunk {chunk_num} ({len(rows)} registros)...")
                
                chunk_errors = 0
                for row in rows:
                    legado_id = row[0]
                    store_id = self.store_id_map.get(legado_id)
                    
                    if not store_id:
                        continue
                    
                    data_inclusao = row[4]
                    data_alteracao = row[5] if row[5] else row[4]
                    
                    # REGISTRO 1 - Email
                    if row[1] and str(row[1]).strip():
                        try:
                            email_value = str(row[1]).strip().lower()
                            
                            insert_query = f"""
                            INSERT INTO {schema}.contacts (
                                id, contactable_id, contactable_type, type, value,
                                created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """
                            
                            cursor_pg.execute(insert_query, (
                                str(uuid.uuid4()),
                                str(store_id),
                                'stores',
                                'email',
                                email_value,
                                data_inclusao,
                                data_alteracao
                            ))
                            
                            self.stats['contacts'] += 1
                            total_processed += 1
                            
                        except Exception as e:
                            error_msg = f"Erro ao inserir email para Estabelecimento Id={legado_id}: {e}"
                            logger.error(error_msg)
                            print(f"ERRO - {error_msg}")
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            try:
                                conn_pg.rollback()
                                cursor_pg = conn_pg.cursor()
                            except Exception as rollback_error:
                                logger.error(f"Erro ao fazer rollback: {rollback_error}")
                    
                    # REGISTRO 2 - Telefone
                    if row[2] and str(row[2]).strip():
                        try:
                            telefone_value = re.sub(r'[^\d]', '', str(row[2]))
                            
                            if telefone_value:
                                insert_query = f"""
                                INSERT INTO {schema}.contacts (
                                    id, contactable_id, contactable_type, type, value,
                                    created_at, updated_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """
                                
                                cursor_pg.execute(insert_query, (
                                    str(uuid.uuid4()),
                                    str(store_id),
                                    'stores',
                                    'phone',
                                    telefone_value,
                                    data_inclusao,
                                    data_alteracao
                                ))
                                
                                self.stats['contacts'] += 1
                                total_processed += 1
                            
                        except Exception as e:
                            error_msg = f"Erro ao inserir telefone para Estabelecimento Id={legado_id}: {e}"
                            logger.error(error_msg)
                            print(f"ERRO - {error_msg}")
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            try:
                                conn_pg.rollback()
                                cursor_pg = conn_pg.cursor()
                            except Exception as rollback_error:
                                logger.error(f"Erro ao fazer rollback: {rollback_error}")
                    
                    # REGISTRO 3 - Celular Gerente
                    if row[3] and str(row[3]).strip():
                        try:
                            celular_value = re.sub(r'[^\d]', '', str(row[3]))
                            
                            if celular_value:
                                insert_query = f"""
                                INSERT INTO {schema}.contacts (
                                    id, contactable_id, contactable_type, type, value,
                                    created_at, updated_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """
                                
                                cursor_pg.execute(insert_query, (
                                    str(uuid.uuid4()),
                                    str(store_id),
                                    'stores',
                                    'cellphone',
                                    celular_value,
                                    data_inclusao,
                                    data_alteracao
                                ))
                                
                                self.stats['contacts'] += 1
                                total_processed += 1
                            
                        except Exception as e:
                            error_msg = f"Erro ao inserir celular para Estabelecimento Id={legado_id}: {e}"
                            logger.error(error_msg)
                            print(f"ERRO - {error_msg}")
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            try:
                                conn_pg.rollback()
                                cursor_pg = conn_pg.cursor()
                            except Exception as rollback_error:
                                logger.error(f"Erro ao fazer rollback: {rollback_error}")
                
                # Commit do chunk
                try:
                    conn_pg.commit()
                    print(f"[ETAPA 7] Chunk {chunk_num} processado: {total_processed} contacts inseridos, {chunk_errors} erros")
                    logger.info(f"[ETAPA 7] Chunk {chunk_num} processado: {total_processed} contacts inseridos, {chunk_errors} erros")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            print(f"\n[ETAPA 7] CONCLUIDA! Total de contacts migrados: {self.stats['contacts']}")
            logger.info(f"ETAPA 7 concluida: {self.stats['contacts']} registros")
            
            # Validação e relatório de qualidade
            self.validate_step7_contacts()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 7: {e}")
            raise
        finally:
            cursor_sql.close()
            conn_sql.close()
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

