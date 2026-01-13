"""
Script de migração de dados: SQL Server PRD -> PostgreSQL Destino
Migra dados da tabela: users
"""

import sys
import os
import uuid
import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from psycopg2.extras import execute_values

# Adicionar diretório utils ao path
utils_path = os.path.join(os.path.dirname(__file__), '..', 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)
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

# Handler para arquivo
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

# Handler para console
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


class UsersMigration:
    """Classe para executar a migração de dados de users"""
    
    def __init__(self, limit_rows=0):
        self.stats = {
            'users': 0,
            'errors': []
        }
        self.user_id_map = {}  # Map: legacy_id -> uuid
        self.limit_rows = limit_rows  # 0 = todos, > 0 = limitar quantidade
    
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
    
    def clean_string_vectorized(self, series: pd.Series, max_length: Optional[int] = None) -> pd.Series:
        """Limpa e trunca strings de forma vetorizada"""
        cleaned = series.astype(str)
        cleaned = cleaned.replace(['nan', 'None', 'NULL', 'NONE'], '')
        cleaned = cleaned.str.strip()
        cleaned = cleaned.replace(['', 'null', 'none'], None)
        if max_length:
            cleaned = cleaned.str[:max_length]
        return cleaned
    
    def convert_status(self, ativo: Optional[bool]) -> str:
        """Converte status booleano para string"""
        if ativo is True:
            return 'active'
        return 'inactive'
    
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
    
    def validate_step1_users(self):
        """Validação e relatório de qualidade - ETAPA 1"""
        try:
            destino = DatabaseConnection.get_destino()
            schema = get_schema_atual()
            
            # Contar origem
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            if self.limit_rows > 0:
                cursor_sql.execute(f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT TOP {self.limit_rows} Id 
                        FROM Usuario 
                        ORDER BY Id
                    ) AS limited
                """)
            else:
                cursor_sql.execute("SELECT COUNT(*) FROM Usuario")
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            conn_pg = DatabaseConnection.get_postgresql_destino_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute(f"SELECT COUNT(*) FROM {schema}.users")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - Usuario):")
            print(f"  Total de registros: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL {destino} - {schema}.users):")
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
    
    def step1_migrate_users(self):
        """ETAPA 1: Migrar users"""
        destino = DatabaseConnection.get_destino()
        schema = get_schema_atual()
        print("\n" + "="*80)
        print("ETAPA 1: MIGRANDO USERS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 1: Migrando users")
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
                    AND table_name = 'users' 
                    AND column_name = 'legacy_id'
                """)
                if not cursor_check.fetchone():
                    print("[ETAPA 1] Criando coluna legacy_id...")
                    cursor_check.execute(f"ALTER TABLE {schema}.users ADD COLUMN legacy_id INTEGER")
                    conn_check.commit()
                    print("OK - Coluna legacy_id criada")
                else:
                    print("[ETAPA 1] Coluna legacy_id ja existe na tabela")
                cursor_check.close()
                conn_check.close()
            except Exception as e:
                logger.warning(f"Nao foi possivel criar/verificar coluna legacy_id: {e}")
        
        # Truncate
        print("\n[ETAPA 1] Limpando tabela users...")
        self.truncate_table('users')
        
        # Buscar dados do SQL Server
        print("[ETAPA 1] Buscando dados do SQL Server...")
        sql_query = """
        SELECT 
            Id,
            Nome,
            Login,
            Email,
            Telefone,
            Senha,
            Ativo,
            DataInclusao,
            DataAlteracao
        FROM Usuario
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
        df = pd.DataFrame.from_records(all_rows, columns=[
            'Id', 'Nome', 'Login', 'Email', 'Telefone', 'Senha', 'Ativo', 'DataInclusao', 'DataAlteracao'
        ])
        
        # Aplicar transformações vetorizadas
        df['legacy_id'] = df['Id']
        
        # Limpar e normalizar strings
        df['name'] = self.clean_string_vectorized(df['Nome'], max_length=None)
        df['user_name'] = self.clean_string_vectorized(df['Login'], max_length=256)
        df['normalized_user_name'] = df['user_name'].str.upper() if df['user_name'].notna().any() else None
        df['email'] = self.clean_string_vectorized(df['Email'], max_length=256)
        df['normalized_email'] = df['email'].str.lower() if df['email'].notna().any() else None
        df['phone_number'] = self.clean_string_vectorized(df['Telefone'], max_length=None)
        df['password_hash'] = self.clean_string_vectorized(df['Senha'], max_length=None)
        
        # Converter status
        df['status'] = df['Ativo'].apply(self.convert_status)
        
        # Datas
        df['created_at'] = df['DataInclusao']
        df['updated_at'] = df['DataAlteracao'].fillna(df['DataInclusao'])
        df['email_confirmed_at'] = df['DataInclusao']
        
        # Valores padrão
        df['temporary_password'] = False
        df['email_confirmed'] = True
        df['phone_number_confirmed'] = False
        df['two_factor_enabled'] = False
        df['lockout_enabled'] = True
        df['access_failed_count'] = 0
        df['deleted_at'] = None
        df['lockout_end'] = None
        df['security_stamp'] = None
        df['concurrency_stamp'] = None
        
        # Remover linhas com erros (name None após limpeza)
        df = df[df['name'].notna()]
        
        print(f"[ETAPA 1] {len(df)} registros processados. Inserindo no banco (otimizado com execute_values)...")
        
        # Conectar ao PostgreSQL
        conn_pg = DatabaseConnection.get_postgresql_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Preparar query e dados baseado em include_legacy
        if include_legacy:
            insert_query = f"""
            INSERT INTO {schema}.users (
                id, legacy_id, name, user_name, normalized_user_name, email, normalized_email,
                phone_number, password_hash, status, created_at, updated_at, deleted_at,
                email_confirmed, email_confirmed_at, phone_number_confirmed, temporary_password,
                two_factor_enabled, lockout_enabled, lockout_end, access_failed_count,
                security_stamp, concurrency_stamp
            ) VALUES %s
            RETURNING id, legacy_id
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            processed_tuples = list(zip(
                df['legacy_id'].tolist(),
                df['name'].tolist(),
                df['user_name'].tolist(),
                df['normalized_user_name'].tolist(),
                df['email'].tolist(),
                df['normalized_email'].tolist(),
                df['phone_number'].tolist(),
                df['password_hash'].tolist(),
                df['status'].tolist(),
                df['created_at'].tolist(),
                df['updated_at'].tolist(),
                df['deleted_at'].tolist(),
                df['email_confirmed'].tolist(),
                df['email_confirmed_at'].tolist(),
                df['phone_number_confirmed'].tolist(),
                df['temporary_password'].tolist(),
                df['two_factor_enabled'].tolist(),
                df['lockout_enabled'].tolist(),
                df['lockout_end'].tolist(),
                df['access_failed_count'].tolist(),
                df['security_stamp'].tolist(),
                df['concurrency_stamp'].tolist()
            ))
            all_legacy_ids_inserted = df['legacy_id'].tolist()
        else:
            insert_query = f"""
            INSERT INTO {schema}.users (
                id, name, user_name, normalized_user_name, email, normalized_email,
                phone_number, password_hash, status, created_at, updated_at, deleted_at,
                email_confirmed, email_confirmed_at, phone_number_confirmed, temporary_password,
                two_factor_enabled, lockout_enabled, lockout_end, access_failed_count,
                security_stamp, concurrency_stamp
            ) VALUES %s
            """
            insert_template = f"(gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            processed_tuples = list(zip(
                df['name'].tolist(),
                df['user_name'].tolist(),
                df['normalized_user_name'].tolist(),
                df['email'].tolist(),
                df['normalized_email'].tolist(),
                df['phone_number'].tolist(),
                df['password_hash'].tolist(),
                df['status'].tolist(),
                df['created_at'].tolist(),
                df['updated_at'].tolist(),
                df['deleted_at'].tolist(),
                df['email_confirmed'].tolist(),
                df['email_confirmed_at'].tolist(),
                df['phone_number_confirmed'].tolist(),
                df['temporary_password'].tolist(),
                df['two_factor_enabled'].tolist(),
                df['lockout_enabled'].tolist(),
                df['lockout_end'].tolist(),
                df['access_failed_count'].tolist(),
                df['security_stamp'].tolist(),
                df['concurrency_stamp'].tolist()
            ))
            all_legacy_ids_inserted = []
        
        chunk_num = 0
        total_processed = 0
        
        try:
            for i in range(0, len(processed_tuples), CHUNK_SIZE):
                chunk = processed_tuples[i:i + CHUNK_SIZE]
                chunk_num += 1
                
                if chunk:
                    try:
                        if include_legacy:
                            # Usar execute_values com RETURNING para obter UUIDs gerados
                            execute_values(
                                cursor_pg,
                                insert_query,
                                chunk,
                                template=insert_template,
                                page_size=CHUNK_SIZE,
                                fetch=True
                            )
                            # Obter UUIDs retornados
                            returned_rows = cursor_pg.fetchall()
                            for uuid_row, leg_id in returned_rows:
                                self.user_id_map[leg_id] = uuid_row
                        else:
                            # Sem legacy_id, inserir normalmente
                            execute_values(
                                cursor_pg,
                                insert_query,
                                chunk,
                                template=insert_template,
                                page_size=CHUNK_SIZE
                            )
                        
                        conn_pg.commit()
                        total_processed += len(chunk)
                        print(f"[ETAPA 1] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                        logger.info(f"[ETAPA 1] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                        
                    except Exception as e:
                        conn_pg.rollback()
                        logger.error(f"Erro ao inserir batch de users: {e}")
                        print(f"ERRO ao inserir batch: {e}")
                        raise
            
            # Se não usou RETURNING, buscar UUIDs gerados após todas as inserções
            if include_legacy and all_legacy_ids_inserted:
                print(f"[ETAPA 1] Buscando UUIDs gerados para {len(all_legacy_ids_inserted)} registros...")
                cursor_pg.execute(f"""
                    SELECT id, legacy_id 
                    FROM {schema}.users 
                    WHERE legacy_id = ANY(%s)
                """, (all_legacy_ids_inserted,))
                for uuid_row, leg_id in cursor_pg.fetchall():
                    self.user_id_map[leg_id] = uuid_row
                print(f"[ETAPA 1] {len(self.user_id_map)} UUIDs mapeados")
                logger.info(f"[ETAPA 1] {len(self.user_id_map)} UUIDs mapeados no user_id_map")
            
            cursor_pg.close()
            conn_pg.close()
            
            self.stats['users'] = total_processed
            
            print(f"\n[ETAPA 1] CONCLUIDA! Total de users migrados: {self.stats['users']}")
            logger.info(f"[ETAPA 1] CONCLUIDA! Total: {self.stats['users']}")
            
            # Validação
            self.validate_step1_users()
            
        except Exception as e:
            logger.error(f"Erro na ETAPA 1: {e}")
            print(f"ERRO na ETAPA 1: {e}")
            import traceback
            traceback.print_exc()
            if 'conn_pg' in locals():
                conn_pg.rollback()
                conn_pg.close()
            raise



