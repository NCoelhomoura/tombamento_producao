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
    # Debug para identificar problemas
    import logging
    debug_logger = logging.getLogger(__name__)
    debug_logger.debug(f"[get_schema_atual] Destino obtido: {destino}")
    if destino == 'PRD':
        debug_logger.debug(f"[get_schema_atual] Retornando schema PRD: {SCHEMA_PRD}")
        return SCHEMA_PRD
    else:
        debug_logger.debug(f"[get_schema_atual] Retornando schema HML: {SCHEMA_HML}")
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
    
    def truncate_table(self, table_name: str, schema: str = None, destino: str = None):
        """
        Faz TRUNCATE em uma tabela
        
        Args:
            table_name: Nome da tabela
            schema: Nome do schema. Se None, será determinado baseado no destino
            destino: 'HML' ou 'PRD'. Se None, tenta obter de DatabaseConnection.get_destino()
        """
        if schema is None:
            if destino is None:
                destino = DatabaseConnection.get_destino()
            schema = SCHEMA_PRD if destino == 'PRD' else SCHEMA_HML
        
        conn = None
        try:
            # ⚠️ CRÍTICO: Usar conexão PRD diretamente quando destino for PRD
            if destino == 'PRD':
                conn = DatabaseConnection.get_postgresql_prd_destino_connection()
            else:
                conn = DatabaseConnection.get_postgresql_hml_destino_connection()
            cursor = conn.cursor()
            
            # Configurar search_path explicitamente para garantir que o schema seja encontrado
            cursor.execute(f"SET search_path TO {schema}, public")
            
            # Tentar TRUNCATE usando o schema explícito primeiro
            query = f'TRUNCATE TABLE "{schema}"."{table_name}" CASCADE'
            try:
                cursor.execute(query)
            except Exception as e1:
                # Se falhar com aspas, tentar sem aspas
                logger.warning(f"[truncate_table] Tentativa com aspas falhou: {e1}. Tentando sem aspas...")
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
                try:
                    conn.rollback()
                except:
                    pass
                conn.close()
            raise
    
    def insert_manual_user(self, destino: str = None):
        """
        Insere o usuário manual (sysadmin) na tabela users após TRUNCATE
        
        Args:
            destino: 'HML' ou 'PRD'. Se None, tenta obter de DatabaseConnection.get_destino()
        """
        if destino is None:
            destino = DatabaseConnection.get_destino()
        schema = SCHEMA_PRD if destino == 'PRD' else SCHEMA_HML
        
        # Dados do usuário manual
        users_manual_insert = {
            'id': 'b1d3a1a3-580b-4db4-92e1-0b7cb66ffe9f',
            'name': 'Sys Admin',
            'temporary_password': False,
            'email_confirmed_at': datetime(2025, 11, 2, 21, 0, 0),
            'created_at': datetime(2025, 11, 2, 21, 0, 0),
            'updated_at': datetime(2025, 11, 2, 21, 0, 0),
            'deleted_at': None,
            'status': 'active',
            'user_name': 'sysadmin@superaholdings.com.br',
            'normalized_user_name': 'SYSADMIN@SUPERAHOLDINGS.COM.BR',
            'email': 'sysadmin@superaholdings.com.br',
            'normalized_email': 'SYSADMIN@SUPERAHOLDINGS.COM.BR',
            'email_confirmed': True,
            'password_hash': 'AQAAAAIAAYagAAAAEGoSbamR81zarXFRUKHMViwspBPUvtVxw+CEvAuu3iV2pC/0nQzbmxX+P6RBr18acw==',
            'security_stamp': 'Z6EA7HANNVN6UJ4S7HJLRMZJVG5YWQ3G',
            'concurrency_stamp': 'fb407352-8645-4895-aaad-afd8cad924bc',
            'phone_number': None,
            'phone_number_confirmed': False,
            'two_factor_enabled': False,
            'lockout_end': None,
            'lockout_enabled': False,
            'access_failed_count': 0,
            'legacy_id': None
        }
        
        conn = None
        try:
            print("[ETAPA 1] Inserindo usuário manual (Sys Admin)...")
            
            # ⚠️ CRÍTICO: Usar conexão PRD diretamente quando destino for PRD
            if destino == 'PRD':
                conn = DatabaseConnection.get_postgresql_prd_destino_connection()
            else:
                conn = DatabaseConnection.get_postgresql_hml_destino_connection()
            cursor = conn.cursor()
            
            # Configurar search_path
            cursor.execute(f"SET search_path TO {schema}, public")
            
            # Verificar se deve incluir legacy_id
            include_legacy = self.should_include_legacy_id()
            
            if include_legacy:
                insert_query = f"""
                INSERT INTO "{schema}"."users" (
                    id, legacy_id, name, user_name, normalized_user_name, email, normalized_email,
                    phone_number, password_hash, status, created_at, updated_at, deleted_at,
                    email_confirmed, email_confirmed_at, phone_number_confirmed, temporary_password,
                    two_factor_enabled, lockout_enabled, lockout_end, access_failed_count,
                    security_stamp, concurrency_stamp
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """
                cursor.execute(insert_query, (
                    users_manual_insert['id'],
                    users_manual_insert['legacy_id'],
                    users_manual_insert['name'],
                    users_manual_insert['user_name'],
                    users_manual_insert['normalized_user_name'],
                    users_manual_insert['email'],
                    users_manual_insert['normalized_email'],
                    users_manual_insert['phone_number'],
                    users_manual_insert['password_hash'],
                    users_manual_insert['status'],
                    users_manual_insert['created_at'],
                    users_manual_insert['updated_at'],
                    users_manual_insert['deleted_at'],
                    users_manual_insert['email_confirmed'],
                    users_manual_insert['email_confirmed_at'],
                    users_manual_insert['phone_number_confirmed'],
                    users_manual_insert['temporary_password'],
                    users_manual_insert['two_factor_enabled'],
                    users_manual_insert['lockout_enabled'],
                    users_manual_insert['lockout_end'],
                    users_manual_insert['access_failed_count'],
                    users_manual_insert['security_stamp'],
                    users_manual_insert['concurrency_stamp']
                ))
            else:
                insert_query = f"""
                INSERT INTO "{schema}"."users" (
                    id, name, user_name, normalized_user_name, email, normalized_email,
                    phone_number, password_hash, status, created_at, updated_at, deleted_at,
                    email_confirmed, email_confirmed_at, phone_number_confirmed, temporary_password,
                    two_factor_enabled, lockout_enabled, lockout_end, access_failed_count,
                    security_stamp, concurrency_stamp
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """
                cursor.execute(insert_query, (
                    users_manual_insert['id'],
                    users_manual_insert['name'],
                    users_manual_insert['user_name'],
                    users_manual_insert['normalized_user_name'],
                    users_manual_insert['email'],
                    users_manual_insert['normalized_email'],
                    users_manual_insert['phone_number'],
                    users_manual_insert['password_hash'],
                    users_manual_insert['status'],
                    users_manual_insert['created_at'],
                    users_manual_insert['updated_at'],
                    users_manual_insert['deleted_at'],
                    users_manual_insert['email_confirmed'],
                    users_manual_insert['email_confirmed_at'],
                    users_manual_insert['phone_number_confirmed'],
                    users_manual_insert['temporary_password'],
                    users_manual_insert['two_factor_enabled'],
                    users_manual_insert['lockout_enabled'],
                    users_manual_insert['lockout_end'],
                    users_manual_insert['access_failed_count'],
                    users_manual_insert['security_stamp'],
                    users_manual_insert['concurrency_stamp']
                ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print("OK - Usuário manual (Sys Admin) inserido com sucesso")
            logger.info(f"[ETAPA 1] Usuário manual (Sys Admin) inserido: {users_manual_insert['user_name']}")
            
        except Exception as e:
            logger.error(f"Erro ao inserir usuário manual: {e}")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
                conn.close()
            raise
    
    def validate_step1_users(self, destino: str = None):
        """
        Validação e relatório de qualidade - ETAPA 1
        
        Args:
            destino: 'HML' ou 'PRD'. Se None, tenta obter de DatabaseConnection.get_destino()
        """
        try:
            if destino is None:
                destino = DatabaseConnection.get_destino()
            schema = SCHEMA_PRD if destino == 'PRD' else SCHEMA_HML
            
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
            # ⚠️ CRÍTICO: Usar conexão PRD diretamente quando destino for PRD
            if destino == 'PRD':
                conn_pg = DatabaseConnection.get_postgresql_prd_destino_connection()
            else:
                conn_pg = DatabaseConnection.get_postgresql_hml_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            # Configurar search_path
            cursor_pg.execute(f"SET search_path TO {schema}, public")
            
            cursor_pg.execute(f'SELECT COUNT(*) FROM "{schema}"."users"')
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
    
    def step1_migrate_users(self, destino: str = None):
        """
        ETAPA 1: Migrar users
        
        Args:
            destino: 'HML' ou 'PRD'. Se None, tenta obter de DatabaseConnection.get_destino()
        """
        # ========================================================================
        # DETERMINAÇÃO DO DESTINO E SCHEMA
        # ========================================================================
        # Prioridade:
        # 1. Parâmetro destino (explicito)
        # 2. DatabaseConnection.get_destino() (fallback)
        
        # 1. Determinar destino
        if destino is None:
            destino = DatabaseConnection.get_destino()
            logger.info(f"[DESTINO] Destino obtido de DatabaseConnection.get_destino(): {destino}")
        else:
            destino = destino.upper()
            if destino not in ['HML', 'PRD']:
                raise ValueError(f"Destino invalido: {destino}. Use 'HML' ou 'PRD'")
            logger.info(f"[DESTINO] Destino recebido como parâmetro: {destino}")
        
        # 2. Determinar schema baseado no destino (mapeamento direto)
        # Mapeamento conforme definição:
        # - HML → gmcore (definido em SCHEMA_HML e POSTGRESQL_HML_DESTINO_CONFIG)
        # - PRD → core (definido em SCHEMA_PRD e POSTGRESQL_PRD_DESTINO_CONFIG)
        if destino == 'PRD':
            schema = SCHEMA_PRD  # 'core'
        else:
            schema = SCHEMA_HML  # 'gmcore'
        
        logger.info(f"[DESTINO] Schema determinado: {schema} (baseado em destino={destino})")
        
        # 3. Validação de consistência: garantir que destino e schema estão alinhados
        if destino == 'PRD' and schema != 'core':
            logger.error(f"[DESTINO] ⚠️ ERRO DE INCONSISTÊNCIA: Destino=PRD mas Schema={schema}. Forçando schema=core")
            schema = 'core'
        elif destino == 'HML' and schema != 'gmcore':
            logger.error(f"[DESTINO] ⚠️ ERRO DE INCONSISTÊNCIA: Destino=HML mas Schema={schema}. Forçando schema=gmcore")
            schema = 'gmcore'
        
        logger.info(f"[DESTINO] ✅ Schema final validado: {schema} para destino: {destino}")
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
                # ⚠️ CRÍTICO: Usar conexão PRD diretamente quando destino for PRD
                if destino == 'PRD':
                    conn_check = DatabaseConnection.get_postgresql_prd_destino_connection()
                else:
                    conn_check = DatabaseConnection.get_postgresql_hml_destino_connection()
                cursor_check = conn_check.cursor()
                
                # Configurar search_path
                cursor_check.execute(f"SET search_path TO {schema}, public")
                
                cursor_check.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = %s 
                    AND table_name = 'users' 
                    AND column_name = 'legacy_id'
                """, (schema,))
                if not cursor_check.fetchone():
                    print("[ETAPA 1] Criando coluna legacy_id...")
                    # ⚠️ NUNCA criar colunas em PRD - apenas verificar
                    if destino == 'PRD':
                        logger.warning(f"[ETAPA 1] Tentativa de criar coluna legacy_id em PRD ignorada (não permitido)")
                        print("[ETAPA 1] Coluna legacy_id não existe, mas criação em PRD não é permitida")
                    else:
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
        self.truncate_table('users', destino=destino)
        
        # Inserir usuário manual (Sys Admin) após TRUNCATE
        self.insert_manual_user(destino=destino)
        
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
        # ⚠️ CRÍTICO: Usar conexão PRD diretamente quando destino for PRD para garantir contexto correto
        if destino == 'PRD':
            conn_pg = DatabaseConnection.get_postgresql_prd_destino_connection()
        else:
            conn_pg = DatabaseConnection.get_postgresql_hml_destino_connection()
        cursor_pg = conn_pg.cursor()
        
        # Configurar search_path para garantir acesso ao schema correto
        cursor_pg.execute(f"SET search_path TO {schema}, public")
        
        # Verificar se a tabela existe antes de tentar inserir
        cursor_pg.execute("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = 'users'
            )
        """, (schema,))
        table_exists = cursor_pg.fetchone()[0]
        
        if not table_exists:
            error_msg = f"Tabela '{schema}.users' não existe no banco de dados"
            logger.error(f"[ERRO] {error_msg}")
            raise ValueError(error_msg)
        
        logger.info(f"[VERIFICAÇÃO] Tabela '{schema}.users' existe e está acessível")
        
        # Preparar query e dados baseado em include_legacy
        if include_legacy:
            insert_query = f"""
            INSERT INTO "{schema}"."users" (
                id, legacy_id, name, user_name, normalized_user_name, email, normalized_email,
                phone_number, password_hash, status, created_at, updated_at, deleted_at,
                email_confirmed, email_confirmed_at, phone_number_confirmed, temporary_password,
                two_factor_enabled, lockout_enabled, lockout_end, access_failed_count,
                security_stamp, concurrency_stamp
            ) VALUES %s
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
                        # Usar execute_values para inserção otimizada em bulk
                        execute_values(
                            cursor_pg,
                            insert_query,
                            chunk,
                            template=insert_template,
                            page_size=CHUNK_SIZE,
                            fetch=False
                        )
                        
                        conn_pg.commit()
                        total_processed += len(chunk)
                        self.stats['users'] += len(chunk)
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
            
            print(f"\n[ETAPA 1] CONCLUIDA! Total de users migrados: {self.stats['users']}")
            logger.info(f"[ETAPA 1] CONCLUIDA! Total: {self.stats['users']}")
            
            # Validação
            self.validate_step1_users(destino=destino)
            
        except Exception as e:
            logger.error(f"Erro na ETAPA 1: {e}")
            print(f"ERRO na ETAPA 1: {e}")
            import traceback
            traceback.print_exc()
            if 'conn_pg' in locals():
                conn_pg.rollback()
                conn_pg.close()
            raise
    
    def step2_migrate_user_roles(self, destino: str = None):
        """
        ETAPA 2: Migrar user_roles
        
        Args:
            destino: 'HML' ou 'PRD'. Se None, tenta obter de DatabaseConnection.get_destino()
        """
        # Determinar destino e schema
        if destino is None:
            destino = DatabaseConnection.get_destino()
            logger.info(f"[DESTINO] Destino obtido de DatabaseConnection.get_destino(): {destino}")
        else:
            destino = destino.upper()
            if destino not in ['HML', 'PRD']:
                raise ValueError(f"Destino invalido: {destino}. Use 'HML' ou 'PRD'")
            logger.info(f"[DESTINO] Destino recebido como parâmetro: {destino}")
        
        if destino == 'PRD':
            schema = SCHEMA_PRD  # 'core'
        else:
            schema = SCHEMA_HML  # 'gmcore'
        
        logger.info(f"[DESTINO] Schema determinado: {schema} (baseado em destino={destino})")
        
        print("\n" + "="*80)
        print("ETAPA 2: MIGRANDO USER_ROLES")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 2: Migrando user_roles")
        logger.info(f"Ambiente: {destino} | Schema: {schema}")
        logger.info("="*80)
        
        # UUID fixo da role HUNTER
        ROLE_HUNTER_UUID = '019bb283-9894-75e4-9c15-84491fb67224'
        
        # Array com registro manual
        manual_user_role = {
            'user_id': 'b1d3a1a3-580b-4db4-92e1-0b7cb66ffe9f',
            'role_id': '10f04eee-b8ff-4aa2-9f8a-0f2531573d32',
        }
        
        conn_pg = None
        try:
            # ⚠️ CRÍTICO: Usar conexão PRD diretamente quando destino for PRD
            if destino == 'PRD':
                conn_pg = DatabaseConnection.get_postgresql_prd_destino_connection()
            else:
                conn_pg = DatabaseConnection.get_postgresql_hml_destino_connection()
            cursor_pg = conn_pg.cursor()
            
            # Configurar search_path
            cursor_pg.execute(f"SET search_path TO {schema}, public")
            
            # Verificar se a tabela existe
            cursor_pg.execute("""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_name = 'user_roles'
                )
            """, (schema,))
            table_exists = cursor_pg.fetchone()[0]
            
            if not table_exists:
                error_msg = f"Tabela '{schema}.user_roles' não existe no banco de dados"
                logger.error(f"[ERRO] {error_msg}")
                raise ValueError(error_msg)
            
            logger.info(f"[VERIFICAÇÃO] Tabela '{schema}.user_roles' existe e está acessível")
            
            # Verificar e garantir que os roles existem antes de inserir
            print("\n[ETAPA 2] Verificando se os roles existem na tabela roles...")
            
            # Verificar se a tabela roles existe (pode estar em schema diferente)
            # Tentar encontrar a tabela roles em diferentes schemas possíveis
            roles_table_schema = None
            roles_table_name = None
            
            # Verificar em diferentes schemas possíveis
            possible_schemas = [schema, 'public', 'asp_net_roles', 'identity']
            for check_schema in possible_schemas:
                try:
                    cursor_pg.execute("""
                        SELECT EXISTS (
                            SELECT 1 
                            FROM information_schema.tables 
                            WHERE table_schema = %s 
                            AND table_name = 'roles'
                        )
                    """, (check_schema,))
                    if cursor_pg.fetchone()[0]:
                        roles_table_schema = check_schema
                        roles_table_name = 'roles'
                        logger.info(f"[ETAPA 2] Tabela roles encontrada em schema: {check_schema}")
                        break
                except Exception:
                    continue
            
            if not roles_table_schema:
                # Tentar buscar em asp_net_roles (nome completo da constraint sugere isso)
                try:
                    cursor_pg.execute("""
                        SELECT table_schema, table_name
                        FROM information_schema.tables 
                        WHERE table_name LIKE '%role%'
                        AND table_schema IN ('public', %s, 'asp_net_roles', 'identity')
                        LIMIT 1
                    """, (schema,))
                    result = cursor_pg.fetchone()
                    if result:
                        roles_table_schema, roles_table_name = result
                        logger.info(f"[ETAPA 2] Tabela de roles encontrada: {roles_table_schema}.{roles_table_name}")
                except Exception as e:
                    logger.warning(f"[ETAPA 2] Não foi possível encontrar tabela roles: {e}")
            
            # Verificar se o role do registro manual existe
            role_id_to_use = None
            if roles_table_schema and roles_table_name:
                try:
                    # Verificar se o role_id do registro manual existe
                    check_role_query = f"""
                        SELECT id 
                        FROM "{roles_table_schema}"."{roles_table_name}"
                        WHERE id = %s
                    """
                    cursor_pg.execute(check_role_query, (manual_user_role['role_id'],))
                    if cursor_pg.fetchone():
                        role_id_to_use = manual_user_role['role_id']
                        print(f"[ETAPA 2] Role do registro manual encontrado: {role_id_to_use}")
                        logger.info(f"[ETAPA 2] Role do registro manual encontrado: {role_id_to_use}")
                    else:
                        # Verificar se o ROLE_HUNTER_UUID existe
                        cursor_pg.execute(check_role_query, (ROLE_HUNTER_UUID,))
                        if cursor_pg.fetchone():
                            role_id_to_use = ROLE_HUNTER_UUID
                            print(f"[ETAPA 2] Role HUNTER encontrado: {role_id_to_use}")
                            logger.info(f"[ETAPA 2] Role HUNTER encontrado: {role_id_to_use}")
                        else:
                            # Buscar qualquer role existente
                            cursor_pg.execute(f"""
                                SELECT id 
                                FROM "{roles_table_schema}"."{roles_table_name}"
                                LIMIT 1
                            """)
                            result = cursor_pg.fetchone()
                            if result:
                                role_id_to_use = str(result[0])
                                print(f"[ETAPA 2] Usando role existente encontrado: {role_id_to_use}")
                                logger.warning(f"[ETAPA 2] Roles especificados não encontrados. Usando role existente: {role_id_to_use}")
                            else:
                                error_msg = f"Nenhum role encontrado na tabela {roles_table_schema}.{roles_table_name}. É necessário criar roles antes de migrar user_roles."
                                logger.error(f"[ERRO] {error_msg}")
                                raise ValueError(error_msg)
                except Exception as e:
                    logger.warning(f"[ETAPA 2] Erro ao verificar roles: {e}. Tentando usar role_id do registro manual...")
                    role_id_to_use = manual_user_role['role_id']
            else:
                # Se não encontrou a tabela roles, usar o role_id do registro manual que funcionou
                logger.warning("[ETAPA 2] Tabela roles não encontrada. Usando role_id do registro manual.")
                role_id_to_use = manual_user_role['role_id']
            
            # Validar que role_id_to_use foi definido
            if not role_id_to_use:
                error_msg = "Não foi possível determinar um role_id válido. Verifique se a tabela roles existe e contém roles."
                logger.error(f"[ERRO] {error_msg}")
                raise ValueError(error_msg)
            
            # Atualizar o role_id do registro manual para usar o mesmo role
            manual_user_role['role_id'] = role_id_to_use
            print(f"[ETAPA 2] Role_id que será usado: {role_id_to_use}")
            logger.info(f"[ETAPA 2] Role_id que será usado: {role_id_to_use}")
            
            # Limpar tabela (TRUNCATE)
            print("\n[ETAPA 2] Limpando tabela user_roles...")
            try:
                cursor_pg.execute(f'TRUNCATE TABLE "{schema}"."user_roles" CASCADE')
            except Exception as e1:
                logger.warning(f"[ETAPA 2] Tentativa com aspas falhou: {e1}. Tentando sem aspas...")
                cursor_pg.execute(f"TRUNCATE TABLE {schema}.user_roles CASCADE")
            conn_pg.commit()
            print("OK - Tabela user_roles truncada")
            logger.info(f"Tabela {schema}.user_roles truncada com sucesso")
            
            # 1. Inserir registro manual primeiro
            print("\n[ETAPA 2] Inserindo registro manual...")
            manual_insert_query = f"""
            INSERT INTO "{schema}"."user_roles" (
                user_id, role_id
            ) VALUES (
                %s, %s
            )
            """
            cursor_pg.execute(manual_insert_query, (
                manual_user_role['user_id'],
                manual_user_role['role_id']
            ))
            conn_pg.commit()
            print(f"OK - Registro manual inserido: user_id={manual_user_role['user_id']}, role_id={manual_user_role['role_id']}")
            logger.info(f"[ETAPA 2] Registro manual inserido: user_id={manual_user_role['user_id']}, role_id={manual_user_role['role_id']}")
            
            # 2. Buscar todos os usuários da origem (exceto o user_id do array manual)
            print("\n[ETAPA 2] Buscando usuários da tabela users (excluindo registro manual)...")
            users_query = f"""
            SELECT id
            FROM "{schema}"."users"
            WHERE id != %s
            ORDER BY id
            """
            cursor_pg.execute(users_query, (manual_user_role['user_id'],))
            all_users = cursor_pg.fetchall()
            
            if not all_users:
                print("[ETAPA 2] Nenhum usuário encontrado para migrar (exceto o manual)")
                logger.warning("[ETAPA 2] Nenhum usuário encontrado para migrar")
                cursor_pg.close()
                conn_pg.close()
                return
            
            print(f"[ETAPA 2] {len(all_users)} usuários encontrados. Criando registros em user_roles...")
            
            # Preparar dados para inserção em lote
            insert_query = f"""
            INSERT INTO "{schema}"."user_roles" (
                user_id, role_id
            ) VALUES %s
            """
            insert_template = f"(%s, %s)"
            
            # Criar lista de tuplas com todos os usuários (exceto o manual) associados ao role verificado
            # Usar o mesmo role_id que foi usado no registro manual (já verificado que existe)
            processed_tuples = []
            for (user_id,) in all_users:
                processed_tuples.append((
                    str(user_id),
                    role_id_to_use  # Usar o role_id verificado que existe
                ))
            
            # Inserir em chunks
            chunk_num = 0
            total_processed = 0
            
            for i in range(0, len(processed_tuples), CHUNK_SIZE):
                chunk = processed_tuples[i:i + CHUNK_SIZE]
                chunk_num += 1
                
                if chunk:
                    try:
                        # Usar execute_values para inserção otimizada em bulk
                        execute_values(
                            cursor_pg,
                            insert_query,
                            chunk,
                            template=insert_template,
                            page_size=CHUNK_SIZE,
                            fetch=False
                        )
                        
                        conn_pg.commit()
                        total_processed += len(chunk)
                        print(f"[ETAPA 2] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                        logger.info(f"[ETAPA 2] Chunk {chunk_num} processado: {total_processed}/{len(processed_tuples)} registros inseridos")
                        
                    except Exception as e:
                        conn_pg.rollback()
                        logger.error(f"Erro ao inserir batch de user_roles: {e}")
                        print(f"ERRO ao inserir batch: {e}")
                        raise
            
            cursor_pg.close()
            conn_pg.close()
            
            total_records = len(processed_tuples) + 1  # +1 para o registro manual
            print(f"\n[ETAPA 2] CONCLUIDA! Total de user_roles migrados: {total_records} (1 manual + {len(processed_tuples)} da origem)")
            logger.info(f"[ETAPA 2] CONCLUIDA! Total: {total_records} (1 manual + {len(processed_tuples)} da origem)")
            
        except Exception as e:
            logger.error(f"Erro na ETAPA 2: {e}")
            print(f"ERRO na ETAPA 2: {e}")
            import traceback
            traceback.print_exc()
            if conn_pg:
                conn_pg.rollback()
                conn_pg.close()
            raise



