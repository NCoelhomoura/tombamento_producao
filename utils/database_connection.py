"""
Arquivo de configuração de conexão com databases
- SQL Server PRD (origem - apenas leitura)
- PostgreSQL PRD (origem schemas/tabelas - apenas leitura)
- PostgreSQL HML (destino - leitura/escrita, schema gmcore)
"""

import pyodbc
import psycopg2
from psycopg2 import pool
from typing import Optional
import os


class DatabaseConnection:
    """Classe para gerenciar conexões com os databases"""
    
    # Configurações SQL Server PRD (Origem - APENAS LEITURA)
    SQL_SERVER_PRD_CONFIG = {
        'host': 'ec2-54-207-169-15.sa-east-1.compute.amazonaws.com',
        'database': 'FINANCEIRO',
        'user': 'azure_datalake',
        'password': 'CFcoY5oDtduRsKWO1f',
        'driver': '{ODBC Driver 17 for SQL Server}'  # Ajuste conforme necessário
    }
    
    # Configurações PostgreSQL PRD (Origem Schemas/Tabelas - APENAS LEITURA)
    POSTGRESQL_PRD_CONFIG = {
        'host': 'gmcore-eks-dev-postgres.ckksg9kcwfzj.us-east-2.rds.amazonaws.com',
        'database': 'gmcoredb',
        'port': 5432,
        'user': 'postgres',
        'password': 'lmlVyIGz8eWT6iBtzLJU'
    }
    
    # Configurações PostgreSQL HML (Destino - Leitura/Escrita, schema gmcore)
    POSTGRESQL_HML_DESTINO_CONFIG = {
        'host': 'apgsql-gmpromo-prd.eastus.cloudapp.azure.com',
        'database': 'supera_dev_seed',
        'schema': 'gmcore',
        'port': 5432,
        'user': 'postgres',
        'password': 'Taztaz@21'
    }
    
    # Configurações PostgreSQL PRD AWS (Destino - Leitura/Escrita, schema core)
    # Credenciais do DESTINO SCHEMAS TABELAS PRD (diretrizes_migracao.txt)
    POSTGRESQL_PRD_DESTINO_CONFIG = {
        'host': 'gmcore-eks-dev-postgres.ckksg9kcwfzj.us-east-2.rds.amazonaws.com',
        'database': 'gmcoredb',
        'schema': 'core',
        'port': 5432,
        'user': 'postgres',
        'password': 'lmlVyIGz8eWT6iBtzLJU'
    }
    
    # Chaveador de destino: 'HML' ou 'PRD'
    # Pode ser alterado via variável de ambiente MIGRATION_DESTINO ou método set_destino()
    _destino_atual = 'PRD'  # Padrão: PRD (alterado de HML)
    
    @staticmethod
    def get_sql_server_prd_connection():
        """
        Cria e retorna uma conexão com SQL Server PRD (APENAS LEITURA)
        
        Returns:
            pyodbc.Connection: Conexão com SQL Server PRD
        """
        try:
            connection_string = (
                f"DRIVER={DatabaseConnection.SQL_SERVER_PRD_CONFIG['driver']};"
                f"SERVER={DatabaseConnection.SQL_SERVER_PRD_CONFIG['host']};"
                f"DATABASE={DatabaseConnection.SQL_SERVER_PRD_CONFIG['database']};"
                f"UID={DatabaseConnection.SQL_SERVER_PRD_CONFIG['user']};"
                f"PWD={DatabaseConnection.SQL_SERVER_PRD_CONFIG['password']};"
                "TrustServerCertificate=yes;"
            )
            conn = pyodbc.connect(connection_string)
            return conn
        except Exception as e:
            print(f"Erro ao conectar com SQL Server PRD: {e}")
            raise
    
    @staticmethod
    def get_postgresql_prd_connection():
        """
        Cria e retorna uma conexão com PostgreSQL PRD (APENAS LEITURA)
        
        Returns:
            psycopg2.extensions.connection: Conexão com PostgreSQL PRD
        """
        try:
            conn = psycopg2.connect(
                host=DatabaseConnection.POSTGRESQL_PRD_CONFIG['host'],
                database=DatabaseConnection.POSTGRESQL_PRD_CONFIG['database'],
                port=DatabaseConnection.POSTGRESQL_PRD_CONFIG['port'],
                user=DatabaseConnection.POSTGRESQL_PRD_CONFIG['user'],
                password=DatabaseConnection.POSTGRESQL_PRD_CONFIG['password']
            )
            return conn
        except Exception as e:
            print(f"Erro ao conectar com PostgreSQL PRD: {e}")
            raise
    
    @staticmethod
    def set_destino(destino: str):
        """
        Define o destino da migracao: 'HML' ou 'PRD'
        
        Args:
            destino (str): 'HML' ou 'PRD'
        """
        destino_upper = destino.upper()
        if destino_upper not in ['HML', 'PRD']:
            raise ValueError(f"Destino invalido: {destino}. Use 'HML' ou 'PRD'")
        DatabaseConnection._destino_atual = destino_upper
        print(f"Destino configurado para: {destino_upper}")
    
    @staticmethod
    def get_destino():
        """
        Retorna o destino atual configurado
        
        Returns:
            str: 'HML' ou 'PRD'
        """
        # Verificar variável de ambiente primeiro
        env_destino = os.getenv('MIGRATION_DESTINO', '').upper()
        if env_destino in ['HML', 'PRD']:
            DatabaseConnection._destino_atual = env_destino
        
        return DatabaseConnection._destino_atual
    
    @staticmethod
    def get_postgresql_destino_connection():
        """
        Cria e retorna uma conexão com o destino configurado (HML ou PRD)
        Configura o schema padrão como 'gmcore'
        
        Returns:
            psycopg2.extensions.connection: Conexão com PostgreSQL destino
        """
        destino = DatabaseConnection.get_destino()
        
        if destino == 'HML':
            return DatabaseConnection.get_postgresql_hml_destino_connection()
        elif destino == 'PRD':
            return DatabaseConnection.get_postgresql_prd_destino_connection()
        else:
            raise ValueError(f"Destino invalido: {destino}")
    
    @staticmethod
    def get_postgresql_hml_destino_connection():
        """
        Cria e retorna uma conexão com PostgreSQL HML (Destino - Leitura/Escrita)
        Configura o schema padrão como 'gmcore'
        
        Returns:
            psycopg2.extensions.connection: Conexão com PostgreSQL HML
        """
        try:
            conn = psycopg2.connect(
                host=DatabaseConnection.POSTGRESQL_HML_DESTINO_CONFIG['host'],
                database=DatabaseConnection.POSTGRESQL_HML_DESTINO_CONFIG['database'],
                port=DatabaseConnection.POSTGRESQL_HML_DESTINO_CONFIG['port'],
                user=DatabaseConnection.POSTGRESQL_HML_DESTINO_CONFIG['user'],
                password=DatabaseConnection.POSTGRESQL_HML_DESTINO_CONFIG['password']
            )
            # Configurar o schema padrão (será configurado dinamicamente pelo código que usa)
            # O schema será passado nas queries, não precisa configurar search_path aqui
            return conn
        except Exception as e:
            print(f"Erro ao conectar com PostgreSQL HML (Destino): {e}")
            raise
    
    @staticmethod
    def get_postgresql_prd_destino_connection():
        """
        Cria e retorna uma conexão com PostgreSQL PRD AWS (Destino - Leitura/Escrita)
        Configura o schema padrão como 'gmcore'
        
        Returns:
            psycopg2.extensions.connection: Conexão com PostgreSQL PRD AWS
        """
        try:
            config = DatabaseConnection.POSTGRESQL_PRD_DESTINO_CONFIG
            
            # Credenciais já configuradas
            
            conn = psycopg2.connect(
                host=config['host'],
                database=config['database'],
                port=config['port'],
                user=config['user'],
                password=config['password']
            )
            # Configurar o schema padrão (será configurado dinamicamente pelo código que usa)
            # O schema será passado nas queries, não precisa configurar search_path aqui
            return conn
        except Exception as e:
            print(f"Erro ao conectar com PostgreSQL PRD AWS (Destino): {e}")
            raise
    
    @staticmethod
    def get_postgresql_hml_connection():
        """
        Alias para get_postgresql_hml_destino_connection() - mantido para compatibilidade
        """
        return DatabaseConnection.get_postgresql_hml_destino_connection()
    
    # Métodos de compatibilidade (mantidos para não quebrar código existente)
    @staticmethod
    def get_sql_server_connection():
        """Alias para get_sql_server_prd_connection() - mantido para compatibilidade"""
        return DatabaseConnection.get_sql_server_prd_connection()
    
    @staticmethod
    def get_postgresql_connection():
        """Alias para get_postgresql_destino_connection() - mantido para compatibilidade"""
        return DatabaseConnection.get_postgresql_destino_connection()
    
    @staticmethod
    def execute_sql_server_prd_query(query: str):
        """
        Executa uma query no SQL Server PRD (APENAS LEITURA)
        
        Args:
            query (str): Query SQL a ser executada
            
        Returns:
            list: Lista de resultados
        """
        # Validar que não é uma operação de escrita
        query_upper = query.strip().upper()
        write_operations = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE']
        if any(query_upper.startswith(op) for op in write_operations):
            raise ValueError(f"Operação de escrita não permitida no banco PRD: {query[:50]}...")
        
        conn = None
        cursor = None
        try:
            conn = DatabaseConnection.get_sql_server_prd_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            
            # Para SELECT, retorna os resultados
            if query_upper.startswith('SELECT'):
                columns = [column[0] for column in cursor.description]
                results = cursor.fetchall()
                return [dict(zip(columns, row)) for row in results]
            else:
                # Mesmo que não seja escrita, não faz commit em PRD
                return cursor.rowcount
        except Exception as e:
            print(f"Erro ao executar query no SQL Server PRD: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    @staticmethod
    def execute_postgresql_prd_query(query: str):
        """
        Executa uma query no PostgreSQL PRD (APENAS LEITURA)
        
        Args:
            query (str): Query SQL a ser executada
            
        Returns:
            list: Lista de resultados
        """
        # Validar que não é uma operação de escrita
        query_upper = query.strip().upper()
        write_operations = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE']
        if any(query_upper.startswith(op) for op in write_operations):
            raise ValueError(f"Operação de escrita não permitida no banco PRD: {query[:50]}...")
        
        conn = None
        cursor = None
        try:
            conn = DatabaseConnection.get_postgresql_prd_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            
            # Para SELECT, retorna os resultados
            if query_upper.startswith('SELECT'):
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                return [dict(zip(columns, row)) for row in results]
            else:
                # Mesmo que não seja escrita, não faz commit em PRD
                return cursor.rowcount
        except Exception as e:
            print(f"Erro ao executar query no PostgreSQL PRD: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    @staticmethod
    def execute_postgresql_destino_query(query: str):
        """
        Executa uma query no destino configurado (HML ou PRD)
        
        Args:
            query (str): Query SQL a ser executada
            
        Returns:
            list: Lista de resultados ou número de linhas afetadas
        """
        conn = None
        cursor = None
        try:
            conn = DatabaseConnection.get_postgresql_destino_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            
            # Para SELECT, retorna os resultados
            if query.strip().upper().startswith('SELECT'):
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                return [dict(zip(columns, row)) for row in results]
            else:
                conn.commit()
                return cursor.rowcount
        except Exception as e:
            if conn:
                conn.rollback()
            destino = DatabaseConnection.get_destino()
            print(f"Erro ao executar query no PostgreSQL {destino} (Destino): {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    @staticmethod
    def execute_postgresql_hml_query(query: str):
        """
        Executa uma query no PostgreSQL HML (Leitura/Escrita)
        Mantido para compatibilidade - usa destino configurado
        """
        return DatabaseConnection.execute_postgresql_destino_query(query)
    
    # Métodos de compatibilidade (mantidos para não quebrar código existente)
    @staticmethod
    def execute_sql_server_query(query: str):
        """Alias para execute_sql_server_prd_query() - mantido para compatibilidade"""
        return DatabaseConnection.execute_sql_server_prd_query(query)
    
    @staticmethod
    def execute_postgresql_query(query: str):
        """Alias para execute_postgresql_destino_query() - mantido para compatibilidade"""
        return DatabaseConnection.execute_postgresql_destino_query(query)


# Exemplo de uso
if __name__ == "__main__":
    print("="*80)
    print("TESTE DE CONEXÕES")
    print("="*80)
    
    # Teste 1: SQL Server PRD
    print("\n[1] Testando conexão SQL Server PRD...")
    try:
        conn = DatabaseConnection.get_sql_server_prd_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION as version")
        result = cursor.fetchone()
        print(f"✓ Conexão SQL Server PRD OK")
        print(f"  Versão: {result[0][:50]}...")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"✗ Erro na conexão SQL Server PRD: {e}")
    
    # Teste 2: PostgreSQL PRD
    print("\n[2] Testando conexão PostgreSQL PRD...")
    try:
        conn = DatabaseConnection.get_postgresql_prd_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        result = cursor.fetchone()
        print(f"✓ Conexão PostgreSQL PRD OK")
        print(f"  Versão: {result[0][:50]}...")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"✗ Erro na conexão PostgreSQL PRD: {e}")
    
    # Teste 3: PostgreSQL HML
    print("\n[3] Testando conexão PostgreSQL HML...")
    try:
        conn = DatabaseConnection.get_postgresql_hml_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT current_database(), current_schema()")
        result = cursor.fetchone()
        print(f"✓ Conexão PostgreSQL HML OK")
        print(f"  Database: {result[0]}")
        print(f"  Schema atual: {result[1]}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"✗ Erro na conexão PostgreSQL HML: {e}")
    
    print("\n" + "="*80)
    print("TESTE CONCLUÍDO")
    print("="*80)

