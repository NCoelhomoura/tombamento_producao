"""
Script para clonar todas as tabelas do schema 'core' do PostgreSQL PRD
para o schema 'gmcore' do PostgreSQL HML
"""

# Configurar encoding para evitar problemas no Windows
import sys
import io
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import psycopg2
from psycopg2 import sql
import logging
import subprocess
import tempfile
import os
import re

# Adicionar diretório utils ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database_connection import DatabaseConnection

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('clone_tables.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def get_all_tables_from_core_schema():
    """Lista todas as tabelas do schema 'core' no PostgreSQL PRD"""
    print("\n" + "="*80)
    print("ETAPA 1: Listando tabelas do schema 'core' no PostgreSQL PRD")
    print("="*80)
    
    config = DatabaseConnection.POSTGRESQL_PRD_CONFIG
    print(f"PRD - Database: {config['database']}")
    print(f"PRD - Schema: core")
    print(f"PRD - Host: {config['host']}")
    
    conn = None
    try:
        conn = DatabaseConnection.get_postgresql_prd_connection()
        cursor = conn.cursor()
        
        # Verificar database conectado
        cursor.execute("SELECT current_database()")
        current_db = cursor.fetchone()[0]
        print(f"OK - Conectado ao database: {current_db}")
        
        # Buscar todas as tabelas do schema 'core'
        query = """
        SELECT 
            table_name,
            table_type
        FROM information_schema.tables
        WHERE table_schema = 'core'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
        """
        
        cursor.execute(query)
        tables = cursor.fetchall()
        
        table_names = [table[0] for table in tables]
        
        print(f"\nOK - Encontradas {len(table_names)} tabelas no schema 'core' do database '{current_db}':")
        for i, table_name in enumerate(table_names, 1):
            print(f"  {i}. {table_name}")
        
        logger.info(f"Database PRD: {current_db}, Schema: core, Tabelas encontradas: {len(table_names)}")
        
        cursor.close()
        conn.close()
        
        return table_names
        
    except Exception as e:
        logger.error(f"Erro ao listar tabelas: {e}")
        if conn:
            conn.close()
        raise


def get_table_ddl_pg_dump(table_name):
    """Obtém o DDL usando pg_dump (método mais confiável)"""
    config = DatabaseConnection.POSTGRESQL_PRD_CONFIG
    
    try:
        # Construir comando pg_dump
        pg_dump_cmd = [
            'pg_dump',
            '-h', config['host'],
            '-p', str(config['port']),
            '-U', config['user'],
            '-d', config['database'],
            '-t', f"core.{table_name}",
            '--schema-only',
            '--no-owner',
            '--no-privileges'
        ]
        
        # Definir senha via variável de ambiente
        env = os.environ.copy()
        env['PGPASSWORD'] = config['password']
        
        # Executar pg_dump
        result = subprocess.run(
            pg_dump_cmd,
            capture_output=True,
            text=True,
            env=env,
            timeout=30
        )
        
        if result.returncode != 0:
            raise Exception(f"pg_dump falhou: {result.stderr}")
        
        ddl = result.stdout
        
        # Substituir schema 'core' por 'gmcore' (evitar duplicação)
        # Primeiro substituir referências completas, depois genéricas
        ddl = re.sub(r'\bcore\.', 'gmcore.', ddl)
        ddl = ddl.replace('CREATE TABLE gmcore.', 'CREATE TABLE gmcore.')
        ddl = ddl.replace('TABLE gmcore.', 'TABLE gmcore.')
        # Garantir que não tenha gmgmcore
        ddl = re.sub(r'gmgmcore', 'gmcore', ddl)
        
        return ddl
        
    except FileNotFoundError:
        # pg_dump não encontrado, usar método alternativo
        logger.warning("pg_dump não encontrado, usando método alternativo")
        return None
    except Exception as e:
        logger.warning(f"Erro ao usar pg_dump: {e}, usando método alternativo")
        return None


def get_table_ddl(table_name):
    """Obtém o DDL (CREATE TABLE) de uma tabela do schema 'core'"""
    # Tentar primeiro com pg_dump (mais confiável)
    ddl = get_table_ddl_pg_dump(table_name)
    if ddl:
        return ddl
    
    # Método alternativo: Construir DDL manualmente
    conn = None
    try:
        conn = DatabaseConnection.get_postgresql_prd_connection()
        cursor = conn.cursor()
        
        # Construir DDL manualmente a partir do information_schema
        cursor.execute("""
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default,
                udt_name
            FROM information_schema.columns
            WHERE table_schema = 'core'
            AND table_name = %s
            ORDER BY ordinal_position;
        """, (table_name,))
        
        columns = cursor.fetchall()
        
        # Buscar constraints (PK, FK, UNIQUE, CHECK)
        cursor.execute("""
            SELECT
                tc.constraint_name,
                tc.constraint_type,
                kcu.column_name,
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            LEFT JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.table_schema = 'core'
            AND tc.table_name = %s
            ORDER BY tc.constraint_type, tc.constraint_name;
        """, (table_name,))
        
        constraints = cursor.fetchall()
        
        # Buscar índices
        cursor.execute("""
            SELECT
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname = 'core'
            AND tablename = %s;
        """, (table_name,))
        
        indexes = cursor.fetchall()
        
        # Construir DDL
        ddl_parts = [f"CREATE TABLE gmcore.{table_name} ("]
        
        # Adicionar colunas
        column_defs = []
        primary_key_cols = []
        
        for col in columns:
            col_name = col[0]
            data_type = col[1]
            char_max_len = col[2]
            numeric_precision = col[3]
            numeric_scale = col[4]
            is_nullable = col[5]
            column_default = col[6]
            udt_name = col[7]
            
            # Construir tipo de dados
            if data_type == 'character varying' or data_type == 'varchar':
                if char_max_len:
                    type_str = f"VARCHAR({char_max_len})"
                else:
                    type_str = "VARCHAR"
            elif data_type == 'character' or data_type == 'char':
                if char_max_len:
                    type_str = f"CHAR({char_max_len})"
                else:
                    type_str = "CHAR"
            elif data_type == 'numeric' or data_type == 'decimal':
                if numeric_precision and numeric_scale:
                    type_str = f"NUMERIC({numeric_precision},{numeric_scale})"
                elif numeric_precision:
                    type_str = f"NUMERIC({numeric_precision})"
                else:
                    type_str = "NUMERIC"
            elif data_type == 'integer':
                type_str = "INTEGER"
            elif data_type == 'bigint':
                type_str = "BIGINT"
            elif data_type == 'smallint':
                type_str = "SMALLINT"
            elif data_type == 'double precision':
                type_str = "DOUBLE PRECISION"
            elif data_type == 'real':
                type_str = "REAL"
            elif data_type == 'boolean':
                type_str = "BOOLEAN"
            elif data_type == 'date':
                type_str = "DATE"
            elif data_type == 'timestamp without time zone':
                type_str = "TIMESTAMP"
            elif data_type == 'timestamp with time zone':
                type_str = "TIMESTAMP WITH TIME ZONE"
            elif data_type == 'time without time zone':
                type_str = "TIME"
            elif data_type == 'time with time zone':
                type_str = "TIME WITH TIME ZONE"
            elif data_type == 'text':
                type_str = "TEXT"
            elif data_type == 'uuid':
                type_str = "UUID"
            elif data_type == 'json' or data_type == 'jsonb':
                type_str = data_type.upper()
            elif data_type == 'ARRAY':
                # Para arrays, usar o udt_name
                type_str = udt_name.replace('_', '[]')
            else:
                # Fallback: usar udt_name
                type_str = udt_name.upper()
            
            col_def = f"    {col_name} {type_str}"
            
            # Adicionar NOT NULL se necessário
            if is_nullable == 'NO':
                col_def += " NOT NULL"
            
            # Adicionar DEFAULT se existir
            if column_default:
                # Limpar o default (remover ::type se houver)
                default_clean = str(column_default).replace("::" + udt_name, "")
                col_def += f" DEFAULT {default_clean}"
            
            column_defs.append(col_def)
        
        ddl_parts.append(",\n".join(column_defs))
        
        # Adicionar PRIMARY KEY se existir (evitar duplicação)
        pk_constraints = [c for c in constraints if c[1] == 'PRIMARY KEY']
        if pk_constraints:
            # Usar dict para manter ordem e remover duplicatas
            pk_cols_dict = {}
            for pk_const in pk_constraints:
                pk_cols_dict[pk_const[2]] = True
            pk_cols = list(pk_cols_dict.keys())
            if pk_cols:
                ddl_parts.append(f",\n    PRIMARY KEY ({', '.join(pk_cols)})")
        
        # Adicionar UNIQUE constraints
        unique_constraints = {}
        for constraint in constraints:
            if constraint[1] == 'UNIQUE':
                if constraint[0] not in unique_constraints:
                    unique_constraints[constraint[0]] = []
                unique_constraints[constraint[0]].append(constraint[2])
        
        for constraint_name, cols in unique_constraints.items():
            ddl_parts.append(f",\n    CONSTRAINT {constraint_name} UNIQUE ({', '.join(cols)})")
        
        # Adicionar FOREIGN KEY constraints
        # Nota: FKs serão criadas depois, quando todas as tabelas existirem
        fk_constraints = {}
        for constraint in constraints:
            if constraint[1] == 'FOREIGN KEY':
                if constraint[0] not in fk_constraints:
                    fk_constraints[constraint[0]] = {
                        'column': constraint[2],
                        'ref_table': constraint[4],
                        'ref_column': constraint[5]
                    }
        
        # Por enquanto, não adicionar FKs no CREATE TABLE
        # Elas serão adicionadas depois quando todas as tabelas existirem
        
        ddl_parts.append("\n);")
        
        ddl = "".join(ddl_parts)
        
        # Adicionar índices (após CREATE TABLE)
        index_ddls = []
        for index in indexes:
            # Substituir schema e nome da tabela no indexdef
            index_def = index[1]
            # Substituir referências ao schema core
            index_def = re.sub(r'\bcore\.', 'gmcore.', index_def)
            # Garantir que não tenha gmgmcore
            index_def = re.sub(r'gmgmcore', 'gmcore', index_def)
            index_ddls.append(index_def + ";")
        
        if index_ddls:
            ddl += "\n\n" + "\n".join(index_ddls)
        
        # Garantir que não há duplicação de schema no DDL final
        ddl = re.sub(r'gmgmcore', 'gmcore', ddl)
        
        cursor.close()
        conn.close()
        
        return ddl
        
    except Exception as e:
        logger.error(f"Erro ao obter DDL da tabela {table_name}: {e}")
        if conn:
            conn.close()
        raise


def create_table_in_hml(table_name, ddl):
    """Cria uma tabela no PostgreSQL HML usando o DDL fornecido"""
    config = DatabaseConnection.POSTGRESQL_HML_CONFIG
    
    conn = None
    try:
        conn = DatabaseConnection.get_postgresql_hml_connection()
        cursor = conn.cursor()
        
        # Verificar database e schema conectados
        cursor.execute("SELECT current_database(), current_schema()")
        current_db, current_schema = cursor.fetchone()
        
        if current_schema != 'gmcore':
            logger.warning(f"Schema atual é '{current_schema}', configurando para 'gmcore'...")
            cursor.execute("SET search_path TO gmcore, public;")
            conn.commit()
            cursor.execute("SELECT current_schema()")
            current_schema = cursor.fetchone()[0]
        
        logger.info(f"Criando tabela {table_name} no HML - Database: {current_db}, Schema: {current_schema}")
        
        # Verificar se a tabela já existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'gmcore' 
                AND table_name = %s
            )
        """, (table_name,))
        
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            print(f"  AVISO - Tabela {table_name} já existe no schema 'gmcore'. Pulando...")
            logger.warning(f"Tabela {table_name} já existe no HML (database: {current_db}, schema: gmcore)")
            cursor.close()
            conn.close()
            return False
        
        # Executar DDL
        cursor.execute(ddl)
        conn.commit()
        
        # Verificar se foi criada corretamente
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'gmcore' 
                AND table_name = %s
            )
        """, (table_name,))
        
        created = cursor.fetchone()[0]
        if created:
            logger.info(f"Tabela {table_name} criada com sucesso no HML (database: {current_db}, schema: gmcore)")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao criar tabela {table_name} no HML: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise


def validate_tables_created():
    """Valida que todas as tabelas foram criadas corretamente"""
    print("\n" + "="*80)
    print("VALIDAÇÃO: Verificando tabelas criadas no HML")
    print("="*80)
    
    # Listar tabelas no PRD
    config_prd = DatabaseConnection.POSTGRESQL_PRD_CONFIG
    conn_prd = DatabaseConnection.get_postgresql_prd_connection()
    cursor_prd = conn_prd.cursor()
    
    cursor_prd.execute("SELECT current_database()")
    prd_db = cursor_prd.fetchone()[0]
    
    cursor_prd.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'core'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """)
    prd_tables = [row[0] for row in cursor_prd.fetchall()]
    cursor_prd.close()
    conn_prd.close()
    
    print(f"\nPRD:")
    print(f"  Database: {prd_db}")
    print(f"  Schema: core")
    print(f"  Tabelas encontradas: {len(prd_tables)}")
    
    # Listar tabelas no HML
    config_hml = DatabaseConnection.POSTGRESQL_HML_CONFIG
    conn_hml = DatabaseConnection.get_postgresql_hml_connection()
    cursor_hml = conn_hml.cursor()
    
    cursor_hml.execute("SELECT current_database(), current_schema()")
    hml_db, hml_schema = cursor_hml.fetchone()
    
    cursor_hml.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'gmcore'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """)
    hml_tables = [row[0] for row in cursor_hml.fetchall()]
    cursor_hml.close()
    conn_hml.close()
    
    print(f"\nHML:")
    print(f"  Database: {hml_db}")
    print(f"  Schema: {hml_schema}")
    print(f"  Tabelas encontradas: {len(hml_tables)}")
    
    missing_tables = set(prd_tables) - set(hml_tables)
    extra_tables = set(hml_tables) - set(prd_tables)
    
    if missing_tables:
        print(f"\nAVISO - Tabelas faltando no HML ({len(missing_tables)}):")
        for table in sorted(missing_tables):
            print(f"  - {table}")
    
    if extra_tables:
        print(f"\nINFO - Tabelas extras no HML ({len(extra_tables)}):")
        for table in sorted(extra_tables):
            print(f"  - {table}")
    
    if not missing_tables and not extra_tables:
        print("\nOK - Todas as tabelas foram criadas corretamente!")
        logger.info(f"Validação OK - PRD: {prd_db}.core ({len(prd_tables)} tabelas) = HML: {hml_db}.gmcore ({len(hml_tables)} tabelas)")
        return True
    else:
        logger.warning(f"Validação com diferenças - PRD: {prd_db}.core ({len(prd_tables)} tabelas) vs HML: {hml_db}.gmcore ({len(hml_tables)} tabelas)")
        return False


def main():
    """Executa o processo completo de clonagem de tabelas"""
    print("\n" + "="*80)
    print("CLONAGEM DE TABELAS: PRD -> HML")
    print("="*80)
    
    config_prd = DatabaseConnection.POSTGRESQL_PRD_CONFIG
    config_hml = DatabaseConnection.POSTGRESQL_HML_CONFIG
    
    print(f"\nORIGEM (PRD):")
    print(f"  Database: {config_prd['database']}")
    print(f"  Schema: core")
    print(f"  Host: {config_prd['host']}")
    
    print(f"\nDESTINO (HML):")
    print(f"  Database: {config_hml['database']}")
    print(f"  Schema: {config_hml['schema']}")
    print(f"  Host: {config_hml['host']}")
    
    print("\n" + "="*80)
    
    try:
        # Etapa 1: Listar tabelas
        tables = get_all_tables_from_core_schema()
        
        if not tables:
            print("\nAVISO - Nenhuma tabela encontrada no schema 'core' do PRD")
            return
        
        # Etapa 2: Clonar cada tabela
        print("\n" + "="*80)
        print("ETAPA 2: Clonando tabelas para o HML")
        print("="*80)
        
        success_count = 0
        error_count = 0
        
        for i, table_name in enumerate(tables, 1):
            print(f"\n[{i}/{len(tables)}] Processando tabela: {table_name}")
            logger.info(f"Processando tabela {i}/{len(tables)}: {table_name}")
            
            try:
                # Obter DDL
                print(f"  -> Obtendo estrutura da tabela...")
                ddl = get_table_ddl(table_name)
                
                # Criar no HML
                print(f"  -> Criando tabela no HML...")
                if create_table_in_hml(table_name, ddl):
                    print(f"  OK - Tabela {table_name} criada com sucesso!")
                    success_count += 1
                else:
                    print(f"  AVISO - Tabela {table_name} já existe ou foi pulada")
                
            except Exception as e:
                print(f"  ERRO - Erro ao processar tabela {table_name}: {e}")
                logger.error(f"Erro ao processar tabela {table_name}: {e}")
                error_count += 1
                continue
        
        # Etapa 3: Validação
        print("\n" + "="*80)
        print("RESUMO")
        print("="*80)
        print(f"Total de tabelas processadas: {len(tables)}")
        print(f"Tabelas criadas com sucesso: {success_count}")
        print(f"Erros: {error_count}")
        
        # Validação final
        validate_tables_created()
        
        print("\n" + "="*80)
        print("PROCESSO CONCLUÍDO")
        print("="*80)
        
    except Exception as e:
        logger.error(f"Erro crítico no processo: {e}")
        print(f"\nERRO CRITICO: {e}")
        raise


if __name__ == "__main__":
    main()

