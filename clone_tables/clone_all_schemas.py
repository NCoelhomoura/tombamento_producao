"""
Script para clonar TODOS os schemas e tabelas do PostgreSQL PRD
para o PostgreSQL HML

Mapeamento de schemas:
- PRD: core, commercial, pdv, etc.
- HML: gmcore, gmcommercial, gmpdv, etc. (prefixo 'gm' adicionado)
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
from typing import List, Dict, Tuple

# Adicionar diretório utils ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database_connection import DatabaseConnection

# Configurar logging
log_file_path = os.path.join(os.path.dirname(__file__), 'clone_all_schemas.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Schemas do sistema que devem ser ignorados
SYSTEM_SCHEMAS = {
    'information_schema',
    'pg_catalog',
    'pg_toast',
    'pg_temp_1',
    'pg_toast_temp_1',
    'pg_temp_2',
    'pg_toast_temp_2',
    'pg_temp_3',
    'pg_toast_temp_3',
    'public'  # Schema padrão do PostgreSQL, geralmente não precisa ser clonado
}


def get_schema_mapping(prd_schema: str) -> str:
    """
    Mapeia schema do PRD para schema do HML
    Adiciona prefixo 'gm' se ainda não tiver
    """
    if prd_schema.startswith('gm'):
        # Já tem prefixo, retornar como está
        return prd_schema
    else:
        # Adicionar prefixo 'gm'
        return f'gm{prd_schema}'


def get_all_schemas_from_prd() -> List[str]:
    """Lista todos os schemas do PostgreSQL PRD (excluindo schemas do sistema)"""
    print("\n" + "="*80)
    print("ETAPA 1: Listando schemas do PostgreSQL PRD")
    print("="*80)
    
    config = DatabaseConnection.POSTGRESQL_PRD_CONFIG
    print(f"PRD - Database: {config['database']}")
    print(f"PRD - Host: {config['host']}")
    
    conn = None
    try:
        conn = DatabaseConnection.get_postgresql_prd_connection()
        cursor = conn.cursor()
        
        # Verificar database conectado
        cursor.execute("SELECT current_database()")
        current_db = cursor.fetchone()[0]
        print(f"OK - Conectado ao database: {current_db}")
        
        # Buscar todos os schemas (excluindo schemas do sistema)
        query = """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        AND schema_name NOT LIKE 'pg_temp_%'
        AND schema_name NOT LIKE 'pg_toast_temp_%'
        ORDER BY schema_name;
        """
        
        cursor.execute(query)
        schemas = cursor.fetchall()
        
        schema_names = [schema[0] for schema in schemas if schema[0] not in SYSTEM_SCHEMAS]
        
        print(f"\nOK - Encontrados {len(schema_names)} schemas no database '{current_db}':")
        for i, schema_name in enumerate(schema_names, 1):
            hml_schema = get_schema_mapping(schema_name)
            print(f"  {i}. {schema_name} -> {hml_schema}")
        
        logger.info(f"Database PRD: {current_db}, Schemas encontrados: {len(schema_names)}")
        
        cursor.close()
        conn.close()
        
        return schema_names
        
    except Exception as e:
        logger.error(f"Erro ao listar schemas: {e}")
        if conn:
            conn.close()
        raise


def get_all_tables_from_schema(prd_schema: str) -> List[str]:
    """Lista todas as tabelas de um schema específico no PostgreSQL PRD"""
    conn = None
    try:
        conn = DatabaseConnection.get_postgresql_prd_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
        """
        
        cursor.execute(query, (prd_schema,))
        tables = cursor.fetchall()
        
        table_names = [table[0] for table in tables]
        
        cursor.close()
        conn.close()
        
        return table_names
        
    except Exception as e:
        logger.error(f"Erro ao listar tabelas do schema {prd_schema}: {e}")
        if conn:
            conn.close()
        return []


def drop_schema_in_hml(hml_schema: str, cascade: bool = True) -> bool:
    """Remove um schema do PostgreSQL HML e todas suas tabelas"""
    conn = None
    try:
        conn = DatabaseConnection.get_postgresql_hml_connection()
        cursor = conn.cursor()
        
        # Verificar se o schema existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.schemata 
                WHERE schema_name = %s
            )
        """, (hml_schema,))
        
        schema_exists = cursor.fetchone()[0]
        
        if not schema_exists:
            logger.info(f"Schema {hml_schema} não existe no HML. Nada a fazer.")
            cursor.close()
            conn.close()
            return False
        
        # Fazer DROP do schema
        cascade_clause = "CASCADE" if cascade else "RESTRICT"
        cursor.execute(sql.SQL("DROP SCHEMA IF EXISTS {} {}").format(
            sql.Identifier(hml_schema),
            sql.SQL(cascade_clause)
        ))
        conn.commit()
        
        logger.info(f"Schema {hml_schema} removido com sucesso do HML (CASCADE={cascade})")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao remover schema {hml_schema} do HML: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise


def create_schema_in_hml(hml_schema: str, drop_existing: bool = False) -> bool:
    """Cria um schema no PostgreSQL HML
    
    Args:
        hml_schema: Nome do schema a ser criado
        drop_existing: Se True, remove o schema existente antes de criar
    """
    conn = None
    try:
        conn = DatabaseConnection.get_postgresql_hml_connection()
        cursor = conn.cursor()
        
        # Verificar se o schema já existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.schemata 
                WHERE schema_name = %s
            )
        """, (hml_schema,))
        
        schema_exists = cursor.fetchone()[0]
        
        if schema_exists:
            if drop_existing:
                logger.info(f"Schema {hml_schema} existe. Removendo antes de recriar...")
                cursor.close()
                conn.close()
                drop_schema_in_hml(hml_schema, cascade=True)
                # Reconectar após DROP
                conn = DatabaseConnection.get_postgresql_hml_connection()
                cursor = conn.cursor()
            else:
                logger.info(f"Schema {hml_schema} já existe no HML")
                cursor.close()
                conn.close()
                return False
        
        # Criar schema
        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(hml_schema)
        ))
        conn.commit()
        
        action = "recriado" if drop_existing and schema_exists else "criado"
        logger.info(f"Schema {hml_schema} {action} com sucesso no HML")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao criar schema {hml_schema} no HML: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise


def rewrite_nextval_default_for_hml(column_default, hml_schema: str):
    """
    Reescreve DEFAULT nextval('seq'::regclass) para usar o schema HML (gm*).
    Sem isso, nextval aponta para sequência inexistente no HML.
    """
    if not column_default or "nextval" not in str(column_default).lower():
        return column_default
    s = str(column_default)

    def repl(m):
        inner = m.group(1).replace('"', "")
        if "." in inner:
            sch, seq = inner.split(".", 1)
            h_sch = get_schema_mapping(sch)
            return f"nextval('{h_sch}.{seq}'::regclass)"
        return f"nextval('{hml_schema}.{inner}'::regclass)"

    return re.sub(
        r"nextval\(\s*'([^']+)'\s*::\s*regclass\s*\)",
        repl,
        s,
        flags=re.IGNORECASE,
    )


def fetch_sequence_create_sql(cursor, seq_schema: str, seq_name: str) -> str:
    """
    Gera CREATE SEQUENCE IF NOT EXISTS no schema HML correspondente,
    copiando parâmetros da sequência no PRD.
    """
    cursor.execute(
        """
        SELECT s.seqincrement, s.seqmin, s.seqmax, s.seqstart, s.seqcache, s.seqcycle
        FROM pg_sequence s
        JOIN pg_class c ON c.oid = s.seqrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'S' AND n.nspname = %s AND c.relname = %s
        """,
        (seq_schema, seq_name),
    )
    row = cursor.fetchone()
    if not row:
        cursor.execute(
            """
            SELECT s.seqincrement, s.seqmin, s.seqmax, s.seqstart, s.seqcache, s.seqcycle
            FROM pg_sequence s
            JOIN pg_class c ON c.oid = s.seqrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'S' AND n.nspname = 'public' AND c.relname = %s
            """,
            (seq_name,),
        )
        row = cursor.fetchone()
    if not row:
        return None

    inc, vmin, vmax, vstart, cache, cycle = row
    hml_sch = get_schema_mapping(seq_schema)
    cyc = "CYCLE" if cycle else "NO CYCLE"
    return (
        f'CREATE SEQUENCE IF NOT EXISTS "{hml_sch}"."{seq_name}" '
        f"INCREMENT BY {inc} MINVALUE {vmin} MAXVALUE {vmax} "
        f"START WITH {vstart} CACHE {cache} {cyc};"
    )


def collect_sequence_ddls_before_table(cursor, prd_schema: str, table_name: str, columns) -> List[str]:
    """
    Para colunas com DEFAULT nextval(...), cria CREATE SEQUENCE no HML antes do CREATE TABLE.
    Usa pg_get_serial_sequence quando possível; senão, extrai o nome da sequência do default.
    """
    seen = set()
    out: List[str] = []

    for col in columns:
        col_name = col[0]
        column_default = col[6]
        if not column_default or "nextval" not in str(column_default).lower():
            continue

        seq_schema = None
        seq_base = None

        try:
            cursor.execute(
                "SELECT pg_get_serial_sequence(%s, %s)",
                (f"{prd_schema}.{table_name}", col_name),
            )
            r = cursor.fetchone()
            if r and r[0]:
                fq = str(r[0]).replace('"', "")
                if "." in fq:
                    seq_schema, seq_base = fq.split(".", 1)
        except Exception:
            pass

        if not seq_schema or not seq_base:
            m = re.search(
                r"nextval\(\s*'([^']+)'\s*::\s*regclass\s*\)",
                str(column_default),
                re.I,
            )
            if not m:
                continue
            inner = m.group(1).replace('"', "")
            if "." in inner:
                seq_schema, seq_base = inner.split(".", 1)
            else:
                seq_schema, seq_base = prd_schema, inner

        key = f"{seq_schema}.{seq_base}"
        if key in seen:
            continue
        seen.add(key)

        sql_seq = fetch_sequence_create_sql(cursor, seq_schema, seq_base)
        if sql_seq:
            out.append(sql_seq)
        else:
            logger.warning(
                f"Sequência {seq_schema}.{seq_base} não encontrada no PRD "
                f"(tabela {prd_schema}.{table_name}.{col_name})"
            )

    return out


def get_table_ddl_pg_dump(table_name: str, prd_schema: str) -> str:
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
            '-t', f"{prd_schema}.{table_name}",
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
        
        # Substituir referências ao schema PRD pelo schema HML
        hml_schema = get_schema_mapping(prd_schema)
        ddl = re.sub(rf'\b{re.escape(prd_schema)}\.', f'{hml_schema}.', ddl)
        ddl = re.sub(rf'\bCREATE TABLE {re.escape(prd_schema)}\.', f'CREATE TABLE {hml_schema}.', ddl)
        ddl = re.sub(rf'\bTABLE {re.escape(prd_schema)}\.', f'TABLE {hml_schema}.', ddl)
        
        # Evitar duplicação (ex: gmgmcore -> gmcore)
        ddl = re.sub(rf'gm{re.escape(hml_schema)}', hml_schema, ddl)
        
        return ddl
        
    except FileNotFoundError:
        # pg_dump não encontrado, usar método alternativo
        logger.warning("pg_dump não encontrado, usando método alternativo")
        return None
    except Exception as e:
        logger.warning(f"Erro ao usar pg_dump: {e}, usando método alternativo")
        return None


def get_table_ddl(table_name: str, prd_schema: str) -> str:
    """Obtém o DDL (CREATE TABLE) de uma tabela de um schema específico"""
    # Tentar primeiro com pg_dump (mais confiável)
    ddl = get_table_ddl_pg_dump(table_name, prd_schema)
    if ddl:
        return ddl
    
    # Método alternativo: Construir DDL manualmente
    conn = None
    try:
        conn = DatabaseConnection.get_postgresql_prd_connection()
        cursor = conn.cursor()
        
        hml_schema = get_schema_mapping(prd_schema)
        
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
            WHERE table_schema = %s
            AND table_name = %s
            ORDER BY ordinal_position;
        """, (prd_schema, table_name))
        
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
            WHERE tc.table_schema = %s
            AND tc.table_name = %s
            ORDER BY tc.constraint_type, tc.constraint_name;
        """, (prd_schema, table_name))
        
        constraints = cursor.fetchall()
        
        # Buscar índices
        cursor.execute("""
            SELECT
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname = %s
            AND tablename = %s;
        """, (prd_schema, table_name))
        
        indexes = cursor.fetchall()
        
        # Sequências usadas em DEFAULT nextval(...) — criar no HML antes do CREATE TABLE
        sequence_ddls = collect_sequence_ddls_before_table(
            cursor, prd_schema, table_name, columns
        )
        
        # Construir DDL
        ddl_parts = [f"CREATE TABLE {hml_schema}.{table_name} ("]
        
        # Adicionar colunas
        column_defs = []
        
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
                type_str = udt_name.replace('_', '[]')
            else:
                type_str = udt_name.upper()
            
            col_def = f"    {col_name} {type_str}"
            
            # Adicionar NOT NULL se necessário
            if is_nullable == 'NO':
                col_def += " NOT NULL"
            
            # Adicionar DEFAULT se existir (nextval deve apontar para schema gm* no HML)
            if column_default:
                rewritten = rewrite_nextval_default_for_hml(column_default, hml_schema)
                default_clean = str(rewritten).replace("::" + udt_name, "")
                col_def += f" DEFAULT {default_clean}"
            
            column_defs.append(col_def)
        
        ddl_parts.append(",\n".join(column_defs))
        
        # Adicionar PRIMARY KEY se existir
        pk_constraints = [c for c in constraints if c[1] == 'PRIMARY KEY']
        if pk_constraints:
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
        
        ddl_parts.append("\n);")
        
        ddl = "".join(ddl_parts)
        if sequence_ddls:
            ddl = "\n".join(sequence_ddls) + "\n\n" + ddl
        
        # Adicionar índices (após CREATE TABLE)
        index_ddls = []
        for index in indexes:
            index_def = index[1]
            # Substituir referências ao schema PRD
            index_def = re.sub(rf'\b{re.escape(prd_schema)}\.', f'{hml_schema}.', index_def)
            index_def = re.sub(rf'gm{re.escape(hml_schema)}', hml_schema, index_def)
            index_ddls.append(index_def + ";")
        
        if index_ddls:
            ddl += "\n\n" + "\n".join(index_ddls)
        
        # Garantir que não há duplicação de schema no DDL final
        ddl = re.sub(rf'gm{re.escape(hml_schema)}', hml_schema, ddl)
        
        cursor.close()
        conn.close()
        
        return ddl
        
    except Exception as e:
        logger.error(f"Erro ao obter DDL da tabela {prd_schema}.{table_name}: {e}")
        if conn:
            conn.close()
        raise


def execute_hml_ddl_statements(cursor, ddl: str) -> None:
    """
    psycopg2 executa apenas um comando por execute(). DDL com CREATE SEQUENCE +
    CREATE TABLE + índices precisa ser dividido.
    """
    ddl = ddl.strip()
    if not ddl:
        return
    parts = [p.strip() for p in ddl.split(";") if p.strip()]
    for stmt in parts:
        cursor.execute(stmt + ";")


def drop_table_in_hml(table_name: str, hml_schema: str) -> bool:
    """Remove uma tabela do PostgreSQL HML"""
    conn = None
    try:
        conn = DatabaseConnection.get_postgresql_hml_connection()
        cursor = conn.cursor()
        
        # Verificar se a tabela existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            )
        """, (hml_schema, table_name))
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            return False
        
        # Fazer DROP da tabela
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
            sql.Identifier(hml_schema),
            sql.Identifier(table_name)
        ))
        conn.commit()
        
        logger.info(f"Tabela {hml_schema}.{table_name} removida com sucesso do HML")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao remover tabela {hml_schema}.{table_name} do HML: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise


def create_table_in_hml(table_name: str, ddl: str, hml_schema: str, drop_existing: bool = False) -> bool:
    """Cria uma tabela no PostgreSQL HML usando o DDL fornecido
    
    Args:
        table_name: Nome da tabela
        ddl: DDL da tabela (CREATE TABLE)
        hml_schema: Schema onde a tabela será criada
        drop_existing: Se True, remove a tabela existente antes de criar
    """
    conn = None
    try:
        conn = DatabaseConnection.get_postgresql_hml_connection()
        cursor = conn.cursor()
        
        # Configurar search_path para o schema correto
        cursor.execute(f"SET search_path TO {hml_schema}, public;")
        conn.commit()
        
        # Verificar se a tabela já existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            )
        """, (hml_schema, table_name))
        
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            if drop_existing:
                logger.info(f"Tabela {hml_schema}.{table_name} existe. Removendo antes de recriar...")
                cursor.close()
                conn.close()
                drop_table_in_hml(table_name, hml_schema)
                # Reconectar após DROP
                conn = DatabaseConnection.get_postgresql_hml_connection()
                cursor = conn.cursor()
                cursor.execute(f"SET search_path TO {hml_schema}, public;")
                conn.commit()
            else:
                logger.warning(f"Tabela {hml_schema}.{table_name} já existe no HML. Pulando...")
                cursor.close()
                conn.close()
                return False
        
        # Executar DDL (vários comandos: sequências, tabela, índices)
        execute_hml_ddl_statements(cursor, ddl)
        conn.commit()
        
        # Verificar se foi criada corretamente
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            )
        """, (hml_schema, table_name))
        
        created = cursor.fetchone()[0]
        if created:
            action = "recriada" if drop_existing and table_exists else "criada"
            logger.info(f"Tabela {hml_schema}.{table_name} {action} com sucesso no HML")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao criar tabela {hml_schema}.{table_name} no HML: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise


def validate_schemas_and_tables():
    """Valida que todos os schemas e tabelas foram criados corretamente"""
    print("\n" + "="*80)
    print("VALIDAÇÃO: Verificando schemas e tabelas criadas no HML")
    print("="*80)
    
    # Listar schemas no PRD
    conn_prd = DatabaseConnection.get_postgresql_prd_connection()
    cursor_prd = conn_prd.cursor()
    
    cursor_prd.execute("SELECT current_database()")
    prd_db = cursor_prd.fetchone()[0]
    
    cursor_prd.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        AND schema_name NOT LIKE 'pg_temp_%'
        AND schema_name NOT LIKE 'pg_toast_temp_%'
        ORDER BY schema_name;
    """)
    prd_schemas = [row[0] for row in cursor_prd.fetchall() if row[0] not in SYSTEM_SCHEMAS]
    
    # Listar schemas no HML
    conn_hml = DatabaseConnection.get_postgresql_hml_connection()
    cursor_hml = conn_hml.cursor()
    
    cursor_hml.execute("SELECT current_database()")
    hml_db = cursor_hml.fetchone()[0]
    
    cursor_hml.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        AND schema_name NOT LIKE 'pg_temp_%'
        AND schema_name NOT LIKE 'pg_toast_temp_%'
        ORDER BY schema_name;
    """)
    hml_schemas = [row[0] for row in cursor_hml.fetchall() if row[0] not in SYSTEM_SCHEMAS]
    
    print(f"\nPRD:")
    print(f"  Database: {prd_db}")
    print(f"  Schemas encontrados: {len(prd_schemas)}")
    
    print(f"\nHML:")
    print(f"  Database: {hml_db}")
    print(f"  Schemas encontrados: {len(hml_schemas)}")
    
    # Validar cada schema
    total_prd_tables = 0
    total_hml_tables = 0
    
    for prd_schema in prd_schemas:
        hml_schema = get_schema_mapping(prd_schema)
        
        # Contar tabelas no PRD
        cursor_prd.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE';
        """, (prd_schema,))
        prd_count = cursor_prd.fetchone()[0]
        total_prd_tables += prd_count
        
        # Contar tabelas no HML
        cursor_hml.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE';
        """, (hml_schema,))
        hml_count = cursor_hml.fetchone()[0]
        total_hml_tables += hml_count
        
        if prd_count != hml_count:
            print(f"\n  ⚠️  Schema {prd_schema} -> {hml_schema}: PRD={prd_count} tabelas, HML={hml_count} tabelas")
        else:
            print(f"  ✅ Schema {prd_schema} -> {hml_schema}: {prd_count} tabelas")
    
    cursor_prd.close()
    conn_prd.close()
    cursor_hml.close()
    conn_hml.close()
    
    print(f"\nRESUMO:")
    print(f"  Total de tabelas no PRD: {total_prd_tables}")
    print(f"  Total de tabelas no HML: {total_hml_tables}")
    
    if total_prd_tables == total_hml_tables:
        print("\n✅ Todas as tabelas foram criadas corretamente!")
        logger.info(f"Validação OK - PRD: {prd_db} ({total_prd_tables} tabelas) = HML: {hml_db} ({total_hml_tables} tabelas)")
        return True
    else:
        print(f"\n⚠️  Diferença encontrada: {abs(total_prd_tables - total_hml_tables)} tabelas")
        logger.warning(f"Validação com diferenças - PRD: {prd_db} ({total_prd_tables} tabelas) vs HML: {hml_db} ({total_hml_tables} tabelas)")
        return False


def main(drop_existing: bool = False):
    """Executa o processo completo de clonagem de todos os schemas e tabelas
    
    Args:
        drop_existing: Se True, remove schemas e tabelas existentes antes de recriar
    """
    print("\n" + "="*80)
    print("CLONAGEM DE TODOS OS SCHEMAS E TABELAS: PRD -> HML")
    print("="*80)
    
    if drop_existing:
        print("\n⚠️  MODO: DROP E RECONSTRUÇÃO COMPLETA")
        print("   Todos os schemas e tabelas existentes serão removidos antes de recriar!")
    else:
        print("\nℹ️  MODO: CRIAÇÃO INCREMENTAL")
        print("   Schemas e tabelas existentes serão preservados (pulados)")
    
    config_prd = DatabaseConnection.POSTGRESQL_PRD_CONFIG
    config_hml = DatabaseConnection.POSTGRESQL_HML_DESTINO_CONFIG
    
    print(f"\nORIGEM (PRD):")
    print(f"  Database: {config_prd['database']}")
    print(f"  Host: {config_prd['host']}")
    
    print(f"\nDESTINO (HML):")
    print(f"  Database: {config_hml['database']}")
    print(f"  Host: {config_hml['host']}")
    
    print("\n" + "="*80)
    
    try:
        # Etapa 1: Listar schemas
        prd_schemas = get_all_schemas_from_prd()
        
        if not prd_schemas:
            print("\nAVISO - Nenhum schema encontrado no PRD")
            return
        
        # Etapa 2: Criar schemas no HML
        print("\n" + "="*80)
        print("ETAPA 2: Criando schemas no HML")
        print("="*80)
        
        schemas_created = 0
        schemas_existing = 0
        
        for prd_schema in prd_schemas:
            hml_schema = get_schema_mapping(prd_schema)
            print(f"\nProcessando schema: {prd_schema} -> {hml_schema}")
            
            try:
                if create_schema_in_hml(hml_schema, drop_existing=drop_existing):
                    action = "recriado" if drop_existing else "criado"
                    print(f"  ✅ Schema {hml_schema} {action}")
                    schemas_created += 1
                else:
                    print(f"  ℹ️  Schema {hml_schema} já existe")
                    schemas_existing += 1
            except Exception as e:
                print(f"  ❌ Erro ao criar schema {hml_schema}: {e}")
                logger.error(f"Erro ao criar schema {hml_schema}: {e}")
                continue
        
        # Etapa 3: Clonar tabelas de cada schema
        print("\n" + "="*80)
        print("ETAPA 3: Clonando tabelas para o HML")
        print("="*80)
        
        total_tables_processed = 0
        total_tables_created = 0
        total_tables_existing = 0
        total_errors = 0
        
        for prd_schema in prd_schemas:
            hml_schema = get_schema_mapping(prd_schema)
            
            print(f"\n{'='*80}")
            print(f"Schema: {prd_schema} -> {hml_schema}")
            print(f"{'='*80}")
            
            # Listar tabelas do schema
            tables = get_all_tables_from_schema(prd_schema)
            
            if not tables:
                print(f"  ℹ️  Nenhuma tabela encontrada no schema {prd_schema}")
                continue
            
            print(f"  📋 Encontradas {len(tables)} tabelas")
            
            for i, table_name in enumerate(tables, 1):
                total_tables_processed += 1
                print(f"\n  [{i}/{len(tables)}] Processando tabela: {table_name}")
                logger.info(f"Processando tabela {i}/{len(tables)}: {prd_schema}.{table_name} -> {hml_schema}.{table_name}")
                
                try:
                    # Obter DDL
                    ddl = get_table_ddl(table_name, prd_schema)
                    
                    # Criar no HML
                    if create_table_in_hml(table_name, ddl, hml_schema, drop_existing=drop_existing):
                        action = "recriada" if drop_existing else "criada"
                        print(f"    ✅ Tabela {hml_schema}.{table_name} {action} com sucesso!")
                        total_tables_created += 1
                    else:
                        print(f"    ℹ️  Tabela {hml_schema}.{table_name} já existe ou foi pulada")
                        total_tables_existing += 1
                    
                except Exception as e:
                    print(f"    ❌ Erro ao processar tabela {prd_schema}.{table_name}: {e}")
                    logger.error(f"Erro ao processar tabela {prd_schema}.{table_name}: {e}")
                    total_errors += 1
                    continue
        
        # Etapa 4: Resumo e Validação
        print("\n" + "="*80)
        print("RESUMO")
        print("="*80)
        print(f"Schemas processados: {len(prd_schemas)}")
        print(f"  - Schemas criados: {schemas_created}")
        print(f"  - Schemas já existentes: {schemas_existing}")
        print(f"\nTabelas processadas: {total_tables_processed}")
        print(f"  - Tabelas criadas: {total_tables_created}")
        print(f"  - Tabelas já existentes: {total_tables_existing}")
        print(f"  - Erros: {total_errors}")
        
        # Validação final
        validate_schemas_and_tables()
        
        print("\n" + "="*80)
        print("PROCESSO CONCLUÍDO")
        print("="*80)
        
    except Exception as e:
        logger.error(f"Erro crítico no processo: {e}")
        print(f"\n❌ ERRO CRÍTICO: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Clona todos os schemas e tabelas do PostgreSQL PRD para HML'
    )
    parser.add_argument(
        '--drop-existing',
        action='store_true',
        help='Remove schemas e tabelas existentes antes de recriar (DROP CASCADE)'
    )
    
    args = parser.parse_args()
    
    main(drop_existing=args.drop_existing)
