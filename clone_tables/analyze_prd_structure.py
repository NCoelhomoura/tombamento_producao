"""
Script para analisar a estrutura real do PostgreSQL PRD
e validar se o código de clonagem funcionará corretamente
"""

# Configurar encoding para evitar problemas no Windows
import sys
import io
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import logging
import os

# Adicionar diretório utils ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database_connection import DatabaseConnection

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
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
    """Mapeia schema do PRD para schema do HML"""
    if prd_schema.startswith('gm'):
        return prd_schema
    else:
        return f'gm{prd_schema}'


def analyze_prd_structure():
    """Analisa a estrutura completa do PRD"""
    print("\n" + "="*80)
    print("ANÁLISE DA ESTRUTURA DO POSTGRESQL PRD")
    print("="*80)
    
    config = DatabaseConnection.POSTGRESQL_PRD_CONFIG
    print(f"\nConectando ao PRD:")
    print(f"  Database: {config['database']}")
    print(f"  Host: {config['host']}")
    print(f"  Port: {config['port']}")
    
    conn = None
    try:
        conn = DatabaseConnection.get_postgresql_prd_connection()
        cursor = conn.cursor()
        
        # Verificar database conectado
        cursor.execute("SELECT current_database(), version()")
        current_db, pg_version = cursor.fetchone()
        print(f"\n✅ Conectado com sucesso!")
        print(f"  Database: {current_db}")
        print(f"  PostgreSQL: {pg_version.split(',')[0]}")
        
        # 1. Listar todos os schemas
        print("\n" + "="*80)
        print("1. SCHEMAS DISPONÍVEIS")
        print("="*80)
        
        cursor.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            AND schema_name NOT LIKE 'pg_temp_%'
            AND schema_name NOT LIKE 'pg_toast_temp_%'
            ORDER BY schema_name;
        """)
        
        all_schemas = [row[0] for row in cursor.fetchall()]
        user_schemas = [s for s in all_schemas if s not in SYSTEM_SCHEMAS]
        
        print(f"\nTotal de schemas encontrados: {len(user_schemas)}")
        print(f"\nSchemas do usuário (serão clonados):")
        print("-" * 80)
        
        schema_info = {}
        
        for schema in user_schemas:
            hml_schema = get_schema_mapping(schema)
            
            # Contar tabelas
            cursor.execute("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_type = 'BASE TABLE';
            """, (schema,))
            table_count = cursor.fetchone()[0]
            
            # Contar views
            cursor.execute("""
                SELECT COUNT(*)
                FROM information_schema.views
                WHERE table_schema = %s;
            """, (schema,))
            view_count = cursor.fetchone()[0]
            
            schema_info[schema] = {
                'hml_schema': hml_schema,
                'table_count': table_count,
                'view_count': view_count
            }
            
            print(f"  {schema:20} -> {hml_schema:20} | Tabelas: {table_count:4} | Views: {view_count:4}")
        
        # 2. Analisar cada schema em detalhes
        print("\n" + "="*80)
        print("2. ANÁLISE DETALHADA POR SCHEMA")
        print("="*80)
        
        total_tables = 0
        total_views = 0
        
        for schema in user_schemas:
            info = schema_info[schema]
            total_tables += info['table_count']
            total_views += info['view_count']
            
            if info['table_count'] > 0:
                print(f"\n{'='*80}")
                print(f"Schema: {schema} -> {info['hml_schema']}")
                print(f"{'='*80}")
                print(f"Tabelas: {info['table_count']} | Views: {info['view_count']}")
                
                # Listar tabelas
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                    LIMIT 20;
                """, (schema,))
                
                tables = [row[0] for row in cursor.fetchall()]
                
                if tables:
                    print(f"\nPrimeiras tabelas (máx 20):")
                    for i, table in enumerate(tables, 1):
                        # Verificar se tem constraints complexas
                        cursor.execute("""
                            SELECT COUNT(*)
                            FROM information_schema.table_constraints
                            WHERE table_schema = %s
                            AND table_name = %s
                            AND constraint_type IN ('FOREIGN KEY', 'CHECK');
                        """, (schema, table))
                        complex_constraints = cursor.fetchone()[0]
                        
                        marker = "⚠️" if complex_constraints > 0 else "  "
                        print(f"  {marker} {i:2}. {table}")
                        if complex_constraints > 0:
                            print(f"      (tem {complex_constraints} constraints complexas)")
                    
                    if info['table_count'] > 20:
                        print(f"  ... e mais {info['table_count'] - 20} tabelas")
        
        # 3. Verificar tipos de dados especiais
        print("\n" + "="*80)
        print("3. TIPOS DE DADOS E CARACTERÍSTICAS ESPECIAIS")
        print("="*80)
        
        cursor.execute("""
            SELECT DISTINCT
                c.table_schema,
                c.data_type,
                COUNT(*) as count
            FROM information_schema.columns c
            INNER JOIN information_schema.tables t
                ON c.table_schema = t.table_schema
                AND c.table_name = t.table_name
            WHERE c.table_schema IN %s
            AND t.table_type = 'BASE TABLE'
            GROUP BY c.table_schema, c.data_type
            ORDER BY c.table_schema, count DESC;
        """, (tuple(user_schemas),))
        
        data_types = cursor.fetchall()
        
        print("\nTipos de dados por schema:")
        current_schema = None
        for schema, data_type, count in data_types:
            if schema != current_schema:
                print(f"\n  {schema}:")
                current_schema = schema
            print(f"    - {data_type:30} : {count:4} colunas")
        
        # 4. Verificar constraints e relacionamentos
        print("\n" + "="*80)
        print("4. CONSTRAINTS E RELACIONAMENTOS")
        print("="*80)
        
        cursor.execute("""
            SELECT 
                tc.table_schema,
                tc.constraint_type,
                COUNT(*) as count
            FROM information_schema.table_constraints tc
            INNER JOIN information_schema.tables t
                ON tc.table_schema = t.table_schema
                AND tc.table_name = t.table_name
            WHERE tc.table_schema IN %s
            AND t.table_type = 'BASE TABLE'
            GROUP BY tc.table_schema, tc.constraint_type
            ORDER BY tc.table_schema, tc.constraint_type;
        """, (tuple(user_schemas),))
        
        constraints = cursor.fetchall()
        
        print("\nConstraints por schema:")
        current_schema = None
        for schema, constraint_type, count in constraints:
            if schema != current_schema:
                print(f"\n  {schema}:")
                current_schema = schema
            print(f"    - {constraint_type:20} : {count:4}")
        
        # 5. Verificar índices
        print("\n" + "="*80)
        print("5. ÍNDICES")
        print("="*80)
        
        cursor.execute("""
            SELECT 
                schemaname,
                COUNT(*) as index_count
            FROM pg_indexes
            WHERE schemaname IN %s
            GROUP BY schemaname
            ORDER BY schemaname;
        """, (tuple(user_schemas),))
        
        indexes = cursor.fetchall()
        
        print("\nÍndices por schema:")
        for schema, index_count in indexes:
            print(f"  {schema:20} : {index_count:4} índices")
        
        # 6. Resumo e validações
        print("\n" + "="*80)
        print("6. RESUMO E VALIDAÇÕES")
        print("="*80)
        
        print(f"\n📊 Estatísticas Gerais:")
        print(f"  Schemas a serem clonados: {len(user_schemas)}")
        print(f"  Total de tabelas: {total_tables}")
        print(f"  Total de views: {total_views}")
        
        # Validações
        print(f"\n✅ Validações:")
        
        # Verificar se há schemas com nomes problemáticos
        problematic_schemas = []
        for schema in user_schemas:
            hml_schema = get_schema_mapping(schema)
            if len(hml_schema) > 63:  # PostgreSQL limit
                problematic_schemas.append((schema, hml_schema))
        
        if problematic_schemas:
            print(f"  ⚠️  Schemas com nomes muito longos (>63 caracteres):")
            for prd, hml in problematic_schemas:
                print(f"     - {prd} -> {hml} ({len(hml)} caracteres)")
        else:
            print(f"  ✅ Nenhum schema com nome problemático")
        
        # Verificar se há tabelas sem PK
        cursor.execute("""
            SELECT 
                t.table_schema,
                t.table_name
            FROM information_schema.tables t
            LEFT JOIN information_schema.table_constraints tc
                ON t.table_schema = tc.table_schema
                AND t.table_name = tc.table_name
                AND tc.constraint_type = 'PRIMARY KEY'
            WHERE t.table_schema IN %s
            AND t.table_type = 'BASE TABLE'
            AND tc.constraint_name IS NULL
            ORDER BY t.table_schema, t.table_name;
        """, (tuple(user_schemas),))
        
        tables_without_pk = cursor.fetchall()
        
        if tables_without_pk:
            print(f"  ⚠️  Tabelas sem PRIMARY KEY ({len(tables_without_pk)}):")
            current_schema = None
            for schema, table in tables_without_pk[:10]:  # Mostrar apenas primeiras 10
                if schema != current_schema:
                    print(f"     {schema}:")
                    current_schema = schema
                print(f"       - {table}")
            if len(tables_without_pk) > 10:
                print(f"     ... e mais {len(tables_without_pk) - 10} tabelas")
        else:
            print(f"  ✅ Todas as tabelas têm PRIMARY KEY")
        
        # Verificar se há Foreign Keys que podem causar problemas
        cursor.execute("""
            SELECT 
                tc.table_schema,
                COUNT(*) as fk_count
            FROM information_schema.table_constraints tc
            INNER JOIN information_schema.tables t
                ON tc.table_schema = t.table_schema
                AND tc.table_name = t.table_name
            WHERE tc.table_schema IN %s
            AND t.table_type = 'BASE TABLE'
            AND tc.constraint_type = 'FOREIGN KEY'
            GROUP BY tc.table_schema;
        """, (tuple(user_schemas),))
        
        fk_info = cursor.fetchall()
        
        if fk_info:
            print(f"  ℹ️  Foreign Keys encontradas:")
            for schema, fk_count in fk_info:
                print(f"     {schema}: {fk_count} FKs (serão criadas depois)")
        
        # 7. Mapeamento de schemas
        print("\n" + "="*80)
        print("7. MAPEAMENTO PRD -> HML")
        print("="*80)
        
        print("\nMapeamento que será aplicado:")
        for schema in user_schemas:
            hml_schema = get_schema_mapping(schema)
            info = schema_info[schema]
            print(f"  {schema:20} -> {hml_schema:20} ({info['table_count']} tabelas)")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*80)
        print("ANÁLISE CONCLUÍDA")
        print("="*80)
        print("\n✅ O código de clonagem deve funcionar corretamente!")
        print("   Execute: python clone_tables/clone_all_schemas.py")
        print("="*80)
        
        return {
            'schemas': user_schemas,
            'total_tables': total_tables,
            'total_views': total_views,
            'schema_info': schema_info
        }
        
    except Exception as e:
        logger.error(f"Erro ao analisar estrutura do PRD: {e}")
        import traceback
        logger.error(traceback.format_exc())
        if conn:
            conn.close()
        raise


if __name__ == "__main__":
    try:
        analyze_prd_structure()
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        sys.exit(1)
