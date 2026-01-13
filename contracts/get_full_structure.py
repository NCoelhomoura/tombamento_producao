"""
Script para obter estrutura completa das views e tabelas principais
"""

# Configurar encoding para evitar problemas no Windows
import sys
import io
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database_connection import DatabaseConnection

def get_structure(schema, name, is_view=False):
    """Obtém estrutura completa"""
    conn = None
    try:
        conn = DatabaseConnection.get_sql_server_prd_connection()
        cursor = conn.cursor()
        
        table_type = 'VIEWS' if is_view else 'TABLES'
        
        cursor.execute(f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{name}'
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        
        pk_columns = []
        if not is_view:
            cursor.execute(f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = '{schema}'
                AND TABLE_NAME = '{name}'
                AND CONSTRAINT_NAME LIKE 'PK_%'
                ORDER BY ORDINAL_POSITION
            """)
            pk_columns = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return columns, pk_columns
        
    except Exception as e:
        print(f"ERRO ao obter estrutura de {schema}.{name}: {e}")
        if conn:
            conn.close()
        return None, None


def print_structure(title, schema, name, columns, pk_columns=None, is_view=False):
    """Imprime estrutura formatada"""
    print(f"\n{'='*100}")
    print(f"{title}: {schema}.{name}")
    print(f"{'='*100}")
    
    if not columns:
        print("Nenhuma coluna encontrada")
        return
    
    print(f"\n{'Nome':<45} {'Tipo':<30} {'Tamanho':<12} {'Nullable':<10}", end='')
    if not is_view:
        print(f"{'PK':<5}")
    else:
        print()
    print(f"{'-'*100}")
    
    for col in columns:
        col_name = col[0]
        data_type = col[1]
        max_length = col[2] if col[2] else ''
        precision = col[3] if col[3] else ''
        scale = col[4] if col[4] else ''
        nullable = col[5]
        
        if data_type in ['nvarchar', 'varchar', 'char', 'nchar']:
            if max_length == -1:
                type_str = f"{data_type}(MAX)"
            else:
                type_str = f"{data_type}({max_length})" if max_length else data_type
        elif data_type in ['numeric', 'decimal']:
            type_str = f"{data_type}({precision},{scale})" if precision and scale else data_type
        else:
            type_str = data_type
        
        print(f"{col_name:<45} {type_str:<30} {str(max_length):<12} {nullable:<10}", end='')
        if not is_view:
            is_pk = 'SIM' if col_name in (pk_columns or []) else 'NAO'
            print(f"{is_pk:<5}")
        else:
            print()


def main():
    """Executa investigação completa"""
    print("\n" + "="*100)
    print("INVESTIGAÇÃO COMPLETA - ESTRUTURAS DE ORCAMENTOS")
    print("="*100)
    
    # 1. ViewOrcamentosLojas (view principal)
    print("\n\n" + "="*100)
    print("VIEW PRINCIPAL: ViewOrcamentosLojas")
    print("="*100)
    cols, _ = get_structure('dbo', 'ViewOrcamentosLojas', is_view=True)
    print_structure("VIEW", 'dbo', 'ViewOrcamentosLojas', cols, is_view=True)
    
    # 2. Tabela Orcamento
    print("\n\n" + "="*100)
    print("TABELA PRINCIPAL: Orcamento")
    print("="*100)
    cols, pks = get_structure('dbo', 'Orcamento', is_view=False)
    print_structure("TABELA", 'dbo', 'Orcamento', cols, pks)
    
    # 3. Tabela OrcamentoLojas
    print("\n\n" + "="*100)
    print("TABELA: OrcamentoLojas")
    print("="*100)
    cols, pks = get_structure('dbo', 'OrcamentoLojas', is_view=False)
    print_structure("TABELA", 'dbo', 'OrcamentoLojas', cols, pks)
    
    # 4. Tabela FaturamentoOrcamentoComissao (para vendedores e comissão)
    print("\n\n" + "="*100)
    print("TABELA: FaturamentoOrcamentoComissao (Vendedores e Comissão)")
    print("="*100)
    cols, pks = get_structure('dbo', 'FaturamentoOrcamentoComissao', is_view=False)
    print_structure("TABELA", 'dbo', 'FaturamentoOrcamentoComissao', cols, pks)


if __name__ == "__main__":
    main()




