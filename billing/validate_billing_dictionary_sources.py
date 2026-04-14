"""
Valida conexões e estruturas usadas pelo billing_dictionary.txt:
- SQL Server PRD (FINANCEIRO): colunas de dbo.ViewFaturamentoOrcamento
- PostgreSQL PRD: financial.billings
- PostgreSQL HML: gmfinancial.billings (supera_dev_seed)

Uso: python billing/validate_billing_dictionary_sources.py
"""

import os
import sys

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from utils.database_connection import DatabaseConnection

QUERY_VIEW_COLUMNS = """
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
FROM FINANCEIRO.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'ViewFaturamentoOrcamento'
ORDER BY ORDINAL_POSITION;
"""

QUERY_PG_BILLINGS = """
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = %s AND table_name = 'billings'
ORDER BY ordinal_position;
"""


def print_sql_server_view():
    print("=" * 80)
    print("SQL Server PRD — dbo.ViewFaturamentoOrcamento (FINANCEIRO)")
    print("=" * 80)
    rows = DatabaseConnection.execute_sql_server_prd_query(QUERY_VIEW_COLUMNS)
    if not rows:
        print("Nenhuma coluna encontrada (view inexistente ou sem permissão).")
        return
    print(f"\n{'COLUMN_NAME':<42} {'DATA_TYPE':<18} {'LEN':<8} NULL")
    print("-" * 90)
    for r in rows:
        name = r.get("COLUMN_NAME", "")
        dtype = r.get("DATA_TYPE", "")
        length = r.get("CHARACTER_MAXIMUM_LENGTH")
        if length is not None and length == -1:
            length_str = "MAX"
        elif length is not None:
            length_str = str(length)
        else:
            length_str = "-"
        null = r.get("IS_NULLABLE", "")
        print(f"{name:<42} {dtype:<18} {length_str:<8} {null}")
    print("-" * 90)
    print(f"Total colunas: {len(rows)}")


def print_postgres_billings(schema: str, label: str):
    print("\n" + "=" * 80)
    print(f"PostgreSQL — {label} — {schema}.billings")
    print("=" * 80)
    conn = None
    cur = None
    try:
        if "HML" in label.upper():
            conn = DatabaseConnection.get_postgresql_hml_destino_connection()
        else:
            conn = DatabaseConnection.get_postgresql_prd_connection()
        cur = conn.cursor()
        cur.execute(QUERY_PG_BILLINGS, (schema,))
        rows = cur.fetchall()
        colnames = [d[0] for d in cur.description]
        if not rows:
            print(f"Nenhuma coluna (tabela ausente ou schema incorreto).")
            return
        print(f"\n{'column_name':<42} {'data_type':<22} nullable")
        print("-" * 80)
        for row in rows:
            d = dict(zip(colnames, row))
            print(
                f"{d.get('column_name', ''):<42} "
                f"{d.get('data_type', ''):<22} "
                f"{d.get('is_nullable', '')}"
            )
        print("-" * 80)
        print(f"Total colunas: {len(rows)}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def main():
    ok = True
    print("\nValidação de fontes — billing_dictionary / contract_dictionary pattern\n")

    try:
        print_sql_server_view()
    except Exception as e:
        ok = False
        print(f"\n[ERRO] SQL Server: {e}")

    try:
        print_postgres_billings("financial", "PostgreSQL PRD (gmcoredb)")
    except Exception as e:
        ok = False
        print(f"\n[ERRO] PostgreSQL PRD financial.billings: {e}")

    try:
        print_postgres_billings("gmfinancial", "PostgreSQL HML (supera_dev_seed)")
    except Exception as e:
        ok = False
        print(f"\n[ERRO] PostgreSQL HML gmfinancial.billings: {e}")

    print("\n" + "=" * 80)
    if ok:
        print("Validação concluída sem erros de conexão/consulta.")
    else:
        print("Validação concluída com erros (ver mensagens acima).")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
