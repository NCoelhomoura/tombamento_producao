"""
Compara tabelas PRD (gmcoredb) vs HML (supera_dev_seed) por par de schema
e cria em HML as tabelas que existem no PRD mas faltam no HML.

Pares (mesmo mapeamento do clone_all_schemas):
  core -> gmcore, commercial -> gmcommercial, pdv -> gmpdv, financial -> gmfinancial

Uso:
  python sync_missing_tables_prd_to_hml.py           # dry-run (só lista)
  python sync_missing_tables_prd_to_hml.py --apply # cria tabelas em falta
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "utils"))
from database_connection import DatabaseConnection

sys.path.insert(0, os.path.dirname(__file__))
from clone_all_schemas import (
    create_schema_in_hml,
    create_table_in_hml,
    get_all_tables_from_schema,
    get_schema_mapping,
    get_table_ddl,
)

# Schemas de destino da migração (PRD -> HML com prefixo gm)
SCHEMA_PAIRS = [
    ("core", "gmcore"),
    ("commercial", "gmcommercial"),
    ("pdv", "gmpdv"),
    ("financial", "gmfinancial"),
]


def get_tables_hml(hml_schema: str) -> list[str]:
    conn = DatabaseConnection.get_postgresql_hml_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
        """,
        (hml_schema,),
    )
    out = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Cria no HML as tabelas em falta (DDL vindo do PRD)",
    )
    args = ap.parse_args()

    print("=" * 80)
    print("PRD vs HML — comparação de tabelas (schemas de migração)")
    print("=" * 80)

    all_missing: list[tuple[str, str, str]] = []

    for prd_schema, hml_schema in SCHEMA_PAIRS:
        assert get_schema_mapping(prd_schema) == hml_schema
        prd_tables = set(get_all_tables_from_schema(prd_schema))
        hml_tables = set(get_tables_hml(hml_schema))
        missing = sorted(prd_tables - hml_tables)
        extra = sorted(hml_tables - prd_tables)

        print(f"\n--- {prd_schema} -> {hml_schema} ---")
        print(f"  PRD: {len(prd_tables)} tabelas | HML: {len(hml_tables)} tabelas")
        if missing:
            print(f"  FALTAM no HML ({len(missing)}): {missing[:30]}{'...' if len(missing) > 30 else ''}")
            for t in missing:
                all_missing.append((prd_schema, hml_schema, t))
        else:
            print("  OK — nenhuma tabela a mais no PRD em relação ao HML.")
        if extra:
            print(f"  [INFO] Tabelas só no HML (não removidas): {len(extra)}")

    print("\n" + "=" * 80)
    print(f"Total de tabelas a criar no HML: {len(all_missing)}")
    print("=" * 80)

    if not all_missing:
        return 0

    if not args.apply:
        print("\nDry-run: nada foi criado. Execute com --apply para criar as tabelas em falta.")
        return 1

    for prd_schema, hml_schema, table_name in all_missing:
        print(f"\nCriando {hml_schema}.{table_name} (origem PRD {prd_schema})...")
        try:
            create_schema_in_hml(hml_schema, drop_existing=False)
            ddl = get_table_ddl(table_name, prd_schema)
            if not ddl or not ddl.strip():
                print(f"  ERRO: DDL vazio para {prd_schema}.{table_name}")
                continue
            create_table_in_hml(table_name, ddl, hml_schema, drop_existing=False)
        except Exception as e:
            print(f"  ERRO: {e}")
            raise

    print("\nConcluído. Rode sem --apply para validar contagens ou confira no banco.")
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
