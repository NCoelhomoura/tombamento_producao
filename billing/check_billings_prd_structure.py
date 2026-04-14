"""
Quick script to query the structure of financial.billings table in PostgreSQL PRD.
"""

import sys
import os

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from utils.database_connection import DatabaseConnection

QUERY = """
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'financial' AND table_name = 'billings'
ORDER BY ordinal_position;
"""


def main():
    print("=" * 80)
    print("financial.billings - Table Structure (PostgreSQL PRD)")
    print("=" * 80)
    try:
        results = DatabaseConnection.execute_postgresql_prd_query(QUERY)
        if not results:
            print("No columns found. Table may not exist.")
            return
        print(f"\n{'column_name':<35} {'data_type':<25} {'is_nullable':<12} column_default")
        print("-" * 100)
        for row in results:
            col = row.get("column_name", "")
            dtype = row.get("data_type", "")
            nullable = row.get("is_nullable", "")
            default = row.get("column_default") or ""
            if default and len(str(default)) > 40:
                default = str(default)[:37] + "..."
            print(f"{col:<35} {dtype:<25} {nullable:<12} {default}")
        print("-" * 100)
        print(f"Total columns: {len(results)}")
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
