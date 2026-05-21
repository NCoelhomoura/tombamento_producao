"""
IdCanalEstabelecimento a partir da tabela Estabelecimento no SQL Server.

ViewOrcamentosLojas no PRD não expõe IdCanalEstabelecimento; usar esta função após obter IdEstabelecimento.
"""

from typing import List

from utils.database_connection import DatabaseConnection


def fetch_distinct_id_canal_from_estabelecimento_ids(estabelecimento_ids: List[int]) -> List[int]:
    if not estabelecimento_ids:
        return []
    conn_sql = DatabaseConnection.get_sql_server_prd_connection()
    cursor_sql = conn_sql.cursor()
    found: List[int] = []
    chunk_sz = 1000
    try:
        for i in range(0, len(estabelecimento_ids), chunk_sz):
            chunk = estabelecimento_ids[i : i + chunk_sz]
            placeholders = ",".join(["?" for _ in chunk])
            cursor_sql.execute(
                f"""
                SELECT DISTINCT IdCanalEstabelecimento
                FROM Estabelecimento
                WHERE Id IN ({placeholders}) AND IdCanalEstabelecimento IS NOT NULL
                """,
                chunk,
            )
            found.extend(row[0] for row in cursor_sql.fetchall())
    finally:
        cursor_sql.close()
        conn_sql.close()
    return sorted(set(found))
