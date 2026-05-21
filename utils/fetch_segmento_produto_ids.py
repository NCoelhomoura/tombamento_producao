"""
IdSegmentoProduto (SegmentoProduto.Id) para escopo de migração.

Ordem no SQL Server (mesmo WHERE que IdEstabelecimento: IdOrcamento + datas + status):
1) ViewOrcamentosLojas.SegmentosProdutos = SegmentoProduto.Nome (espaços removidos)
2) ViewOrcamentosLojas.IdSegmentoProduto (se existir)
3) Orcamento.IdSegmentoProduto via join na view
4) Cliente.IdSegmentoProduto via join na view
"""

import logging
from typing import Any, List, Optional, Sequence

from utils.database_connection import DatabaseConnection

logger = logging.getLogger(__name__)


def _invalid_column_error(exc: Exception) -> bool:
    s = str(exc)
    return "42S22" in s or "Invalid column name" in s


def fetch_distinct_id_segmento_produto_from_cliente_ids(cliente_ids: List[int]) -> List[int]:
    if not cliente_ids:
        return []
    conn_sql = DatabaseConnection.get_sql_server_prd_connection()
    cursor_sql = conn_sql.cursor()
    found: List[int] = []
    chunk_sz = 1000
    try:
        for i in range(0, len(cliente_ids), chunk_sz):
            chunk = cliente_ids[i : i + chunk_sz]
            placeholders = ",".join(["?" for _ in chunk])
            try:
                cursor_sql.execute(
                    f"""
                    SELECT DISTINCT IdSegmentoProduto
                    FROM Cliente
                    WHERE Id IN ({placeholders}) AND IdSegmentoProduto IS NOT NULL
                    """,
                    chunk,
                )
                found.extend(row[0] for row in cursor_sql.fetchall())
            except Exception as e:
                if _invalid_column_error(e):
                    logger.warning(
                        "[fetch_segmento] Cliente.IdSegmentoProduto indisponível no ERP — "
                        "IdSegmentoProduto via Cliente não aplicado: %s",
                        e,
                    )
                    return []
                raise
    finally:
        cursor_sql.close()
        conn_sql.close()
    return sorted(set(found))


def fetch_distinct_id_segmento_produto_from_view_filters(
    id_orcamento: List[int],
    data_aviso_previo_min: Optional[str] = None,
    data_inicio_operacao_max: Optional[str] = None,
    status_pedido: Optional[List[int]] = None,
) -> List[int]:
    """
    DISTINCT IdSegmentoProduto no escopo da view (IdOrcamento XLSX/CLI + datas + status).
    Tenta join SegmentosProdutos↔Nome; depois coluna IdSegmentoProduto na view; depois Orcamento / Cliente.
    """
    if not id_orcamento:
        return []
    status_pedido = status_pedido or []
    where_conditions: List[str] = []
    query_params: List = []
    placeholders = ",".join(["?" for _ in id_orcamento])
    where_conditions.append(f"v.IdOrcamento IN ({placeholders})")
    query_params.extend(id_orcamento)
    if data_aviso_previo_min:
        where_conditions.append(
            "(CONVERT(DATE, v.DataAvisoPrevio) >= ? OR v.DataAvisoPrevio IS NULL)"
        )
        query_params.append(data_aviso_previo_min)
    if data_inicio_operacao_max:
        where_conditions.append("CONVERT(DATE, v.DataInicioOperacao) <= ?")
        query_params.append(data_inicio_operacao_max)
    if len(status_pedido) > 0:
        sph = ",".join(["?" for _ in status_pedido])
        where_conditions.append(f"v.StatusPedido IN ({sph})")
        query_params.extend(status_pedido)
    where_clause = "WHERE " + " AND ".join(where_conditions)

    sql_view_column = f"""
        SELECT DISTINCT
            v.IdSegmentoProduto
        FROM ViewOrcamentosLojas v
        {where_clause}
        AND v.IdSegmentoProduto IS NOT NULL
    """
    sql_segmentos_produtos_nome = f"""
        SELECT DISTINCT
            sp.Id
        FROM ViewOrcamentosLojas v
        INNER JOIN SegmentoProduto sp
            ON REPLACE(LTRIM(RTRIM(ISNULL(v.SegmentosProdutos, ''))), ' ', '')
             = REPLACE(LTRIM(RTRIM(ISNULL(sp.Nome, ''))), ' ', '')
        {where_clause}
        AND NULLIF(LTRIM(RTRIM(v.SegmentosProdutos)), '') IS NOT NULL
    """
    sql_orcamento = f"""
        SELECT DISTINCT
            o.IdSegmentoProduto
        FROM ViewOrcamentosLojas v
        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
        {where_clause}
        AND o.IdSegmentoProduto IS NOT NULL
    """
    sql_cliente = f"""
        SELECT DISTINCT
            c.IdSegmentoProduto
        FROM ViewOrcamentosLojas v
        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
        INNER JOIN Cliente c ON c.Id = v.IdCliente
        {where_clause}
        AND v.IdCliente IS NOT NULL
        AND c.IdSegmentoProduto IS NOT NULL
    """

    conn_sql = DatabaseConnection.get_sql_server_prd_connection()
    cursor_sql = conn_sql.cursor()
    try:
        for label, sql in (
            ("SegmentosProdutos→SegmentoProduto.Nome", sql_segmentos_produtos_nome),
            ("ViewOrcamentosLojas.IdSegmentoProduto", sql_view_column),
            ("Orcamento", sql_orcamento),
            ("Cliente", sql_cliente),
        ):
            try:
                cursor_sql.execute(sql, query_params)
                out = sorted(
                    {int(row[0]) for row in cursor_sql.fetchall() if row[0] is not None}
                )
                if out:
                    return out
            except Exception as e:
                if _invalid_column_error(e):
                    logger.warning(
                        "[fetch_segmento] Caminho %s indisponível: %s",
                        label,
                        e,
                    )
                    continue
                raise
        return []
    finally:
        cursor_sql.close()
        conn_sql.close()


def _as_yyyy_mm_dd(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value)


def resolve_id_segmento_produto_for_json(
    id_cliente_list: Sequence[Any],
    id_orcamento_list: Sequence[Any],
    data_aviso_previo_min: Any = None,
    data_inicio_operacao_max: Any = None,
    status_pedido: Any = None,
) -> List[int]:
    """
    IdSegmentoProduto para aggregated_ids: mesma origem que IdEstabelecimento —
    ViewOrcamentosLojas com IdOrcamento (+ datas/status); depois Cliente.
    """
    orch = [int(x) for x in (id_orcamento_list or []) if x is not None]
    st = status_pedido or []
    if st is None:
        st = []
    if not isinstance(st, (list, tuple)):
        st = [st]
    st_int = [int(x) for x in st if x is not None]
    if orch:
        seg = fetch_distinct_id_segmento_produto_from_view_filters(
            orch,
            _as_yyyy_mm_dd(data_aviso_previo_min),
            _as_yyyy_mm_dd(data_inicio_operacao_max),
            st_int,
        )
        if seg:
            return seg
    clientes = [int(x) for x in id_cliente_list if x is not None]
    return fetch_distinct_id_segmento_produto_from_cliente_ids(clientes)
