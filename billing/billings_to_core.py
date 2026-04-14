"""
Migração: billings (gmfinancial / financial) alinhada a billing/billing_dictionary.txt
e PONTOS_A_FECHAR (BILL-001/002/003).

- Escopo IdOrcamento: interseção aggregated_ids (ViewOrcamentosLojas) ∩ XLSX id_orcamento,
  considerando apenas linhas do XLSX com orcamento_ativo = true (coluna ausente: não filtra).
- Limpeza antes do INSERT: TRUNCATE se clear_data ou sem filtros (igual contracts);
  com filtros e sem clear_data: DELETE só nos billings dos customers do escopo.
- Um registro de billing por customer_id distinto nesse escopo (mesmo modelo do placeholder).
- contract_billing_map: cada contrato no escopo aponta para o billing do seu cliente.
"""

from __future__ import annotations

import os
import sys
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

utils_path = os.path.join(os.path.dirname(__file__), "..", "utils")
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from utils.database_connection import DatabaseConnection

logger = logging.getLogger(__name__)

XLSX_DEFAULT = os.path.join(
    os.path.dirname(__file__), "dados_externos", "contratos_ativos.xlsx"
)


def _schema_gmcore() -> str:
    return "gmcore" if DatabaseConnection.get_destino() == "HML" else "core"


def _schema_financial() -> str:
    return "gmfinancial" if DatabaseConnection.get_destino() == "HML" else "financial"


def _normalize_xlsx_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        str(c).strip().lower().replace(" ", "_") for c in df.columns
    ]
    return df


def _map_calculation_type(desc: Any) -> str:
    if desc is None or (isinstance(desc, float) and pd.isna(desc)):
        return "four_weeks_in_month"
    s = str(desc).strip().lower()
    if "5" in s and "week" in s:
        return "five_weeks_in_month"
    if "4" in s and "week" in s:
        return "four_weeks_in_month"
    return "four_weeks_in_month"


def _safe_int(val: Any, default: int) -> int:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _safe_bool(val: Any, default: bool = True) -> bool:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in ("true", "1", "sim", "yes"):
        return True
    if s in ("false", "0", "nao", "não", "no"):
        return False
    return default


def _load_xlsx_index(path: str) -> Tuple[pd.DataFrame, Dict[int, Dict[str, Any]]]:
    """Retorna DataFrame e mapa id_orcamento -> linha como dict."""
    if not os.path.isfile(path):
        logger.warning(f"[BILLINGS] XLSX não encontrado: {path}")
        return pd.DataFrame(), {}
    xl = pd.ExcelFile(path)
    sheet = (
        "Clientes_2026_03_13"
        if "Clientes_2026_03_13" in xl.sheet_names
        else xl.sheet_names[0]
    )
    df = pd.read_excel(path, sheet_name=sheet)
    df = _normalize_xlsx_columns(df)
    if "id_orcamento" not in df.columns:
        logger.error("[BILLINGS] Coluna id_orcamento ausente no XLSX.")
        return df, {}
    by_id: Dict[int, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        try:
            oid = int(row["id_orcamento"])
        except Exception:
            continue
        if "orcamento_ativo" in df.columns:
            if not _safe_bool(row.get("orcamento_ativo"), True):
                continue
        if oid not in by_id:
            by_id[oid] = row.to_dict()
    return df, by_id


def _clean_billings_before_insert(
    migration: Any,
    schema_fin: str,
    id_list: List[int],
    view_rows: Dict[int, Tuple[Any, Any]],
) -> None:
    """
    Evita linhas órfãs: alinha ao padrão de contracts (clean_table).
    - clear_data ou sem filtros: TRUNCATE schema_fin.billings CASCADE
    - filtros sem clear_data: DELETE billings dos customers do escopo (via IdCliente na view)
    """
    if not id_list:
        return
    clear_data = bool(getattr(migration, "clear_data", False))
    has_filters = bool(migration.has_filters()) if hasattr(migration, "has_filters") else False

    if clear_data or not has_filters:
        migration.truncate_table("billings", schema_fin)
        return

    cust_uuids: List[str] = []
    seen: Set[str] = set()
    for oid in id_list:
        if oid not in view_rows:
            continue
        id_cli, _ = view_rows[oid]
        cu = migration.customer_id_map.get(id_cli)
        if not cu:
            continue
        s = str(cu)
        if s not in seen:
            seen.add(s)
            cust_uuids.append(s)
    if not cust_uuids:
        logger.warning("[BILLINGS] Escopo sem customer UUID para DELETE; limpeza por escopo ignorada.")
        return

    conn = DatabaseConnection.get_postgresql_destino_connection()
    cur = conn.cursor()
    placeholders = ",".join(["%s"] * len(cust_uuids))
    cur.execute(
        f"DELETE FROM {schema_fin}.billings WHERE customer_id IN ({placeholders})",
        cust_uuids,
    )
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    logger.info(
        f"[BILLINGS] DELETE escopo (filtros): {deleted} linha(s), {len(cust_uuids)} customer(s)"
    )
    print(f"[BILLINGS] DELETE escopo: {deleted} billings removidos ({len(cust_uuids)} customers)")


def migrate_billings_for_contracts(migration: Any) -> int:
    """
    Insere billings para o escopo (interseção JSON ∩ XLSX só orçamentos ativos) e preenche migration.contract_billing_map.

    Returns:
        Número de linhas inseridas em financial.billings (uma por customer distinto no escopo).
    """
    destino = DatabaseConnection.get_destino()
    schema_fin = _schema_financial()
    schema_core = _schema_gmcore()

    filter_data = migration.load_filter_json()
    id_from_json: List[int] = []
    if filter_data and "aggregated_ids" in filter_data:
        id_from_json = list(filter_data["aggregated_ids"].get("IdOrcamento", []) or [])
    if migration.id_orcamento_filter:
        id_from_json = list(migration.id_orcamento_filter)

    xlsx_path = os.environ.get("BILLING_CONTRATOS_XLSX", XLSX_DEFAULT)
    _, xlsx_by_orch = _load_xlsx_index(xlsx_path)
    if not xlsx_by_orch:
        logger.warning("[BILLINGS] Sem dados XLSX; interseção vazia.")
        return 0

    xlsx_ids: Set[int] = set(xlsx_by_orch.keys())
    scope: Set[int] = set(id_from_json) & xlsx_ids if id_from_json else set()
    if not scope and id_from_json:
        logger.info(
            f"[BILLINGS] Interseção vazia (JSON={len(id_from_json)} vs XLSX ativos={len(xlsx_ids)})."
        )
        return 0
    if not id_from_json:
        # Sem filtro explícito no JSON: usar somente ids presentes no XLSX que existem em contracts migrados
        scope = xlsx_ids & set(migration.contract_id_map.keys())
        if not scope:
            logger.warning("[BILLINGS] Nenhum IdOrcamento comum entre XLSX e contracts migrados.")
            return 0

    id_list = sorted(scope)
    placeholders = ",".join(["?"] * len(id_list))

    conn_sql = DatabaseConnection.get_sql_server_prd_connection()
    cur = conn_sql.cursor()
    cur.execute(
        f"""
        SELECT v.IdOrcamento, MIN(v.IdCliente) AS IdCliente, MAX(v.NomeCliente) AS NomeCliente
        FROM ViewOrcamentosLojas v
        WHERE v.IdOrcamento IN ({placeholders})
        GROUP BY v.IdOrcamento
        """,
        id_list,
    )
    view_rows = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    cur.close()
    conn_sql.close()

    # Um billing por customer_uuid; vários IdOrcamento podem compartilhar cliente
    customer_to_billing_row: Dict[str, Tuple[Dict[str, Any], Any, Any, Any]] = {}
    for oid in id_list:
        if oid not in view_rows:
            continue
        id_cli, nome_v = view_rows[oid]
        xlsx_row = xlsx_by_orch.get(oid, {})
        cust_uuid = migration.customer_id_map.get(id_cli)
        if not cust_uuid:
            logger.warning(
                f"[BILLINGS] Sem UUID para IdCliente={id_cli} (IdOrcamento={oid}); ignorado."
            )
            continue
        key = str(cust_uuid)
        if key not in customer_to_billing_row:
            customer_to_billing_row[key] = (xlsx_row, id_cli, nome_v, cust_uuid)

    if not customer_to_billing_row:
        return 0

    _clean_billings_before_insert(migration, schema_fin, id_list, view_rows)

    conn_pg = DatabaseConnection.get_postgresql_destino_connection()
    cursor_pg = conn_pg.cursor()

    # Nome fallback: legal_name / trade_name
    def customer_display_name(uid: str) -> str:
        cursor_pg.execute(
            f"""
            SELECT COALESCE(NULLIF(TRIM(trade_name),''), NULLIF(TRIM(legal_name),''), 'Customer')
            FROM {schema_core}.customers WHERE id = %s::uuid
            """,
            (uid,),
        )
        r = cursor_pg.fetchone()
        return r[0] if r and r[0] else "Customer"

    inserted = 0
    billing_by_customer: Dict[str, str] = {}

    for cust_key, (xlsx_row, _id_cli, nome_v, cust_uuid) in customer_to_billing_row.items():
        cursor_pg.execute(
            f"SELECT id FROM {schema_fin}.billings WHERE customer_id = %s::uuid LIMIT 1",
            (cust_key,),
        )
        ex = cursor_pg.fetchone()
        if ex:
            billing_by_customer[cust_key] = str(ex[0])
            continue

        nome_x = xlsx_row.get("nome_cliente")
        name = nome_v or nome_x or customer_display_name(cust_key)
        if isinstance(name, str):
            name = name.strip()[:500]
        else:
            name = str(name)[:500]

        calc = _map_calculation_type(xlsx_row.get("tipo_faturamento_descricao"))
        day_b = _safe_int(xlsx_row.get("dia_faturamento"), 1)
        day_d = _safe_int(xlsx_row.get("dia_vencimento"), 1)
        is_act = _safe_bool(xlsx_row.get("orcamento_ativo"), True)
        now = datetime.now()

        cursor_pg.execute(
            f"""
            INSERT INTO {schema_fin}.billings (
                id, customer_id, name, observations, deleted_at, is_active,
                created_at, updated_at, calculation_type,
                contract_parameters_billing_day, contract_parameters_billing_type,
                contract_parameters_due_day, contract_parameters_thirteenth_salary_type
            ) VALUES (
                gen_random_uuid(), %s::uuid, %s, NULL, NULL, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING id
            """,
            (
                cust_key,
                name,
                is_act,
                now,
                now,
                calc,
                day_b,
                "current",
                day_d,
                "monthly",
            ),
        )
        bid = str(cursor_pg.fetchone()[0])
        billing_by_customer[cust_key] = bid
        inserted += 1

    # contract_billing_map: todos os IdOrcamento no escopo ligados ao billing do cliente
    migration.contract_billing_map.clear()
    for oid in id_list:
        if oid not in view_rows:
            continue
        id_cli, _ = view_rows[oid]
        cu = migration.customer_id_map.get(id_cli)
        if not cu:
            continue
        ckey = str(cu)
        bid = billing_by_customer.get(ckey)
        if not bid:
            continue
        c_uuid = migration.contract_id_map.get(oid)
        if c_uuid:
            migration.contract_billing_map[str(c_uuid)] = bid

    conn_pg.commit()
    cursor_pg.close()
    conn_pg.close()

    mapped = len(migration.contract_billing_map)
    print(
        f"\n[BILLINGS] Migração: {inserted} billings novos no INSERT; "
        f"contract_billing_map={mapped} (schema {schema_fin}, destino {destino})"
    )
    logger.info(f"[BILLINGS] inseridos={inserted}, map={mapped}")
    return mapped


__all__ = ["migrate_billings_for_contracts", "XLSX_DEFAULT"]
