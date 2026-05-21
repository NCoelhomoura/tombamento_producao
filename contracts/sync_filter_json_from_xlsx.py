#!/usr/bin/env python3
"""
Atualiza contracts_filter_main.json com a lista de IdOrcamento extraída do
contratos_ativos.xlsx (linhas com migrar=true).

Uso (na raiz app_migracao_core):
  python contracts/sync_filter_json_from_xlsx.py
  python contracts/sync_filter_json_from_xlsx.py --xlsx caminho/alternativo.xlsx
  python contracts/sync_filter_json_from_xlsx.py --no-require-ativo

Variável de ambiente CONTRATOS_ATIVOS_FILE sobrescreve o XLSX padrão (mesma regra do billing/contracts).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pandas as pd

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from billing.billings_to_core import (  # noqa: E402
    XLSX_DEFAULT,
    _normalize_xlsx_columns,
    _resolve_id_orcamento_xlsx_column,
    _safe_bool,
)

DEFAULT_FILTER_JSON = os.path.join(os.path.dirname(__file__), "contracts_filter_main.json")


def _default_xlsx_path() -> str:
    return os.environ.get("CONTRATOS_ATIVOS_FILE", XLSX_DEFAULT)


def _load_xlsx_dataframe(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Arquivo XLSX não encontrado: {path}")
    xl = pd.ExcelFile(path)
    sheet = (
        "Clientes_2026_03_13"
        if "Clientes_2026_03_13" in xl.sheet_names
        else xl.sheet_names[0]
    )
    df = pd.read_excel(path, sheet_name=sheet)
    return _normalize_xlsx_columns(df)


def _row_passes_ativo(df: pd.DataFrame, row, require_ativo: bool) -> bool:
    if not require_ativo:
        return True
    if "orcamento_ativo" in df.columns:
        return _safe_bool(row.get("orcamento_ativo"), True)
    if "ativo?" in df.columns:
        return _safe_bool(row.get("ativo?"), True)
    return True


def collect_id_orcamentos_migrar(
    path_xlsx: str,
    require_ativo: bool,
) -> List[int]:
    df = _load_xlsx_dataframe(path_xlsx)
    id_col = _resolve_id_orcamento_xlsx_column(df)
    if id_col is None:
        raise ValueError(
            "Coluna de IdOrcamento ausente no XLSX "
            "(use id_orcamento, id_orçamento ou ORÇAMENTO)."
        )
    if "migrar" not in df.columns:
        raise ValueError(
            "Coluna 'migrar' ausente no XLSX após normalização de cabeçalhos."
        )

    out: Set[int] = set()
    for _, row in df.iterrows():
        if not _safe_bool(row.get("migrar"), False):
            continue
        if not _row_passes_ativo(df, row, require_ativo):
            continue
        try:
            oid = int(row[id_col])
        except (TypeError, ValueError):
            continue
        out.add(oid)
    return sorted(out)


def merge_into_filter_json(
    path_json: str,
    id_orcamentos: List[int],
    source_xlsx: str,
    require_ativo: bool,
    filters_applied_updates: Optional[Dict[str, Any]] = None,
    reset_dependent_aggregated_ids: bool = False,
    sync_source_label: str = "sync_filter_json_from_xlsx.py",
) -> None:
    data = {}
    if os.path.isfile(path_json):
        with open(path_json, "r", encoding="utf-8") as f:
            data = json.load(f)

    if not isinstance(data, dict):
        data = {}

    fa = data.get("filters_applied")
    if not isinstance(fa, dict):
        fa = {}
    if filters_applied_updates:
        fa.update(filters_applied_updates)
    fa["id_orcamento"] = list(id_orcamentos)
    data["filters_applied"] = fa

    agg = data.get("aggregated_ids")
    if not isinstance(agg, dict):
        agg = {}
    agg["IdOrcamento"] = list(id_orcamentos)
    if reset_dependent_aggregated_ids:
        agg["IdCliente"] = []
        agg["IdSegmentoProduto"] = []
        agg["IdEstabelecimento"] = []
        agg["IdCanalEstabelecimento"] = []
        agg["IdBandeira"] = []
        agg["IdRede"] = []
    data["aggregated_ids"] = agg

    ex = data.get("execution_info")
    if not isinstance(ex, dict):
        ex = {}
    ex["timestamp"] = datetime.now().isoformat()
    ex["sync_from_xlsx"] = os.path.abspath(source_xlsx)
    ex["sync_note"] = (
        f"IdOrcamento atualizado por {sync_source_label} "
        f"(migrar=true; require_ativo={require_ativo}). "
        + (
            "aggregated_ids dependentes (IdCliente, …) zerados; a ETAPA 1 de contracts regenera o escopo completo."
            if reset_dependent_aggregated_ids
            else "Demais chaves em aggregated_ids foram preservadas; execute o preview / ETAPA 1 de contracts para regenerar o escopo completo se precisar."
        )
    )
    data["execution_info"] = ex

    with open(path_json, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Grava IdOrcamento (migrar=true no XLSX) em contracts_filter_main.json."
    )
    parser.add_argument(
        "--xlsx",
        default=None,
        help=f"Caminho do contratos_ativos.xlsx (padrão: env CONTRATOS_ATIVOS_FILE ou {XLSX_DEFAULT})",
    )
    parser.add_argument(
        "--json",
        default=DEFAULT_FILTER_JSON,
        help=f"Caminho do contracts_filter_main.json (padrão: {DEFAULT_FILTER_JSON})",
    )
    parser.add_argument(
        "--no-require-ativo",
        action="store_true",
        help="Incluir linhas com migrar=true mesmo se ATIVO?/orcamento_ativo for false.",
    )
    args = parser.parse_args()

    xlsx_path = os.path.abspath(args.xlsx or _default_xlsx_path())
    json_path = os.path.abspath(args.json)
    require_ativo = not args.no_require_ativo

    ids = collect_id_orcamentos_migrar(xlsx_path, require_ativo=require_ativo)
    merge_into_filter_json(json_path, ids, xlsx_path, require_ativo)

    print(f"XLSX: {xlsx_path}")
    print(f"JSON: {json_path}")
    print(f"IdOrcamento com migrar=true ({len(ids)}): {ids[:30]}{'...' if len(ids) > 30 else ''}")


if __name__ == "__main__":
    main()
