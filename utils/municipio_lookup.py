"""
Lookup de CodigoMunicipio (IBGE) na origem SQL Server via tabela Municipio.

Chave lógica: UF + NomeMunicipio (normalizado) = UF + Cidade do cadastro (normalizado),
equivalente ao LEFT JOIN com normalizar_caracteres(UPPER(...)) em ambos os lados.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# (UF normalizada, NomeMunicipio normalizado) -> CodigoMunicipio (int)
MunicipioLookup = Dict[Tuple[str, str], int]


def normalizar_caracteres(value: Optional[Any]) -> str:
    """
    Remove acentos, converte para maiúsculas, remove caracteres especiais,
    colapsa espaços — alinhado ao uso em JOIN com Municipio.NomeMunicipio e Cidade.
    """
    if value is None:
        return ""
    s = str(value).strip().upper()
    if not s:
        return ""
    normalized = unicodedata.normalize("NFD", s)
    without_marks = "".join(c for c in normalized if unicodedata.category(c) != "Mn")
    # Mantém apenas letras, dígitos e espaço (remove pontuação e símbolos)
    without_marks = re.sub(r"[^A-Z0-9 ]+", "", without_marks)
    without_marks = re.sub(r"\s+", " ", without_marks).strip()
    return without_marks


def parse_codigo_municipio_int(value: Optional[Any]) -> Optional[int]:
    """Extrai dígitos de CodigoMunicipio na origem; None se vazio ou inválido."""
    if value is None or value == "":
        return None
    try:
        codigo_str = re.sub(r"[^\d]", "", str(value))
        if not codigo_str:
            return None
        return int(codigo_str[:10])
    except (ValueError, TypeError):
        return None


def load_municipio_lookup(cursor) -> MunicipioLookup:
    """
    Carrega dbo.Municipio: (UF, NomeMunicipio) normalizados -> CodigoMunicipio.
    Em duplicidade de chave, mantém a primeira ocorrência.
    """
    lookup: MunicipioLookup = {}
    cursor.execute(
        """
        SELECT CodigoMunicipio, NomeMunicipio, UF
        FROM dbo.Municipio
        """
    )
    rows = cursor.fetchall()
    for row in rows:
        codigo_raw, nome, uf = row[0], row[1], row[2]
        codigo = parse_codigo_municipio_int(codigo_raw)
        if codigo is None or codigo <= 0:
            continue
        k = (normalizar_caracteres(uf), normalizar_caracteres(nome))
        if not k[0] or not k[1]:
            continue
        if k not in lookup:
            lookup[k] = codigo
    logger.info("Municipio: %s chaves de lookup carregadas", len(lookup))
    return lookup


def municipal_code_from_origem(
    codigo_origem: Optional[Any],
    uf: Optional[Any],
    cidade_nome: Optional[Any],
    *,
    municipio_lookup: MunicipioLookup,
) -> int:
    """
    COALESCE(CodigoMunicipio explícito na linha, match em Municipio por UF + cidade).
    Retorna int >= 0 (0 = desconhecido / sem match).
    """
    c = parse_codigo_municipio_int(codigo_origem)
    if c is not None and c > 0:
        return c
    k = (normalizar_caracteres(uf), normalizar_caracteres(cidade_nome))
    if not k[0] or not k[1]:
        return 0
    return int(municipio_lookup.get(k, 0))


def city_code_legacy_str(
    codigo_origem: Optional[Any],
    uf: Optional[Any],
    cidade_nome: Optional[Any],
    *,
    municipio_lookup: MunicipioLookup,
) -> Optional[str]:
    """Para core.address (legado): city_code como string ou None se não houver código."""
    c = municipal_code_from_origem(
        codigo_origem, uf, cidade_nome, municipio_lookup=municipio_lookup
    )
    if c and c > 0:
        return str(c)[:10]
    return None
