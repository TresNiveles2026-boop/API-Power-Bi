"""
Data Discovery Service — Perfilado determinista + enriquecimiento semántico.

WHY:
- Pandas calcula la verdad matemática del dataset (tipos, unicidad, distribución).
- Gemini solo enriquece semánticamente sobre estadísticas reales, sin adivinanza libre.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import pandas as pd

from app.ai.gemini_client import call_gemini
from app.ai.models import SemanticTableProfile
from app.db.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)

CANONICAL_DEFAULT_AGG = {"SUM", "AVERAGE", "COUNT", "MIN", "MAX"}


def _deterministic_profiling(df: pd.DataFrame) -> dict[str, Any]:
    """
    Perfila columnas con reglas deterministas usando Pandas.
    """
    rows = len(df)
    columns: list[dict[str, Any]] = []

    for col in df.columns:
        series = df[col]
        non_null = int(series.notna().sum())
        null_count = int(series.isna().sum())
        unique_count = int(series.nunique(dropna=True))
        is_unique = bool(series.is_unique)

        if pd.api.types.is_numeric_dtype(series):
            semantic_type = "numeric"
        elif pd.api.types.is_datetime64_any_dtype(series):
            semantic_type = "datetime"
        elif pd.api.types.is_bool_dtype(series):
            semantic_type = "boolean"
        else:
            semantic_type = "text"

        primary_key_candidate = bool(
            semantic_type in {"numeric", "text"}
            and is_unique
            and non_null == rows
        )

        col_stats: dict[str, Any] = {
            "name": str(col),
            "dtype": str(series.dtype),
            "semantic_type": semantic_type,
            "rows": rows,
            "non_null_count": non_null,
            "null_count": null_count,
            "unique_count": unique_count,
            "is_unique": is_unique,
            "primary_key_candidate": primary_key_candidate,
            "sample_values": [
                str(v) for v in series.dropna().astype(str).head(5).tolist()
            ],
        }

        if semantic_type == "numeric" and non_null > 0:
            col_stats["min"] = float(series.min())
            col_stats["max"] = float(series.max())
            col_stats["mean"] = float(series.mean())
        elif semantic_type == "datetime" and non_null > 0:
            col_stats["min"] = str(series.min())
            col_stats["max"] = str(series.max())
        elif semantic_type == "text" and non_null > 0:
            lengths = series.dropna().astype(str).str.len()
            if not lengths.empty:
                col_stats["avg_length"] = float(lengths.mean())
                col_stats["max_length"] = int(lengths.max())

        columns.append(col_stats)

    return {
        "rows": rows,
        "columns_count": len(df.columns),
        "columns": columns,
    }


async def _semantic_enrichment(table_name: str, stats: dict[str, Any]) -> SemanticTableProfile:
    """
    Enriquecimiento semántico estructurado basado en estadísticas reales.
    """
    system_prompt = (
        "Eres un Arquitecto de Datos. Responde SOLO JSON válido.\n"
        "No inventes columnas que no existan.\n"
        "Para cada columna entrega: name, description, synonyms (exactamente 3), "
        "default_aggregation (SUM|AVERAGE|COUNT|MIN|MAX o null).\n"
        "Si la columna no es numérica, default_aggregation debe ser null o COUNT."
    )

    user_message = (
        f"table_name: {table_name}\n"
        f"estadisticas: {json.dumps(stats, ensure_ascii=False)}\n\n"
        "Devuelve este esquema exacto:\n"
        "{\n"
        '  "table_name": "string",\n'
        '  "columns": [\n'
        "    {\n"
        '      "name": "string",\n'
        '      "description": "string",\n'
        '      "synonyms": ["a","b","c"],\n'
        '      "default_aggregation": "SUM|AVERAGE|COUNT|MIN|MAX|null"\n'
        "    }\n"
        "  ]\n"
        "}\n"
    )

    result = await call_gemini(
        system_prompt=system_prompt,
        user_message=user_message,
        temperature=0.1,
    )
    clean_result = {k: v for k, v in result.items() if k != "_token_usage"}
    profile = SemanticTableProfile(**clean_result)

    # Normalización defensiva de agregaciones
    for col in profile.columns:
        if col.default_aggregation is None:
            continue
        agg = str(col.default_aggregation).upper().strip()
        col.default_aggregation = agg if agg in CANONICAL_DEFAULT_AGG else None
        if len(col.synonyms) > 3:
            col.synonyms = col.synonyms[:3]

    return profile


async def upsert_semantic_dictionary(
    tenant_id: str,
    report_id: str,
    profile: SemanticTableProfile,
) -> int:
    """
    Upsert de metadata semántica enriquecida en semantic_dictionaries.
    """
    client = get_supabase_client()
    records: list[dict[str, Any]] = []

    for col in profile.columns:
        existing = (
            client.table("semantic_dictionaries")
            .select("data_type,is_measure,dax_expression,sample_values,metadata")
            .eq("tenant_id", tenant_id)
            .eq("report_id", report_id)
            .eq("table_name", profile.table_name)
            .eq("column_name", col.name)
            .limit(1)
            .execute()
        )
        row = (existing.data or [{}])[0]
        prev_meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}

        metadata = {
            **prev_meta,
            "semantic_synonyms": col.synonyms,
            "default_aggregation": col.default_aggregation,
        }

        records.append(
            {
                "tenant_id": tenant_id,
                "report_id": report_id,
                "table_name": profile.table_name,
                "column_name": col.name,
                "data_type": row.get("data_type", "String"),
                "description": col.description,
                "is_measure": row.get("is_measure", False),
                "dax_expression": row.get("dax_expression", ""),
                "sample_values": row.get("sample_values", []),
                "metadata": metadata,
            }
        )

    if not records:
        return 0

    res = (
        client.table("semantic_dictionaries")
        .upsert(records, on_conflict="report_id,table_name,column_name")
        .execute()
    )

    updated = len(res.data) if res.data else len(records)
    logger.info(
        "🧭 Discovery upsert completado: report=%s table=%s columnas=%d",
        report_id,
        profile.table_name,
        updated,
    )
    return updated


async def profile_dataframe_and_upsert(
    tenant_id: str,
    report_id: str,
    table_name: str,
    df: pd.DataFrame,
) -> SemanticTableProfile:
    """
    Pipeline completo: perfilado determinista -> enriquecimiento -> upsert.
    """
    stats = _deterministic_profiling(df)
    profile = await _semantic_enrichment(table_name=table_name, stats=stats)
    await upsert_semantic_dictionary(tenant_id=tenant_id, report_id=report_id, profile=profile)
    return profile

