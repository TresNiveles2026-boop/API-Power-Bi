"""
Schema Enrichment Service — Añade descripciones y sinónimos a columnas.
"""

from __future__ import annotations

import logging
from typing import Any

from app.ai.gemini_client import call_gemini
from app.ai.models import SemanticTableProfile
from app.models.schemas import ColumnSchema

logger = logging.getLogger(__name__)

CANONICAL_DEFAULT_AGG = {"SUM", "AVERAGE", "COUNT", "MIN", "MAX"}
MAX_COLUMNS_PER_BATCH = 60


def _group_columns_by_table(columns: list[ColumnSchema]) -> dict[str, list[ColumnSchema]]:
    grouped: dict[str, list[ColumnSchema]] = {}
    for col in columns:
        grouped.setdefault(col.table_name, []).append(col)
    return grouped


async def _enrich_table(table_name: str, cols: list[ColumnSchema]) -> SemanticTableProfile | None:
    system_prompt = (
        "Eres un Arquitecto de Datos. Responde SOLO JSON válido.\n"
        "No inventes columnas nuevas.\n"
        "Para cada columna entrega: name, description, synonyms (exactamente 3), "
        "default_aggregation (SUM|AVERAGE|COUNT|MIN|MAX|null).\n"
        "Si la columna es texto, default_aggregation debe ser null o COUNT."
    )

    columns_payload = [
        {
            "name": c.column_name,
            "data_type": c.data_type,
            "is_measure": c.is_measure,
        }
        for c in cols
    ]

    user_message = (
        f"table_name: {table_name}\n"
        f"columns: {columns_payload}\n\n"
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

    for col in profile.columns:
        if col.default_aggregation is None:
            continue
        agg = str(col.default_aggregation).upper().strip()
        col.default_aggregation = agg if agg in CANONICAL_DEFAULT_AGG else None
        if len(col.synonyms) > 3:
            col.synonyms = col.synonyms[:3]

    return profile


async def enrich_columns_from_schema(columns: list[ColumnSchema]) -> list[ColumnSchema]:
    if not columns:
        return columns

    grouped = _group_columns_by_table(columns)
    enriched_map: dict[tuple[str, str], dict[str, Any]] = {}

    for table_name, table_cols in grouped.items():
        batches = [
            table_cols[i: i + MAX_COLUMNS_PER_BATCH]
            for i in range(0, len(table_cols), MAX_COLUMNS_PER_BATCH)
        ]
        for batch in batches:
            try:
                profile = await _enrich_table(table_name, batch)
            except Exception as exc:
                logger.warning("⚠️ Enrichment falló para %s: %s", table_name, exc)
                profile = None

            if not profile:
                continue

            for col in profile.columns:
                enriched_map[(table_name, col.name)] = {
                    "description": col.description,
                    "synonyms": col.synonyms,
                    "default_aggregation": col.default_aggregation,
                }

    if not enriched_map:
        return columns

    enriched_columns: list[ColumnSchema] = []
    for col in columns:
        key = (col.table_name, col.column_name)
        enriched = enriched_map.get(key)
        if not enriched:
            enriched_columns.append(col)
            continue

        metadata = dict(col.metadata or {})
        metadata.update(
            {
                "semantic_synonyms": enriched["synonyms"],
                "default_aggregation": enriched["default_aggregation"],
                "source": metadata.get("source", "powerbi"),
            }
        )
        enriched_columns.append(
            ColumnSchema(
                table_name=col.table_name,
                column_name=col.column_name,
                data_type=col.data_type,
                description=enriched["description"],
                is_measure=col.is_measure,
                dax_expression=col.dax_expression,
                sample_values=col.sample_values,
                metadata=metadata,
            )
        )

    return enriched_columns
