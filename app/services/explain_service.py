"""
Explain Service — Pipeline híbrido Pandas + Gemini con Zero-Data-Retention.

WHY: Python calcula los hechos matemáticos de forma determinista y el LLM
solo redacta narrativa ejecutiva. La data cruda vive en memoria el mínimo
tiempo posible y se libera explícitamente al finalizar.
"""

from __future__ import annotations

import gc
import json
from typing import Any

import pandas as pd

from app.ai.gemini_client import call_gemini
from app.ai.models import ExplainRequest
from app.ai.prompts import EXPLAIN_PROMPT


def _to_float(value: Any) -> float | None:
    """Convierte valores numéricos de Pandas a float serializable."""
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _build_statistical_summary(df: pd.DataFrame, visual_title: str) -> str:
    """
    Construye un resumen estadístico estricto a partir del DataFrame.

    WHY: El LLM no debe ver data cruda; solo hechos resumidos y auditables.
    """
    numeric_df = df.apply(pd.to_numeric, errors="coerce")
    numeric_columns = [str(col) for col in numeric_df.columns if numeric_df[col].notna().any()]
    categorical_columns = [
        str(col)
        for col in df.columns
        if str(col) not in numeric_columns
    ]

    summary: dict[str, Any] = {
        "visual_title": visual_title,
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "numeric_columns": numeric_columns,
        "categorical_columns": categorical_columns,
        "metrics": {},
        "ranking": {},
    }

    for column in numeric_columns:
        series = numeric_df[column].dropna()
        if series.empty:
            continue
        summary["metrics"][column] = {
            "total": _to_float(series.sum()),
            "average": _to_float(series.mean()),
            "max": _to_float(series.max()),
            "min": _to_float(series.min()),
        }

    if numeric_columns and categorical_columns:
        metric_col = numeric_columns[0]
        category_col = categorical_columns[0]
        grouped = (
            pd.DataFrame({
                "category": df[category_col].astype("string"),
                "metric": numeric_df[metric_col],
            })
            .dropna(subset=["metric"])
            .groupby("category", dropna=False)["metric"]
            .sum()
            .sort_values(ascending=False)
        )
        if not grouped.empty:
            top_key = grouped.index[0]
            bottom_key = grouped.index[-1]
            summary["ranking"] = {
                "category_column": category_col,
                "metric_column": metric_col,
                "top_1": {
                    "category": None if pd.isna(top_key) else str(top_key),
                    "value": _to_float(grouped.iloc[0]),
                },
                "bottom_1": {
                    "category": None if pd.isna(bottom_key) else str(bottom_key),
                    "value": _to_float(grouped.iloc[-1]),
                },
            }

    return "[RESUMEN ESTADISTICO]\n" + json.dumps(summary, ensure_ascii=False)


async def generate_data_insight(raw_data: list[dict[str, Any]], visual_title: str) -> str:
    """
    Genera insight ejecutivo en memoria y libera la data al finalizar.
    """
    df: pd.DataFrame | None = None
    try:
        if not raw_data:
            return "No hay datos disponibles para explicar el visual."

        df = pd.DataFrame(raw_data)
        if df.empty:
            return "No hay datos disponibles para explicar el visual."

        stats_summary = _build_statistical_summary(df, visual_title)
        result = await call_gemini(
            system_prompt=EXPLAIN_PROMPT,
            user_message=stats_summary,
            temperature=0.1,
        )

        explanation = result.get("explanation") if isinstance(result, dict) else None
        if isinstance(explanation, str) and explanation.strip():
            return explanation.strip()

        if isinstance(result, dict):
            raw = result.get("raw")
            if isinstance(raw, str) and raw.strip():
                return raw.strip()

        return "No fue posible generar un insight analítico en este momento."
    finally:
        if df is not None:
            del df
        del raw_data
        gc.collect()


async def generate_visual_explanation(request: ExplainRequest) -> str:
    """
    Wrapper retrocompatible para el endpoint /api/v1/explain.
    """
    return await generate_data_insight(
        raw_data=list(request.raw_data or []),
        visual_title=str(request.visual_title or request.visual_name or "Visual sin título"),
    )
