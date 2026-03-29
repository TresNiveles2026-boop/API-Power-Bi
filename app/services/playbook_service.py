"""
Playbook Service — Recomendaciones deterministas basadas en el schema.

WHY: El LLM no debe "inventar" recomendaciones costosas; este servicio genera
playbooks reproducibles (reglas) y el frontend puede ejecutarlos como acciones.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.ai.models import VisualAction
from app.services.semantic_service import get_semantic_dictionary


@dataclass(frozen=True)
class Playbook:
    id: str
    title: str
    description: str
    action: VisualAction

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "action": self.action.model_dump(),
        }


def _is_date_dtype(dt: str) -> bool:
    d = (dt or "").lower()
    return any(k in d for k in ("date", "datetime", "time"))


def _is_numeric_dtype(dt: str) -> bool:
    d = (dt or "").lower()
    return any(k in d for k in ("int", "decimal", "double", "float", "number", "numeric", "currency"))


def _pick_first(
    columns: list[dict[str, str]],
    *,
    prefer_name_contains: tuple[str, ...] = (),
    predicate=None,
) -> dict[str, str] | None:
    if not columns:
        return None
    if prefer_name_contains:
        for c in columns:
            name = (c.get("column") or "").lower()
            if any(k in name for k in prefer_name_contains):
                if predicate is None or predicate(c):
                    return c
    for c in columns:
        if predicate is None or predicate(c):
            return c
    return None


def _flatten_dictionary(dictionary: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if not dictionary or not getattr(dictionary, "tables", None):
        return out
    for table_name, cols in dictionary.tables.items():
        for col in cols:
            out.append(
                {
                    "table": table_name,
                    "column": col.column_name,
                    "data_type": col.data_type or "",
                    "is_measure": "true" if (col.is_measure or False) else "false",
                }
            )
    return out


async def generate_playbooks(report_id: str, tenant_id: str) -> list[Playbook]:
    """
    Genera una lista pequeña de playbooks (determinista).

    NOTE: Esto no ejecuta nada; solo propone acciones CREATE seguras.
    """
    dictionary = await get_semantic_dictionary(report_id=report_id, tenant_id=tenant_id)
    if dictionary is None:
        return []

    cols = _flatten_dictionary(dictionary)
    date_col = _pick_first(cols, prefer_name_contains=("fecha", "period", "mes", "ano", "year", "date"), predicate=lambda c: _is_date_dtype(c["data_type"]) or "fecha" in c["column"].lower())
    metric_col = _pick_first(cols, prefer_name_contains=("stock", "venta", "ventas", "monto", "cantidad", "importe"), predicate=lambda c: _is_numeric_dtype(c["data_type"]) or any(k in c["column"].lower() for k in ("stock", "monto", "cantidad", "ventas", "importe")))
    cat_col = _pick_first(cols, prefer_name_contains=("tipo", "categoria", "almacen", "regi", "producto", "material"), predicate=lambda c: not _is_numeric_dtype(c["data_type"]) and not _is_date_dtype(c["data_type"]))

    playbooks: list[Playbook] = []

    if metric_col and cat_col:
        action = VisualAction(
            operation="CREATE",
            visualType="barChart",
            title=f"{metric_col['column']} por {cat_col['column']}",
            layout_intent="chart_full",
            dataRoles={
                "Category": {"table": cat_col["table"], "column": cat_col["column"]},
                "Y": {"table": metric_col["table"], "column": metric_col["column"], "aggregation": "Sum"},
            },
            explanation="Comparación por categoría (barras).",
        )
        playbooks.append(
            Playbook(
                id="bar_by_category",
                title="Barras por categoría",
                description=f"Suma de {metric_col['column']} por {cat_col['column']}.",
                action=action,
            )
        )

    if metric_col and date_col:
        action = VisualAction(
            operation="CREATE",
            visualType="lineChart",
            title=f"Tendencia de {metric_col['column']}",
            layout_intent="chart_full",
            dataRoles={
                "Category": {"table": date_col["table"], "column": date_col["column"]},
                "Y": {"table": metric_col["table"], "column": metric_col["column"], "aggregation": "Sum"},
            },
            explanation="Tendencia en el tiempo (línea).",
        )
        playbooks.append(
            Playbook(
                id="trend_over_time",
                title="Tendencia en el tiempo",
                description=f"Evolución de {metric_col['column']} por {date_col['column']}.",
                action=action,
            )
        )

    if metric_col and cat_col:
        action = VisualAction(
            operation="CREATE",
            visualType="donutChart",
            title=f"Participación de {cat_col['column']}",
            layout_intent="chart_half",
            dataRoles={
                "Category": {"table": cat_col["table"], "column": cat_col["column"]},
                "Y": {"table": metric_col["table"], "column": metric_col["column"], "aggregation": "Sum"},
            },
            explanation="Participación (donut).",
        )
        playbooks.append(
            Playbook(
                id="share_donut",
                title="Participación (donut)",
                description=f"Participación de {metric_col['column']} por {cat_col['column']}.",
                action=action,
            )
        )

    # KPI básico (suma total)
    if metric_col:
        action = VisualAction(
            operation="CREATE",
            visualType="card",
            title=f"Total {metric_col['column']}",
            layout_intent="kpi_top",
            dataRoles={
                "Values": {"table": metric_col["table"], "column": metric_col["column"], "aggregation": "Sum"},
            },
            explanation="KPI total (tarjeta).",
        )
        playbooks.append(
            Playbook(
                id="kpi_total",
                title="KPI total",
                description=f"Suma total de {metric_col['column']}.",
                action=action,
            )
        )

    return playbooks[:8]

