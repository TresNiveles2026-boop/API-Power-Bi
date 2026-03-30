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


def _is_measure(col: dict[str, str]) -> bool:
    return str(col.get("is_measure", "") or "").strip().lower() in {"true", "1", "yes"}


def _looks_like_date_name(name: str) -> bool:
    n = (name or "").strip().lower()
    return any(k in n for k in ("fecha", "date", "periodo", "mes", "año", "ano", "year", "yyyy", "yy"))


def _looks_like_metric_name(name: str) -> bool:
    n = (name or "").strip().lower()
    return any(k in n for k in ("stock", "venta", "ventas", "monto", "cantidad", "importe", "total", "saldo", "valor"))


def _is_date_col(col: dict[str, str]) -> bool:
    return _is_date_dtype(col.get("data_type", "") or "") or _looks_like_date_name(col.get("column", "") or "")


def _looks_numeric_by_samples(samples: list[Any]) -> bool:
    """
    Inferencia defensiva de "columna numérica" a partir de sample_values.

    WHY: En algunos tenants el schema llega con data_type vacío o como texto,
    pero los valores de ejemplo sí son numéricos.
    """
    if not samples:
        return False

    ok = 0
    total = 0
    for v in samples[:8]:
        if v is None:
            continue
        total += 1
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            ok += 1
            continue
        if isinstance(v, str):
            s = v.strip()
            if not s:
                continue
            # Normalizar separadores comunes.
            s2 = s.replace(",", ".")
            try:
                float(s2)
                ok += 1
            except Exception:
                pass

    # Si al menos ~60% de los samples parsean a número, lo tratamos como numérico.
    return total > 0 and (ok / total) >= 0.6


def _is_numeric_col(col: dict[str, str]) -> bool:
    """
    Determina si una columna puede usarse como **métrica** (Y/Values).

    Reglas:
    - Nunca usar fechas como métrica (aunque contengan "stock" en el nombre, ej. "Fecha de stock").
    - Preferir dtype numérico o medidas.
    - Usar heurística por nombre SOLO si el dtype está ausente/desconocido.
    """
    if _is_date_col(col):
        return False

    dt = (col.get("data_type", "") or "").strip().lower()
    name = col.get("column", "") or ""
    samples = col.get("sample_values") or []

    if _is_numeric_dtype(dt):
        return True
    if _is_measure(col):
        return True
    if isinstance(samples, list) and _looks_numeric_by_samples(samples):
        return True

    # Solo si no tenemos tipo confiable, permitimos heurística por nombre.
    if dt in {"", "unknown", "any", "variant"}:
        return _looks_like_metric_name(name)
    return False


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


def _flatten_dictionary(dictionary: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
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
                    "sample_values": list(col.sample_values or []),
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
    date_col = _pick_first(
        cols,
        prefer_name_contains=("fecha", "period", "mes", "ano", "year", "date"),
        predicate=lambda c: _is_date_col(c),
    )
    # Métrica: estricta (numérica/medida). Evita fechas que parezcan métricas por nombre.
    metric_col = _pick_first(
        cols,
        prefer_name_contains=("stock", "venta", "ventas", "monto", "cantidad", "importe"),
        predicate=lambda c: _is_numeric_col(c),
    )
    # Defensa adicional: si por alguna razón el heurístico se equivoca, nunca aceptar una fecha como métrica.
    if metric_col and _is_date_col(metric_col):
        metric_col = None
    if metric_col is None:
        # Fallback ultra-seguro: solo dtype numérico o medidas (sin heurística por nombre).
        metric_col = _pick_first(
            cols,
            predicate=lambda c: (not _is_date_col(c))
            and (_is_measure(c) or _is_numeric_dtype((c.get("data_type", "") or "").lower())),
        )
    cat_col = _pick_first(
        cols,
        prefer_name_contains=("tipo", "categoria", "almacen", "regi", "producto", "material"),
        predicate=lambda c: (not _is_numeric_col(c)) and (not _is_date_col(c)) and (not _is_measure(c)),
    )

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
