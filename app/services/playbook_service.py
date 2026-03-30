"""
Playbook Service — Recomendaciones deterministas basadas en el schema.

WHY: El LLM no debe "inventar" recomendaciones costosas; este servicio genera
playbooks reproducibles (reglas) y el frontend puede ejecutarlos como acciones.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.ai.models import KpiRequirements, VisualAction
from app.services.measure_template_service import get_measure_templates
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


def _looks_like_identifier_name(name: str) -> bool:
    """
    True si el campo parece un identificador/código (no una métrica).

    Ej: Lote, ID, Código, SKU, Nro, etc. Aunque sea numérico, no debe ir a Y/Values.
    """
    n = (name or "").strip().lower()
    return any(
        k in n
        for k in (
            "id",
            "codigo",
            "código",
            "code",
            "sku",
            "lote",
            "lot",
            "nro",
            "num",
            "numero",
            "número",
            "serial",
            "folio",
            "documento",
        )
    )


def _is_technical_table_name(table: str) -> bool:
    t = (table or "").strip()
    return t.startswith("DateTableTemplate") or t.startswith("LocalDateTable")


def _is_technical_column_name(column: str) -> bool:
    c = (column or "").strip()
    return c.startswith("__") or c.startswith("[__") or "DateTableTemplate" in c


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

    # Identificadores nunca son métrica, aunque parezcan numéricos.
    if _looks_like_identifier_name(name):
        return False

    if _is_numeric_dtype(dt):
        return True
    if _is_measure(col):
        return True
    if isinstance(samples, list) and _looks_numeric_by_samples(samples):
        return True

    # Heurística por nombre: permite rescatar métricas reales aunque el dtype sea débil (p.ej. llega como String).
    # _is_date_col ya bloquea "Fecha de stock".
    if _looks_like_metric_name(name):
        return True

    return False


def _pick_first(
    columns: list[dict[str, Any]],
    *,
    prefer_name_contains: tuple[str, ...] = (),
    predicate=None,
) -> dict[str, Any] | None:
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


def _render_template(template_id: str, vars: dict[str, str]) -> str:
    """
    Renderiza una plantilla del registry de medidas (determinista).
    """
    templates = {t.id: t for t in get_measure_templates()}
    tpl = templates.get(template_id)
    if tpl is None:
        return ""
    # El template usa llaves {var}. Validación liviana: si falta var, devuelve vacío.
    for var in (tpl.required_vars or []):
        if var not in vars or vars[var] is None:
            return ""
    return str(tpl.dax_template).format(**vars)


def _make_requirements(
    *,
    operation: str,
    measure_template_id: str,
    suggested_measure_name: str,
    table: str | None = None,
    column: str | None = None,
    dax_suggestion: str,
    format_hint: str | None = None,
) -> KpiRequirements:
    return KpiRequirements(
        needs_measure=True,
        operation=operation,  # type: ignore[arg-type]  # Literal en el modelo
        measure_template_id=measure_template_id,
        suggested_measure_name=suggested_measure_name,
        table=table,
        column=column,
        dax_suggestion=dax_suggestion,
        format_hint=format_hint,
    )


async def generate_playbooks(report_id: str, tenant_id: str) -> list[Playbook]:
    """
    Genera una lista pequeña de playbooks (determinista).

    NOTE: Esto no ejecuta nada; solo propone acciones CREATE seguras.
    """
    dictionary = await get_semantic_dictionary(report_id=report_id, tenant_id=tenant_id)
    if dictionary is None:
        return []

    cols = _flatten_dictionary(dictionary)
    # Filtrar tablas/columnas técnicas (Power BI autogenera DateTableTemplate/LocalDateTable).
    cols_user = [
        c
        for c in cols
        if (not _is_technical_table_name(c.get("table", "")))
        and (not _is_technical_column_name(c.get("column", "")))
    ]
    date_col = _pick_first(
        cols_user,
        prefer_name_contains=("fecha", "period", "mes", "ano", "year", "date"),
        predicate=lambda c: _is_date_col(c),
    )
    # Métrica: estricta (numérica/medida). Evita fechas que parezcan métricas por nombre.
    metric_col = _pick_first(
        cols_user,
        prefer_name_contains=("stock", "venta", "ventas", "monto", "cantidad", "importe"),
        predicate=lambda c: _is_numeric_col(c),
    )
    # Defensa adicional: si por alguna razón el heurístico se equivoca, nunca aceptar una fecha como métrica.
    if metric_col and _is_date_col(metric_col):
        metric_col = None
    if metric_col is None:
        # Fallback ultra-seguro: solo dtype numérico o medidas (sin heurística por nombre).
        metric_col = _pick_first(
            cols_user,
            predicate=lambda c: (not _is_date_col(c))
            and (_is_measure(c) or _is_numeric_dtype((c.get("data_type", "") or "").lower())),
        )
    cat_col = _pick_first(
        cols_user,
        prefer_name_contains=("tipo", "categoria", "almacen", "regi", "producto", "material"),
        predicate=lambda c: (not _is_numeric_col(c)) and (not _is_date_col(c)) and (not _is_measure(c)),
    )

    playbooks: list[Playbook] = []

    # 1) Visuales "sin medida": solo si tenemos métrica numérica confiable.
    if metric_col and cat_col:
        playbooks.append(
            Playbook(
                id="bar_by_category",
                title="Barras por categoría",
                description=f"Suma de {metric_col['column']} por {cat_col['column']}.",
                action=VisualAction(
                    operation="CREATE",
                    visualType="barChart",
                    title=f"{metric_col['column']} por {cat_col['column']}",
                    layout_intent="chart_full",
                    dataRoles={
                        "Category": {"table": cat_col["table"], "column": cat_col["column"]},
                        "Y": {"table": metric_col["table"], "column": metric_col["column"], "aggregation": "Sum"},
                    },
                    explanation="Comparación por categoría (barras).",
                ),
            )
        )

        # Top N (usa filtro nativo, no DAX)
        playbooks.append(
            Playbook(
                id="top10_by_category",
                title="Top 10 por categoría",
                description=f"Top 10 {cat_col['column']} por {metric_col['column']}.",
                action=VisualAction(
                    operation="CREATE",
                    visualType="barChart",
                    title=f"Top 10 {cat_col['column']} por {metric_col['column']}",
                    layout_intent="chart_full",
                    dataRoles={
                        "Category": {"table": cat_col["table"], "column": cat_col["column"]},
                        "Y": {"table": metric_col["table"], "column": metric_col["column"], "aggregation": "Sum"},
                    },
                    top_n={
                        "count": 10,
                        "order_by_column": metric_col["column"],
                        "order_by_table": metric_col["table"],
                        "category_column": cat_col["column"],
                        "category_table": cat_col["table"],
                        "direction": "Top",
                    },
                    explanation="Top 10 por categoría (filtro nativo TopN).",
                ),
            )
        )

        playbooks.append(
            Playbook(
                id="share_donut",
                title="Participación (donut)",
                description=f"Participación de {metric_col['column']} por {cat_col['column']}.",
                action=VisualAction(
                    operation="CREATE",
                    visualType="donutChart",
                    title=f"Participación de {cat_col['column']}",
                    layout_intent="chart_half",
                    dataRoles={
                        "Category": {"table": cat_col["table"], "column": cat_col["column"]},
                        "Y": {"table": metric_col["table"], "column": metric_col["column"], "aggregation": "Sum"},
                    },
                    explanation="Participación (donut).",
                ),
            )
        )

    if metric_col and date_col:
        playbooks.append(
            Playbook(
                id="trend_over_time",
                title="Tendencia en el tiempo",
                description=f"Evolución de {metric_col['column']} por {date_col['column']}.",
                action=VisualAction(
                    operation="CREATE",
                    visualType="lineChart",
                    title=f"Tendencia de {metric_col['column']}",
                    layout_intent="chart_full",
                    dataRoles={
                        "Category": {"table": date_col["table"], "column": date_col["column"]},
                        "Y": {"table": metric_col["table"], "column": metric_col["column"], "aggregation": "Sum"},
                    },
                    explanation="Tendencia en el tiempo (línea).",
                ),
            )
        )

    if metric_col:
        playbooks.append(
            Playbook(
                id="kpi_total",
                title="KPI total",
                description=f"Suma total de {metric_col['column']}.",
                action=VisualAction(
                    operation="CREATE",
                    visualType="card",
                    title=f"Total {metric_col['column']}",
                    layout_intent="kpi_top",
                    dataRoles={
                        "Values": {"table": metric_col["table"], "column": metric_col["column"], "aggregation": "Sum"},
                    },
                    explanation="KPI total (tarjeta).",
                ),
            )
        )

    # 2) Visuales/KPIs que **requieren medida** (fallback determinista → Measure Assistant).
    #    Si NO hay métrica clara, igual entregamos un KPI útil (DistinctCount) basado en una dimensión.
    if cat_col:
        measure_name = f"Total de {cat_col['column']} únicos"
        dax_expr = _render_template(
            "distinct_count",
            {"table": cat_col["table"], "column": cat_col["column"]},
        )
        if dax_expr:
            playbooks.append(
                Playbook(
                    id="kpi_distinct_category",
                    title=f"{cat_col['column']} únicos (KPI)",
                    description=f"DistinctCount de {cat_col['column']} (requiere medida).",
                    action=VisualAction(
                        operation="CREATE",
                        visualType="card",
                        title=measure_name,
                        layout_intent="kpi_top",
                        dataRoles={},  # Se crea placeholder; la medida se asigna manualmente.
                        explanation="Para conteos únicos en tarjetas, Power BI requiere una medida en el modelo.",
                        requirements=_make_requirements(
                            operation="distinct_count",
                            measure_template_id="distinct_count",
                            suggested_measure_name=measure_name,
                            table=cat_col["table"],
                            column=cat_col["column"],
                            dax_suggestion=f"{measure_name} = {dax_expr}",
                        ),
                    ),
                )
            )

    # % del total (si tenemos métrica y dimensión)
    if metric_col and cat_col:
        base_expr = f"SUM('{metric_col['table']}'[{metric_col['column']}])"
        dax_expr = _render_template(
            "percent_of_total_agg",
            {"base_expr": base_expr, "table": cat_col["table"], "category_column": cat_col["column"]},
        )
        if dax_expr:
            name = f"% {metric_col['column']} del total"
            playbooks.append(
                Playbook(
                    id="kpi_percent_of_total",
                    title="% del total (KPI)",
                    description=f"% del total de {metric_col['column']} por {cat_col['column']} (requiere medida).",
                    action=VisualAction(
                        operation="CREATE",
                        visualType="card",
                        title=name,
                        layout_intent="kpi_top",
                        dataRoles={},  # Placeholder; se asigna medida.
                        explanation="Para participación (% del total) en tarjeta, Power BI requiere una medida en el modelo.",
                        requirements=_make_requirements(
                            operation="percent_of_total",
                            measure_template_id="percent_of_total_agg",
                            suggested_measure_name=name,
                            table=cat_col["table"],
                            column=cat_col["column"],
                            dax_suggestion=f"{name} = {dax_expr}",
                            format_hint="percentage",
                        ),
                    ),
                )
            )

    # Acumulado / YoY (si existe fecha + métrica)
    if metric_col and date_col:
        base_expr = f"SUM('{metric_col['table']}'[{metric_col['column']}])"

        rt_expr = _render_template(
            "running_total_agg",
            {"base_expr": base_expr, "date_table": date_col["table"], "date_col": date_col["column"]},
        )
        if rt_expr:
            name = f"{metric_col['column']} acumulado"
            playbooks.append(
                Playbook(
                    id="kpi_running_total",
                    title="Acumulado (KPI)",
                    description=f"Running total de {metric_col['column']} (requiere medida).",
                    action=VisualAction(
                        operation="CREATE",
                        visualType="card",
                        title=name,
                        layout_intent="kpi_top",
                        dataRoles={},
                        explanation="Para acumulados (running total) en tarjeta, Power BI requiere una medida en el modelo.",
                        requirements=_make_requirements(
                            operation="running_total",
                            measure_template_id="running_total_agg",
                            suggested_measure_name=name,
                            table=date_col["table"],
                            column=date_col["column"],
                            dax_suggestion=f"{name} = {rt_expr}",
                        ),
                    ),
                )
            )

        yoy_expr = _render_template(
            "yoy_delta_agg",
            {"base_expr": base_expr, "date_table": date_col["table"], "date_col": date_col["column"]},
        )
        if yoy_expr:
            name = f"YoY {metric_col['column']}"
            playbooks.append(
                Playbook(
                    id="kpi_yoy_delta",
                    title="YoY (KPI)",
                    description=f"Variación interanual de {metric_col['column']} (requiere medida).",
                    action=VisualAction(
                        operation="CREATE",
                        visualType="card",
                        title=name,
                        layout_intent="kpi_top",
                        dataRoles={},
                        explanation="Para YoY en tarjeta, Power BI requiere una medida en el modelo.",
                        requirements=_make_requirements(
                            operation="yoy",
                            measure_template_id="yoy_delta_agg",
                            suggested_measure_name=name,
                            table=date_col["table"],
                            column=date_col["column"],
                            dax_suggestion=f"{name} = {yoy_expr}",
                        ),
                    ),
                )
            )

    # Limitar ruido: máximo 8.
    return playbooks[:8]
