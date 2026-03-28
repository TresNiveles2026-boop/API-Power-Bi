"""
Measure Template Service — Catálogo determinista de medidas DAX.

WHY: Evitamos alucinaciones y reducimos dependencia de permisos/SDK.
El LLM no "inventa" DAX; el sistema ofrece plantillas aprobadas que
el usuario puede crear manualmente en Power BI Desktop una sola vez.
"""

from __future__ import annotations

from app.models.schemas import MeasureTemplate


def get_measure_templates() -> list[MeasureTemplate]:
    # 10 plantillas base (robustas y frecuentes en BI).
    return [
        MeasureTemplate(
            id="distinct_count",
            display_name="Valores únicos (DistinctCount)",
            description="Cuenta valores distintos de una columna (ej. Materiales únicos).",
            dax_template="DISTINCTCOUNT('{table}'[{column}])",
            required_vars=["table", "column"],
        ),
        MeasureTemplate(
            id="count_rows",
            display_name="Cantidad de registros (CountRows)",
            description="Cuenta filas de una tabla (ej. total de transacciones).",
            dax_template="COUNTROWS('{table}')",
            required_vars=["table"],
        ),
        MeasureTemplate(
            id="count_non_blank",
            display_name="Conteo no vacío (CountA)",
            description="Cuenta valores no vacíos de una columna (seguro para texto y número).",
            dax_template="COUNTA('{table}'[{column}])",
            required_vars=["table", "column"],
        ),
        MeasureTemplate(
            id="percent_of_total_agg",
            display_name="% del total (auto-contenida)",
            description="Participación de una categoría sobre el total, usando una agregación autocontenida.",
            dax_template=(
                "DIVIDE(\n"
                "    {base_expr},\n"
                "    CALCULATE({base_expr}, ALL('{table}'[{category_column}]))\n"
                ")"
            ),
            required_vars=["base_expr", "table", "category_column"],
        ),
        MeasureTemplate(
            id="percent_of_total",
            display_name="% del total",
            description="Participación de una categoría sobre el total.",
            dax_template=(
                "DIVIDE(\n"
                "    [{base_measure}],\n"
                "    CALCULATE([{base_measure}], ALL('{table}'[{category_column}]))\n"
                ")"
            ),
            required_vars=["base_measure", "table", "category_column"],
        ),
        MeasureTemplate(
            id="running_total",
            display_name="Acumulado (Running Total)",
            description="Acumulado en el tiempo para una medida base.",
            dax_template=(
                "CALCULATE(\n"
                "    [{base_measure}],\n"
                "    FILTER(\n"
                "        ALL('{date_table}'[{date_col}]),\n"
                "        '{date_table}'[{date_col}] <= MAX('{date_table}'[{date_col}])\n"
                "    )\n"
                ")"
            ),
            required_vars=["base_measure", "date_table", "date_col"],
        ),
        MeasureTemplate(
            id="running_total_agg",
            display_name="Acumulado (Running Total, auto-contenida)",
            description="Acumulado en el tiempo para una agregación autocontenida (no requiere medida base).",
            dax_template=(
                "CALCULATE(\n"
                "    {base_expr},\n"
                "    FILTER(\n"
                "        ALL('{date_table}'[{date_col}]),\n"
                "        '{date_table}'[{date_col}] <= MAX('{date_table}'[{date_col}])\n"
                "    )\n"
                ")"
            ),
            required_vars=["base_expr", "date_table", "date_col"],
        ),
        MeasureTemplate(
            id="yoy_delta",
            display_name="YoY (variación interanual)",
            description="Diferencia vs el mismo período del año anterior.",
            dax_template=(
                "[{base_measure}] -\n"
                "CALCULATE([{base_measure}], SAMEPERIODLASTYEAR('{date_table}'[{date_col}]))"
            ),
            required_vars=["base_measure", "date_table", "date_col"],
        ),
        MeasureTemplate(
            id="yoy_delta_agg",
            display_name="YoY (variación interanual, auto-contenida)",
            description="Diferencia vs el mismo período del año anterior para una agregación autocontenida.",
            dax_template=(
                "{base_expr} -\n"
                "CALCULATE({base_expr}, SAMEPERIODLASTYEAR('{date_table}'[{date_col}]))"
            ),
            required_vars=["base_expr", "date_table", "date_col"],
        ),
        MeasureTemplate(
            id="yoy_percent",
            display_name="YoY % (crecimiento interanual)",
            description="Porcentaje vs el mismo período del año anterior.",
            dax_template=(
                "DIVIDE(\n"
                "    [{yoy_delta_measure}],\n"
                "    CALCULATE([{base_measure}], SAMEPERIODLASTYEAR('{date_table}'[{date_col}]))\n"
                ")"
            ),
            required_vars=["yoy_delta_measure", "base_measure", "date_table", "date_col"],
        ),
        MeasureTemplate(
            id="yoy_percent_agg",
            display_name="YoY % (crecimiento interanual, auto-contenida)",
            description="Porcentaje vs el mismo período del año anterior para una agregación autocontenida.",
            dax_template=(
                "VAR __curr = {base_expr}\n"
                "VAR __prev = CALCULATE({base_expr}, SAMEPERIODLASTYEAR('{date_table}'[{date_col}]))\n"
                "RETURN DIVIDE(__curr - __prev, __prev)"
            ),
            required_vars=["base_expr", "date_table", "date_col"],
        ),
        MeasureTemplate(
            id="mom_delta",
            display_name="MoM (variación mensual)",
            description="Diferencia vs el período anterior (mes).",
            dax_template=(
                "[{base_measure}] -\n"
                "CALCULATE([{base_measure}], DATEADD('{date_table}'[{date_col}], -1, MONTH))"
            ),
            required_vars=["base_measure", "date_table", "date_col"],
        ),
        MeasureTemplate(
            id="mom_delta_agg",
            display_name="MoM (variación mensual, auto-contenida)",
            description="Diferencia vs el período anterior (mes) para una agregación autocontenida.",
            dax_template=(
                "{base_expr} -\n"
                "CALCULATE({base_expr}, DATEADD('{date_table}'[{date_col}], -1, MONTH))"
            ),
            required_vars=["base_expr", "date_table", "date_col"],
        ),
        MeasureTemplate(
            id="mom_percent",
            display_name="MoM % (crecimiento mensual)",
            description="Porcentaje vs el período anterior (mes).",
            dax_template=(
                "DIVIDE(\n"
                "    [{mom_delta_measure}],\n"
                "    CALCULATE([{base_measure}], DATEADD('{date_table}'[{date_col}], -1, MONTH))\n"
                ")"
            ),
            required_vars=["mom_delta_measure", "base_measure", "date_table", "date_col"],
        ),
        MeasureTemplate(
            id="mom_percent_agg",
            display_name="MoM % (crecimiento mensual, auto-contenida)",
            description="Porcentaje vs el período anterior (mes) para una agregación autocontenida.",
            dax_template=(
                "VAR __curr = {base_expr}\n"
                "VAR __prev = CALCULATE({base_expr}, DATEADD('{date_table}'[{date_col}], -1, MONTH))\n"
                "RETURN DIVIDE(__curr - __prev, __prev)"
            ),
            required_vars=["base_expr", "date_table", "date_col"],
        ),
        MeasureTemplate(
            id="rank_desc_agg",
            display_name="Ranking (DESC, auto-contenida)",
            description="Ranking de una categoría según una agregación autocontenida.",
            dax_template="RANKX(ALL('{table}'[{category_column}]), {base_expr}, , DESC)",
            required_vars=["table", "category_column", "base_expr"],
        ),
        MeasureTemplate(
            id="rank_desc",
            display_name="Ranking (DESC)",
            description="Ranking de una categoría según una medida base.",
            dax_template="RANKX(ALL('{table}'[{category_column}]), [{base_measure}], , DESC)",
            required_vars=["table", "category_column", "base_measure"],
        ),
    ]
