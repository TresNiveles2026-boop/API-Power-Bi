"""
LangGraph Workflow — Grafo de estado para el orquestador de BI.

WHY: Usamos LangGraph en lugar de una cadena secuencial simple porque
necesitamos un CICLO: si el Validator detecta DAX inválido, debe
re-enviar al Generator con el error para auto-corrección (Self-Healing).
LangChain/Chains no soportan ciclos nativamente; LangGraph sí.

ARQUITECTURA DEL GRAFO:
    User Input → Router → Generator → Validator → Deliverer
                            ↑               |
                            └───── retry ───┘

El estado (OrchestratorState) fluye entre nodos. Cada nodo lee lo
que necesita y escribe su resultado. Los nodos están desacoplados:
el Router no sabe qué hace el Generator.
"""

from __future__ import annotations

import json
import logging
import re
from uuid import uuid4
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from app.ai.gemini_client import call_gemini
from app.ai.models import (
    AIResponse,
    IntentClassification,
    OrchestratorState,
    ValidationResult,
    VisualAction,
)
from app.ai.prompts import (
    GENERATOR_PROMPT_CREATE,
    GENERATOR_PROMPT_EXPLAIN,
    GENERATOR_PROMPT_FILTER,
    GENERATOR_PROMPT_NAVIGATE,
    ROUTER_PROMPT,
    VALIDATOR_PROMPT,
    build_system_prompt,
)

logger = logging.getLogger(__name__)
# Soporta tablas con guiones/espacios y evita capturar funciones (SUM, CALCULATE, etc.)
REFERENCE_PATTERN = re.compile(
    r"(?:'([^']+)'|([A-Za-z0-9_\- ]+))\s*\[([^\]]+)\]"
)
CANONICAL_AGGREGATIONS = {"sum", "average", "count", "min", "max", "distinctcount"}

ERROR_SCHEMA_VALIDATION = "SCHEMA_VALIDATION_FAILED"
ERROR_TARGET_AMBIGUOUS = "TARGET_AMBIGUOUS"
ERROR_TARGET_NOT_FOUND = "TARGET_NOT_FOUND"
ERROR_TARGET_MISSING = "TARGET_MISSING"
ERROR_SEMANTIC_FIELD_NOT_FOUND = "SEMANTIC_FIELD_NOT_FOUND"
ERROR_FILTER_TYPE_MISMATCH = "FILTER_TYPE_MISMATCH"
ERROR_TIME_INTELLIGENCE_REQUIRES_DATE_TABLE = "TIME_INTELLIGENCE_NO_DATE_TABLE"
NOTICE_MULTI_FILTER_INTERSECTION = "MULTI_FILTER_INTERSECTION_NOTICE"


# ═══════════════════════════════════════════════════════════════════
# Smart Card Title helpers
# ═══════════════════════════════════════════════════════════════════

_MONTH_NAMES_ES = {
    "01": "Enero", "02": "Febrero", "03": "Marzo", "04": "Abril",
    "05": "Mayo", "06": "Junio", "07": "Julio", "08": "Agosto",
    "09": "Septiembre", "10": "Octubre", "11": "Noviembre", "12": "Diciembre",
}


def _period_to_spanish(period: str) -> str:
    """Convierte '06-2021' → 'Junio 2021'."""
    parts = period.split("-")
    if len(parts) == 2:
        month_name = _MONTH_NAMES_ES.get(parts[0], parts[0])
        return f"{month_name} {parts[1]}"
    return period


def _extract_user_title(user_msg: str) -> str | None:
    """Extrae título explícito del mensaje del usuario, si existe."""
    import re as _re
    # "de título 'X'" o 'que lleve de título "X"'
    patterns = [
        r'(?:de\s+)?t[ií]tulo\s*["\'](.+?)["\']',
        r'(?:de\s+)?t[ií]tulo\s+"(.+?)"',
        r"(?:de\s+)?t[ií]tulo\s+'(.+?)'",
        r'(?:de\s+)?t[ií]tulo\s+(.+?)(?:\.|,|$)',
    ]
    for pattern in patterns:
        match = _re.search(pattern, user_msg, _re.IGNORECASE)
        if match:
            title = match.group(1).strip().strip("\"'")
            if title:
                return title
    return None


def _build_smart_card_title(
    user_msg: str, metric_column: str, target_period: str
) -> str:
    """Genera título inteligente para tarjetas temporales."""
    # 1. Si el usuario pidió un título específico, usarlo
    user_title = _extract_user_title(user_msg)
    if user_title:
        return user_title
    # 2. Auto-generar título descriptivo
    display_period = _period_to_spanish(target_period)
    # Usar nombre corto de la métrica
    short_metric = metric_column.replace("disponible", "").strip()
    if not short_metric:
        short_metric = metric_column
    return f"{short_metric} {display_period}"


# ═══════════════════════════════════════════════════════════════════
# FASE 5.2: Utilidades para Safety Net temporal en el Validator
# ═══════════════════════════════════════════════════════════════════


def _extract_periodo_samples(semantic_schema: dict) -> list[str]:
    """Extrae valores de ejemplo de la columna Periodo_Mes del schema."""
    tables = semantic_schema.get("tables", {})
    if not isinstance(tables, dict):
        return []
    for _table_name, columns in tables.items():
        if not isinstance(columns, list):
            continue
        for col in columns:
            col_obj = col if isinstance(col, dict) else (
                col.__dict__ if hasattr(col, "__dict__") else {}
            )
            col_name = str(col_obj.get("column_name", "")).strip()
            if col_name == "Periodo_Mes":
                samples = col_obj.get("sample_values", [])
                if isinstance(samples, list):
                    return [str(s) for s in samples if s is not None]
    return []


def _compute_previous_period(periodo_mes: str) -> str | None:
    """Calcula el Periodo_Mes anterior a partir de un string 'MM-YYYY'.

    Maneja correctamente el cruce de año:
       '01-2022' → '12-2021'
       '06-2021' → '05-2021'
    """
    try:
        parts = periodo_mes.strip().split("-")
        if len(parts) != 2:
            return None
        month = int(parts[0])
        year = int(parts[1])
        if month == 1:
            return f"12-{year - 1}"
        return f"{month - 1:02d}-{year}"
    except (ValueError, IndexError):
        return None


def _normalize_identifier(value: str) -> str:
    """Normaliza nombres de tabla/columna para comparaciones robustas."""
    clean = value.strip().strip("'").strip('"').lower()
    # Ignorar espacios para matching flexible con nombres naturales
    return re.sub(r"\s+", "", clean)


def _extract_table_column_refs(text: str) -> list[tuple[str, str]]:
    """Extrae pares (tabla, columna) desde expresiones tipo Tabla[Columna]."""
    refs: list[tuple[str, str]] = []
    for quoted_table, plain_table, column in REFERENCE_PATTERN.findall(text):
        table = (quoted_table or plain_table or "").strip()
        if table and column:
            refs.append((table, column.strip()))
    return refs


def _semantic_errors(
    action_data: dict[str, Any],
    semantic_schema: dict[str, list[str]] | dict[str, dict[str, Any]],
) -> list[str]:
    """
    Valida que referencias en JSON/DAX existan en el Semantic Dictionary.

    Si alguna referencia no existe, el validator forzará self-healing.
    """
    errors: list[str] = []

    if not semantic_schema:
        return errors

    allowed: dict[str, set[str]] = {}
    # Soportar ambos formatos: {"tables": {"T": [...]}} y {"T": [...]}
    schema_tables = semantic_schema.get("tables", semantic_schema) if isinstance(semantic_schema, dict) else {}
    if not isinstance(schema_tables, dict):
        return errors
    for table, columns in schema_tables.items():
        table_key = _normalize_identifier(table)
        if isinstance(columns, list):
            col_names: set[str] = set()
            for c in columns:
                if isinstance(c, str):
                    col_names.add(_normalize_identifier(c))
                elif isinstance(c, dict):
                    col_names.add(_normalize_identifier(c.get("column_name", "")))
            allowed[table_key] = col_names
        elif isinstance(columns, dict):
            allowed[table_key] = {_normalize_identifier(column) for column in columns.keys()}

    def _validate_ref(table: str, column: str, source: str) -> None:
        table_norm = _normalize_identifier(table)
        column_norm = _normalize_identifier(column)
        if table_norm not in allowed:
            errors.append(
                f"{source}: tabla inexistente '{table}'. "
                "Debe existir en el diccionario semántico."
            )
            return
        if column_norm not in allowed[table_norm]:
            errors.append(
                f"{source}: columna inexistente '{table}[{column}]'. "
                "Debe existir en el diccionario semántico."
            )

    data_roles = action_data.get("dataRoles", {})
    if isinstance(data_roles, dict):
        for role, ref in data_roles.items():
            if isinstance(ref, str):
                if not ref:
                    continue
                matches = _extract_table_column_refs(ref)
                if not matches:
                    errors.append(
                        f"dataRoles.{role}: referencia inválida '{ref}'. "
                        "Usa formato Table[Column]."
                    )
                    continue
                for table, column in matches:
                    _validate_ref(table, column, f"dataRoles.{role}")
            elif isinstance(ref, dict):
                table = ref.get("table")
                column = ref.get("column")
                ref_value = ref.get("ref")
                if table and column:
                    _validate_ref(str(table), str(column), f"dataRoles.{role}")
                elif isinstance(ref_value, str):
                    matches = _extract_table_column_refs(ref_value)
                    for table_name, column_name in matches:
                        _validate_ref(table_name, column_name, f"dataRoles.{role}")
                else:
                    errors.append(
                        f"dataRoles.{role}: binding inválido; falta table/column o ref."
                    )

    filters = action_data.get("filters", [])
    if isinstance(filters, list):
        for idx, item in enumerate(filters):
            if not isinstance(item, dict):
                continue
            table = item.get("table")
            column = item.get("column")
            if table and column:
                _validate_ref(str(table), str(column), f"filters[{idx}]")

    dax = action_data.get("dax", "")
    if isinstance(dax, str) and dax.strip():
        for table, column in _extract_table_column_refs(dax):
            _validate_ref(table, column, "dax")

    return errors


def _repair_metric_aggregation_hallucinations(
    action_data: dict[str, Any],
    semantic_schema: dict[str, list[str]] | dict[str, dict[str, Any]],
) -> bool:
    """
    Repara un patrón común del LLM:
    inventa una "columna" como 'Promedio X' / 'Suma X' en dataRoles.Values/Y.
    Se reemplaza por la columna base existente y se expresa la agregación
    en el binding (aggregation), dejando dax vacío.
    """
    if not semantic_schema:
        return False

    schema_tables = semantic_schema.get("tables", semantic_schema) if isinstance(semantic_schema, dict) else {}
    if not isinstance(schema_tables, dict):
        return False

    allowed_map: dict[str, dict[str, str]] = {}
    for table, columns in schema_tables.items():
        table_norm = _normalize_identifier(str(table))
        mapping: dict[str, str] = {}
        if isinstance(columns, list):
            for c in columns:
                if isinstance(c, str):
                    mapping[_normalize_identifier(c)] = c
                elif isinstance(c, dict):
                    actual = str(c.get("column_name", "")).strip()
                    if actual:
                        mapping[_normalize_identifier(actual)] = actual
        elif isinstance(columns, dict):
            for actual in columns.keys():
                if actual:
                    mapping[_normalize_identifier(actual)] = str(actual)
        if mapping:
            allowed_map[table_norm] = mapping

    def _pick_base_column(table: str, raw_column: str) -> tuple[str | None, str | None]:
        col = raw_column.strip()
        # Algunos LLM envían `column` como "Tabla[Col]" en vez de solo "Col".
        if "[" in col and "]" in col:
            parsed = _extract_table_column_refs(col)
            if parsed:
                t2, c2 = parsed[0]
                if t2:
                    table = t2
                col = c2.strip()
        # Normaliza brackets residuales.
        col = col.strip().lstrip("[").rstrip("]").strip()
        lower = col.lower()
        agg: str | None = None

        prefixes = [
            ("promedio", "Average"),
            ("average", "Average"),
            ("avg", "Average"),
            ("media", "Average"),
            ("suma", "Sum"),
            ("sum", "Sum"),
            ("total", "Sum"),
            ("conteo", "Count"),
            ("count", "Count"),
            ("cantidad", "Count"),
            ("distinct count", "DistinctCount"),
            ("distinctcount", "DistinctCount"),
            ("max", "Max"),
            ("máximo", "Max"),
            ("min", "Min"),
            ("mínimo", "Min"),
        ]

        for pfx, agg_name in prefixes:
            if lower.startswith(pfx):
                agg = agg_name
                remainder = col[len(pfx):].strip()
                for cut in ("de ", "del ", "of "):
                    if remainder.lower().startswith(cut):
                        remainder = remainder[len(cut):].strip()
                        break
                col = remainder
                break

        if not agg or not col:
            return None, None

        table_norm = _normalize_identifier(table)
        base_norm = _normalize_identifier(col)

        if table_norm in allowed_map and base_norm in allowed_map[table_norm]:
            return allowed_map[table_norm][base_norm], agg

        for mapping in allowed_map.values():
            if base_norm in mapping:
                return mapping[base_norm], agg

        return None, None

    data_roles = action_data.get("dataRoles")
    if not isinstance(data_roles, dict):
        return False

    changed = False
    for role in ["Values", "Y", "Y2"]:
        ref = data_roles.get(role)
        if isinstance(ref, str):
            matches = _extract_table_column_refs(ref)
            if not matches:
                continue
            table, column = matches[0]
            base_col, agg = _pick_base_column(table, column)
            if base_col and agg:
                data_roles[role] = {
                    "table": table,
                    "column": base_col,
                    "ref": f"{table}[{base_col}]",
                    "aggregation": agg,
                }
                changed = True
        elif isinstance(ref, dict):
            table = str(ref.get("table") or "").strip()
            column = str(ref.get("column") or "").strip()
            if (not table or not column) and isinstance(ref.get("ref"), str):
                parsed = _extract_table_column_refs(str(ref.get("ref") or ""))
                if parsed:
                    t2, c2 = parsed[0]
                    table = table or t2
                    column = column or c2
            if not table or not column:
                continue
            base_col, agg = _pick_base_column(table, column)
            if base_col and agg:
                ref["column"] = base_col
                ref["ref"] = f"{table}[{base_col}]"
                ref["aggregation"] = agg
                ref.pop("measure", None)
                ref["is_measure"] = False
                changed = True

    if changed:
        action_data["dataRoles"] = data_roles
        action_data["dax"] = ""
        action_data["dax_name"] = ""

    return changed


def _sanitize_filters(action_data: dict[str, Any]) -> dict[str, Any]:
    """
    Normaliza filtros para cumplir el schema Pydantic:
    - Completa 'table' si viene implícita en column='Tabla[Columna]'.
    - Convierte 'value' -> 'values' (lista).
    """
    filters = action_data.get("filters")
    if not isinstance(filters, list):
        return action_data

    normalized: list[dict[str, Any]] = []
    for item in filters:
        if not isinstance(item, dict):
            continue
        f = dict(item)

        column = f.get("column")
        if isinstance(column, str):
            matches = _extract_table_column_refs(column.strip())
            if matches:
                table_name, column_name = matches[0]
                if not f.get("table"):
                    f["table"] = table_name.strip()
                f["column"] = column_name.strip()

        if "values" not in f and "value" in f:
            raw_value = f.pop("value")
            f["values"] = raw_value if isinstance(raw_value, list) else [raw_value]

        if "values" in f and not isinstance(f["values"], list):
            f["values"] = [f["values"]]

        if not f.get("operator"):
            f["operator"] = "In"

        normalized.append(f)

    action_data["filters"] = normalized
    return action_data


def _normalize_operation(raw_operation: Any, intent: str) -> str:
    """Normaliza operation para compatibilidad legacy y contrato nuevo."""
    op = str(raw_operation or "").strip().upper()
    intent_upper = str(intent or "").strip().upper()
    intent_to_operation = {
        "CREATE_VISUAL": "CREATE",
        "UPDATE_VISUAL": "UPDATE",
        "EXPLAIN": "EXPLAIN",
        "DELETE": "DELETE",
        "FILTER": "FILTER",
        "NAVIGATE": "NAVIGATE",
        "UNKNOWN": "UNKNOWN",
    }
    if op in {"", "NONE", "NULL", "N/A"}:
        return intent_to_operation.get(intent_upper, "UNKNOWN")
    if op == "CREATE_VISUAL":
        return "CREATE"
    return op


def _coerce_action_item(item: dict[str, Any]) -> dict[str, Any]:
    """
    Normaliza un item de acción del LLM para prevenir errores de tipo.

    WHY: El LLM a veces devuelve dataRoles como strings planos
    (ej: "Stock disponible" en vez de {"table":"...", "column":"Stock disponible"})
    o filters como strings en vez de dicts. Esto causa 'str' object has no
    attribute 'get' río abajo. Coercionamos ANTES de cualquier sanitización.

    HARDENED: También protege top_n, format, y layout contra strings.
    """
    action = dict(item)

    # Coercionar dataRoles: convertir strings a {table, column} dicts
    data_roles = action.get("dataRoles")
    if isinstance(data_roles, dict):
        coerced_roles: dict[str, Any] = {}
        for role_name, role_val in data_roles.items():
            if isinstance(role_val, str) and role_val.strip():
                # Intentar parsear formato "Table[Column]" o "'Table'[Column]"
                parsed = _extract_table_column_refs(role_val)
                if parsed:
                    table, column = parsed[0]
                    coerced_roles[role_name] = {"table": table, "column": column}
                else:
                    # String plano → asumimos que es el nombre de columna
                    coerced_roles[role_name] = {"table": "", "column": role_val.strip()}
            elif role_val is None:
                continue  # Omitir roles nulos
            else:
                coerced_roles[role_name] = role_val
        action["dataRoles"] = coerced_roles
    elif isinstance(data_roles, str):
        action["dataRoles"] = {}
    elif data_roles is None:
        action["dataRoles"] = {}

    # Coercionar filters: asegurar que sean lista de dicts
    filters = action.get("filters")
    if isinstance(filters, list):
        action["filters"] = [f for f in filters if isinstance(f, dict)]
    elif isinstance(filters, str):
        action["filters"] = []
    elif filters is None:
        action["filters"] = []

    # Coercionar top_n: debe ser dict o None, nunca string
    top_n = action.get("top_n")
    if isinstance(top_n, str):
        action["top_n"] = None

    # Coercionar format: debe ser dict o None, nunca string
    fmt = action.get("format")
    if isinstance(fmt, str):
        action["format"] = {"title": fmt} if fmt.strip() else None

    # Coercionar layout: debe ser dict o None, nunca string
    layout = action.get("layout")
    if isinstance(layout, str):
        action["layout"] = None

    # Coercionar suggested_visuals y follow_up_questions: deben ser listas
    for list_field in ("suggested_visuals", "follow_up_questions"):
        val = action.get(list_field)
        if isinstance(val, str):
            action[list_field] = [val] if val.strip() else []
        elif val is not None and not isinstance(val, list):
            action[list_field] = []

    return action


def _coerce_ai_response_payload(result: dict[str, Any], intent: str) -> dict[str, Any]:
    """
    Convierte salida de Gemini al contrato maestro {"actions":[...]}.
    Retrocompat: acepta payloads legacy de una sola acción.
    """
    clean_result = {k: v for k, v in result.items() if k != "_token_usage"}
    if isinstance(clean_result.get("actions"), list):
        return clean_result

    single_action = dict(clean_result)
    single_action["operation"] = _normalize_operation(single_action.get("operation"), intent)
    return {"actions": [single_action]}


def _sanitize_action_payload(action_payload: dict[str, Any]) -> dict[str, Any]:
    """Normaliza campos opcionales y filtros de una acción individual."""
    action = dict(action_payload)
    if "dataRoles" in action and isinstance(action["dataRoles"], dict):
        action["dataRoles"] = {
            k: v for k, v in action["dataRoles"].items()
            if v is not None
        }
    for list_field in ("filters", "suggested_visuals", "follow_up_questions"):
        if list_field in action and action[list_field] is None:
            action[list_field] = []
    return _sanitize_filters(action)


def _is_generic_title(title: str) -> bool:
    """Determina si un título es vacío o demasiado genérico para identificación futura."""
    normalized = _normalize_text_token(title)
    if not normalized:
        return True
    generic_tokens = {
        "grafico",
        "gráfico",
        "chart",
        "visual",
        "visualizacion",
        "visualización",
        "nuevo grafico",
        "nuevo gráfico",
        "sin titulo",
        "sin título",
    }
    return normalized in generic_tokens


def _infer_title_base(action_data: dict[str, Any]) -> str:
    """
    Infere una base de título semántico para CREATE.

    WHY: facilitar trazabilidad y futuras operaciones UPDATE/EXPLAIN.
    """
    visual_type = str(action_data.get("visualType", "") or "").strip() or "visual"
    data_roles = action_data.get("dataRoles", {})
    metric_column = ""
    category_column = ""
    if isinstance(data_roles, dict):
        for role_name, value in data_roles.items():
            if not isinstance(value, dict):
                continue
            column = str(value.get("column", "") or "").strip()
            if not column:
                continue
            role = str(role_name or "").lower().strip()
            if role in {"y", "values", "value", "measure"} and not metric_column:
                metric_column = column
            if role in {"category", "axis", "x", "legend", "series"} and not category_column:
                category_column = column
    if metric_column and category_column:
        return f"{metric_column} por {category_column}"
    if metric_column:
        return metric_column
    return visual_type


def _unique_suffix() -> str:
    """Genera sufijo corto y único para títulos de visual."""
    return uuid4().hex[:4]


def _ensure_unique_create_titles(actions_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Garantiza título único para toda acción CREATE/CREATE_VISUAL.

    Regla: SIEMPRE concatena ` - ID: xxxx` al título operativo del visual.
    """
    used_titles: set[str] = set()
    output: list[dict[str, Any]] = []

    for item in actions_data:
        if not isinstance(item, dict):
            continue
        action = dict(item)
        operation = str(action.get("operation", "")).upper()
        raw_title = str(action.get("title", "") or "").strip()
        fmt = action.get("format")
        format_title = ""
        if isinstance(fmt, dict):
            format_title = str(fmt.get("title", "") or "").strip()

        if operation in {"CREATE", "CREATE_VISUAL"}:
            base_title = format_title or raw_title or _infer_title_base(action)
            base_title = re.sub(r"\s+-\s+ID:\s+[0-9a-fA-F]{4}$", "", base_title).strip()
            final_title = f"{base_title} - ID: {_unique_suffix()}"
            while _normalize_text_token(final_title) in used_titles:
                final_title = f"{base_title} - ID: {_unique_suffix()}"

            used_titles.add(_normalize_text_token(final_title))
            action["title"] = final_title

            if isinstance(fmt, dict):
                fmt["title"] = final_title
            else:
                action["format"] = {"title": final_title}
        elif raw_title:
            used_titles.add(_normalize_text_token(raw_title))

        output.append(action)

    return output


def _build_error_action(
    explanation: str,
    error_code: str,
    follow_up_questions: list[str] | None = None,
) -> dict[str, Any]:
    """Crea una acción de error canónica para degradación controlada."""
    return {
        "operation": "ERROR",
        "visualType": None,
        "title": "",
        "targetVisualName": None,
        "layout": None,
        "format": None,
        "dataRoles": {},
        "dax": "",
        "dax_name": "",
        "filters": [],
        "target_page": "",
        "explanation": explanation,
        "suggested_visuals": [],
        "follow_up_questions": follow_up_questions or [],
        "error_code": error_code,
        "query_type": None,
        "payload": None,
    }


def _has_physical_time_dimensions(
    semantic_schema: dict[str, Any],
    column_names: set[str],
) -> bool:
    tables = semantic_schema.get("tables", {}) if isinstance(semantic_schema, dict) else {}
    for _, cols in (tables.items() if isinstance(tables, dict) else []):
        col_list = cols if isinstance(cols, list) else []
        for col in col_list:
            col_obj = col if isinstance(col, dict) else (col.__dict__ if hasattr(col, "__dict__") else {})
            name = str(col_obj.get("column_name", "")).strip().lower()
            if name in column_names:
                return True
    return False


def _requests_virtual_time_grouping(
    user_message: str,
    semantic_schema: dict[str, Any],
) -> bool:
    msg = str(user_message or "").lower()
    if not msg:
        return False

    allowed_columns = {"año", "anio", "trimestre", "mes_num", "mes num", "nombre mes", "nombremes"}
    if _has_physical_time_dimensions(semantic_schema, allowed_columns):
        return False

    grouping_phrases = [
        "por trimestre",
        "agrupa por trimestre",
        "agrupado por trimestre",
        "eje x trimestre",
        "eje x por trimestre",
        "eje x de trimestre",
        "por semestre",
        "agrupa por semestre",
        "agrupado por semestre",
        "eje x semestre",
        "eje x por semestre",
        "eje x de semestre",
        "por bimestre",
        "agrupa por bimestre",
        "agrupado por bimestre",
        "eje x bimestre",
        "eje x por bimestre",
        "eje x de bimestre",
        "por año",
        "por anio",
        "agrupa por año",
        "agrupa por anio",
        "agrupado por año",
        "agrupado por anio",
        "eje x año",
        "eje x anio",
        "eje x por año",
        "eje x por anio",
        "eje x de año",
        "eje x de anio",
        "por nombre de mes",
        "por nombre mes",
        "por mes_num",
        "por mes num",
        "por mes número",
        "por mes numero",
        "agrupa por nombre de mes",
        "agrupa por mes_num",
    ]
    return any(phrase in msg for phrase in grouping_phrases)


def _extract_date_column_for_guidance(semantic_schema: dict[str, Any]) -> tuple[str, str]:
    tables = semantic_schema.get("tables", {}) if isinstance(semantic_schema, dict) else {}
    for table_name, cols in (tables.items() if isinstance(tables, dict) else []):
        col_list = cols if isinstance(cols, list) else []
        for col in col_list:
            col_obj = col if isinstance(col, dict) else (col.__dict__ if hasattr(col, "__dict__") else {})
            col_name = str(col_obj.get("column_name", "")).strip()
            col_type = str(col_obj.get("data_type", "")).lower()
            if not col_name:
                continue
            if "fecha" in col_name.lower():
                return table_name, col_name
            if col_type in {"fecha", "date", "datetime", "datetime64", "datetime64[ns]"}:
                return table_name, col_name
    if isinstance(tables, dict) and tables:
        return next(iter(tables.keys())), "Fecha"
    return "Tabla", "Fecha"


def _build_virtual_time_guidance(semantic_schema: dict[str, Any]) -> tuple[str, list[str]]:
    table_name, date_column = _extract_date_column_for_guidance(semantic_schema)
    explanation = (
        "No puedo agrupar por Año/Trimestre/Mes_Num/NombreMes porque no existen físicamente en el dataset de Power BI. "
        "Pasos: en Power BI Desktop selecciona la tabla '{table}', crea columnas físicas Año = YEAR([{date}]) y "
        "Trimestre = \"T\" & FORMAT([{date}], \"Q\"), publica el dataset y vuelve a consultar. "
        "Mientras tanto puedo usar Periodo_Mes."
    ).format(table=table_name, date=date_column)
    follow_ups = [
        "¿Quieres que lo grafique por Periodo_Mes?",
        "¿Vas a republicar el dataset con columnas físicas de año/trimestre?",
    ]
    return explanation, follow_ups


def _canonicalize_action_contract(action_payload: dict[str, Any], intent: str) -> dict[str, Any]:
    """
    Normaliza contrato de acción antes de Pydantic.

    WHY: evita fallos por null explícito del LLM y estabiliza el parseo
    para todas las operaciones (CREATE/UPDATE/DELETE/EXPLAIN/ERROR).
    """
    action = dict(action_payload)
    operation = _normalize_operation(action.get("operation"), intent)
    action["operation"] = operation

    if action.get("title") is None:
        action["title"] = ""
    if action.get("dataRoles") is None:
        action["dataRoles"] = {}
    if action.get("dax") is None:
        action["dax"] = ""
    if action.get("dax_name") is None:
        action["dax_name"] = ""
    if action.get("filters") is None:
        action["filters"] = []
    if action.get("target_page") is None:
        action["target_page"] = ""
    if action.get("explanation") is None:
        action["explanation"] = ""
    if action.get("suggested_visuals") is None:
        action["suggested_visuals"] = []
    if action.get("follow_up_questions") is None:
        action["follow_up_questions"] = []
    if action.get("query_type") is None:
        action["query_type"] = None
    if action.get("payload") is None:
        action["payload"] = None
    if operation == "CREATE" and action.get("visualType") is None:
        # Mantener validación estricta en Pydantic con mensaje claro.
        action["visualType"] = None

    return _sanitize_action_payload(action)


def _build_snapshot_dax_template(table: str, date_col: str, metric: str) -> str:
    """Construye el template DAX antimuerte para snapshots de tiempo."""
    return f"""VAR CurrentSnapshot = MAX('{table}'[{date_col}])
VAR PreviousSnapshot = 
    CALCULATE(
        MAX('{table}'[{date_col}]),
        '{table}'[{date_col}] < CurrentSnapshot
    )
RETURN 
    CALCULATE(
        SUM('{table}'[{metric}]),
        '{table}'[{date_col}] = PreviousSnapshot
    )"""


def _inject_snapshot_dax(action_data: dict[str, Any], user_message: str) -> dict[str, Any]:
    """
    Interceptor ZERO-TRUST: Inyecta el DAX compilado evaluando el prompt original 
    con regex, ignorando totalmente la obediencia del LLM.
    """
    action = dict(action_data)
    
    # Patrón RegEx agnóstico para comparar periodos de tiempo
    time_comparison_pattern = re.compile(
        r"\b(anterior|pasado|previo|vs)\b", 
        re.IGNORECASE
    )
    
    if time_comparison_pattern.search(user_message):
        visual_type = action.get("visualType", "") or action.get("targetVisualName", "")
        # Escudo protector Fase 3: no secuestrar gráficos de línea a menos que halla múltiples métricas
        if visual_type in ("lineChart", "columnChart"):
             return action

        # Intentamos deducir las columnas involucradas desde los roles
        data_roles = action.get("dataRoles", {})
        table_name = ""
        date_col = ""
        metric_col = ""
        
        if isinstance(data_roles, dict):
            # Buscar métrica en Y o Values
            for m_role in ("Y", "Values", "Value", "Measure"):
                if m_role in data_roles and isinstance(data_roles[m_role], dict):
                    table_name = data_roles[m_role].get("table", "")
                    metric_col = data_roles[m_role].get("column", "")
                    break
                    
            # Buscar fecha/eje en Category o X
            for c_role in ("Category", "X", "Axis", "Series"):
                if c_role in data_roles and isinstance(data_roles[c_role], dict):
                    if not table_name:
                        table_name = data_roles[c_role].get("table", "")
                    date_col = data_roles[c_role].get("column", "")
                    break
        
        # Fallback de columnas si no vienen explícitas (basado en el patrón de inventarios del dominio)
        if not date_col:
            date_col = "Periodo_Mes" # Guess natural
            
        if table_name and date_col and metric_col:
            # Interrupción brutal: ignoramos lo que sea que haya alucinado el LLM y sobreescribimos
            action["dax"] = _build_snapshot_dax_template(table_name, date_col, metric_col)
            action["dax_name"] = f"Comparación vs Anterior ({metric_col})"
            
    return action

def _normalize_matrix_roles(action_data: dict[str, Any]) -> dict[str, Any]:
    """
    Normalizador de Roles: El SDK de Power BI exige 'Rows' y 'Columns' para matrices, 
    pero el LLM frecuentemente alucina 'Category' y 'Series'.
    Mapeamos automáticamente para prevenir un DataRole fallido en React.
    """
    action = dict(action_data)
    visual_type = action.get("visualType", "") or action.get("targetVisualName", "")
    
    if visual_type == "matrix" and "dataRoles" in action and isinstance(action["dataRoles"], dict):
        roles = action["dataRoles"]
        # Intercambiar Category -> Rows
        if "Category" in roles:
            roles["Rows"] = roles.pop("Category")
        # Intercambiar Series -> Columns
        if "Series" in roles:
            roles["Columns"] = roles.pop("Series")
            
        action["dataRoles"] = roles
    
    return action


# Visuales simples que NUNCA deberían recibir DAX complejo
_SIMPLE_CHART_TYPES = frozenset({
    "lineChart", "columnChart", "barChart", "areaChart",
    "pieChart", "donutChart", "scatterChart",
})


def _infer_requested_aggregation(user_message: str) -> str:
    """
    Inferencia determinista de agregación pedida por el usuario.
    Default: Sum.
    """
    msg = str(user_message or "").lower()
    if any(k in msg for k in ("promedio", "average", "avg", "media")):
        return "Average"
    if any(k in msg for k in ("recuento", "conteo", "count", "cantidad de", "número de")):
        return "Count"
    if any(k in msg for k in ("máximo", "maximo", "max")):
        return "Max"
    if any(k in msg for k in ("mínimo", "minimo", "min")):
        return "Min"
    if any(k in msg for k in ("únicos", "unicos", "distinct")):
        return "DistinctCount"
    if any(k in msg for k in ("suma", "sum", "total")):
        return "Sum"
    return "Sum"


def _enforce_measure_aggregation(action_data: dict[str, Any], user_message: str) -> dict[str, Any]:
    """
    Si el LLM omite aggregation en el rol de métrica, lo completamos
    determinísticamente según el mensaje del usuario.
    """
    action = dict(action_data)
    op = str(action.get("operation", "")).upper()
    if op not in {"CREATE", "CREATE_VISUAL"}:
        return action

    data_roles = action.get("dataRoles")
    if not isinstance(data_roles, dict):
        return action

    agg = _infer_requested_aggregation(user_message)

    metric_role_candidates = ["Values", "Y", "Y2", "X"]
    for role in metric_role_candidates:
        ref = data_roles.get(role)
        if not isinstance(ref, dict):
            continue
        if ref.get("aggregation"):
            continue
        # No fuerces agregación sobre medidas explícitas.
        if ref.get("measure"):
            continue
        ref["aggregation"] = agg
        data_roles[role] = ref

    action["dataRoles"] = data_roles
    return action


def _prefer_native_aggregation_over_dax(action_data: dict[str, Any]) -> dict[str, Any]:
    """
    Si hay aggregation en dataRoles, preferimos agregación nativa del SDK
    (aggregationFunction) y evitamos DAX (reduce falsos positivos por tipos stale).
    """
    action = dict(action_data)
    op = str(action.get("operation", "")).upper()
    if op not in {"CREATE", "CREATE_VISUAL"}:
        return action

    visual_type = str(action.get("visualType", "") or "")
    if visual_type not in _SIMPLE_CHART_TYPES and visual_type not in {"matrix", "table"}:
        return action

    roles = action.get("dataRoles") or {}
    if not isinstance(roles, dict):
        return action

    has_agg = any(isinstance(v, dict) and bool(v.get("aggregation")) for v in roles.values())
    if not has_agg:
        return action

    action["dax"] = ""
    action["dax_name"] = ""
    return action

# Patrones DAX que son incompatibles con visual calculations del SDK
_COMPLEX_DAX_PATTERN = re.compile(
    r"\b(VAR|CALCULATE|FILTER|SUMX|AVERAGEX|COUNTX|MAXX|MINX|EARLIER|RETURN)\b",
    re.IGNORECASE,
)


def _sanitize_dax_for_simple_visuals(action_data: dict[str, Any]) -> dict[str, Any]:
    """
    Limpia DAX residual/alucinado de visuales simples.

    Si el visual es un chart estándar (lineChart, barChart, etc.) y el DAX
    contiene patrones complejos (VAR, CALCULATE, etc.), lo eliminamos para
    evitar que el frontend intente inyectarlo como visual calculation y
    Power BI muestre la "X roja".

    Si el DAX fue inyectado deliberadamente por nuestro interceptor de
    snapshots (dax_name contiene "Comparación vs Anterior"), lo respetamos.
    """
    action = dict(action_data)
    visual_type = action.get("visualType", "") or ""
    dax = action.get("dax", "") or ""
    dax_name = action.get("dax_name", "") or ""

    if not dax or visual_type not in _SIMPLE_CHART_TYPES:
        return action

    # Respetar DAX inyectado deliberadamente por nuestro interceptor
    if "Comparación vs Anterior" in dax_name:
        return action

    # Si el DAX es complejo (tiene VARs, CALCULATE, etc.), lo reemplazamos
    # con una medida SUM simple. PBI necesita una medida DAX para renderizar
    # datos en charts — `aggregationFunction: "Sum"` del frontend no basta.
    if _COMPLEX_DAX_PATTERN.search(dax) or "\n" in dax:
        logger.warning(
            "🛡️ DAX complejo purgado para visual simple '%s': %s",
            visual_type,
            dax[:80],
        )

        # ESTRATEGIA 1: Extraer columna REAL del DAX original.
        # El DAX siempre referencia columnas reales dentro de SUM/COUNT/etc.
        # Ejemplo: CALCULATE(SUM('Tabla'[Stock disponible]), ...) → Stock disponible
        _DAX_COL_RE = re.compile(
            r"(?:SUM|AVERAGE|COUNT|MIN|MAX|COUNTA|COUNTX|SUMX)\s*\(\s*'([^']+)'\[([^\]]+)\]",
            re.IGNORECASE,
        )
        col_match = _DAX_COL_RE.search(dax)

        if col_match:
            real_table = col_match.group(1)
            real_col = col_match.group(2)
            simple_dax = f"SUM('{real_table}'[{real_col}])"
            simple_name = f"Sum {real_col}"
            logger.info("🔄 Columna extraída del DAX original: '%s'.'%s'", real_table, real_col)
        else:
            # ESTRATEGIA 2: Fallback — buscar cualquier 'table'[column] en el DAX
            _ANY_COL_RE = re.compile(r"'([^']+)'\[([^\]]+)\]")
            any_match = _ANY_COL_RE.search(dax)
            if any_match:
                real_table = any_match.group(1)
                real_col = any_match.group(2)
                simple_dax = f"SUM('{real_table}'[{real_col}])"
                simple_name = f"Sum {real_col}"
                logger.info("🔄 Columna extraída (fallback regex): '%s'.'%s'", real_table, real_col)
            else:
                simple_dax = ""
                simple_name = ""
                logger.warning("⚠️ No se pudo extraer columna del DAX original")

        action["dax"] = simple_dax
        action["dax_name"] = simple_name

        # Corregir el dataRole Y si referencia un nombre de medida inexistente
        if simple_dax and col_match:
            data_roles = action.get("dataRoles", {}) or {}
            for role_key in ("Y", "Values"):
                role_val = data_roles.get(role_key)
                if isinstance(role_val, dict):
                    # Si la columna del rol NO coincide con la columna real, corregir
                    if role_val.get("column", "") != real_col:
                        logger.info(
                            "🔧 dataRoles.%s corregido: '%s' → '%s'",
                            role_key, role_val.get("column", ""), real_col,
                        )
                        role_val["column"] = real_col
                    break

    return action


# Patrones para detectar intención Top/Bottom N en el prompt del usuario.
# Soporta ambas formas: "top 5" (keyword-first) y "los 5 con mayor" (number-first).
_TOPN_PATTERNS = [
    # keyword-first: "top 5", "mejores 10", "primeros 3"
    re.compile(r"\b(?:top|primeros?|mejores?|mayores?|principales?)\s+(\d{1,3})\b", re.IGNORECASE),
    # number-first reversed: "5 principales", "10 mejores"
    re.compile(r"\b(\d{1,3})\s+(?:principales?|primeros?|mejores?|mayores?)\b", re.IGNORECASE),
    # Spanish natural: "los 5 materiales con mayor", "las 10 con más"
    re.compile(r"\b(?:los|las)\s+(\d{1,3})\s+\w+\s+(?:con\s+)?(?:mayor|más|mejor)", re.IGNORECASE),
]
_BOTTOMN_PATTERNS = [
    re.compile(r"\b(?:bottom|últimos?|menores?|peores?|inferiores?)\s+(\d{1,3})\b", re.IGNORECASE),
    re.compile(r"\b(\d{1,3})\s+(?:últimos?|menores?|peores?|inferiores?)\b", re.IGNORECASE),
    re.compile(r"\b(?:los|las)\s+(\d{1,3})\s+\w+\s+(?:con\s+)?(?:menor|menos|peor)", re.IGNORECASE),
]


def _extract_topn_intent(
    action_data: dict[str, Any],
    user_message: str,
) -> dict[str, Any]:
    """
    FASE 14: Interceptor determinista de TopN.

    Detecta "top 5", "mejores 10", "últimos 3", etc. en el prompt del usuario.
    Inyecta un `top_n` config en la acción para que el frontend aplique
    un filtro TopN nativo del SDK de Power BI (sin DAX).

    Solo actúa en visuals simples (bar, column, pie, donut, line) y cuando
    el LLM no envió un top_n propio. Es 100% determinista — no depende del LLM.
    """
    action = dict(action_data)
    visual_type = action.get("visualType", "") or ""

    # Solo para charts que tienen sentido con TopN
    if visual_type not in _SIMPLE_CHART_TYPES:
        return action

    # No sobrescribir si ya tiene top_n
    if action.get("top_n"):
        return action

    # Detectar intención — probar todos los patrones
    top_match = None
    for pat in _TOPN_PATTERNS:
        top_match = pat.search(user_message)
        if top_match:
            break

    bottom_match = None
    if not top_match:
        for pat in _BOTTOMN_PATTERNS:
            bottom_match = pat.search(user_message)
            if bottom_match:
                break

    match = top_match or bottom_match
    if not match:
        return action

    count = int(match.group(1))
    direction = "Top" if top_match else "Bottom"

    # Extraer columnas del dataRoles
    data_roles = action.get("dataRoles", {}) or {}

    # La medida de orden es típicamente el rol Y/Values
    order_col = ""
    order_table = ""
    for role_key in ("Y", "Values", "X"):
        role_val = data_roles.get(role_key)
        if role_val:
            if isinstance(role_val, str) and "[" in role_val:
                # Formato "Table[Column]"
                parts = role_val.split("[", 1)
                order_table = parts[0].strip("'\"")
                order_col = parts[1].rstrip("]")
            elif isinstance(role_val, dict):
                order_col = role_val.get("column", "")
                order_table = role_val.get("table", "")
            elif isinstance(role_val, str):
                order_col = role_val
            if order_col:
                break

    # La categoría es típicamente de Category/Rows
    cat_col = ""
    cat_table = ""
    for role_key in ("Category", "Rows", "Axis"):
        role_val = data_roles.get(role_key)
        if role_val:
            if isinstance(role_val, str) and "[" in role_val:
                parts = role_val.split("[", 1)
                cat_table = parts[0].strip("'\"")
                cat_col = parts[1].rstrip("]")
            elif isinstance(role_val, dict):
                cat_col = role_val.get("column", "")
                cat_table = role_val.get("table", "")
            elif isinstance(role_val, str):
                cat_col = role_val
            if cat_col:
                break

    if not order_col or not cat_col:
        return action

    action["top_n"] = {
        "count": count,
        "order_by_column": order_col,
        "order_by_table": order_table,
        "category_column": cat_col,
        "category_table": cat_table,
        "direction": direction,
    }

    logger.info(
        "🏆 TopN detectado: %s %d por '%s', categoría '%s'",
        direction,
        count,
        order_col,
        cat_col,
    )

    # FASE 16: Purgar filtros espurios del LLM en la columna de categoría.
    # El LLM malinterpreta "top 5 materiales" como filtro 'material IN ["5"]'
    # lo cual elimina TODOS los datos. Cuando TopN está presente, los filtros
    # del LLM en la columna de categoría son redundantes/incorrectos.
    existing_filters = action.get("filters") or []
    if existing_filters:
        purged = []
        kept = []
        for f in existing_filters:
            f_col = ""
            if isinstance(f, dict):
                t = f.get("target", {}) or {}
                f_col = t.get("column", "")
            if f_col and f_col.lower() == cat_col.lower():
                purged.append(f_col)
            else:
                kept.append(f)
        if purged:
            action["filters"] = kept
            logger.info(
                "🧹 TopN: purgados %d filtro(s) espurio(s) del LLM en columna '%s'",
                len(purged), cat_col,
            )

    return action


def _ensure_aggregation_dax(action_data: dict[str, Any]) -> dict[str, Any]:
    """
    Red de seguridad FINAL: garantiza que todo chart simple tenga una medida
    DAX de agregación. Cubre TODOS los casos:
    - LLM no generó DAX
    - Sanitizador purgó DAX complejo
    - TopN bypass saltó la corrección del Validator

    PBI necesita una medida DAX para renderizar datos en charts.
    `aggregationFunction: "Sum"` del frontend NO es suficiente.
    """
    action = dict(action_data)
    operation = str(action.get("operation", "")).upper()
    visual_type = action.get("visualType", "") or ""
    dax = str(action.get("dax", "") or "").strip()

    # Solo para CREATE de charts simples sin DAX
    if operation not in {"CREATE", "CREATE_VISUAL"}:
        return action
    if visual_type not in _SIMPLE_CHART_TYPES:
        return action
    if dax:
        return action  # Ya tiene DAX, no tocar

    # Si ya hay agregación declarada en dataRoles, el frontend puede usar aggregationFunction.
    roles = action.get("dataRoles") or {}
    if isinstance(roles, dict):
        for v in roles.values():
            if isinstance(v, dict) and v.get("aggregation"):
                return action

    # Buscar la columna numérica del dataRole Y/Values
    data_roles = action.get("dataRoles", {}) or {}
    for role_key in ("Y", "Values", "X"):
        role_val = data_roles.get(role_key)
        if not role_val:
            continue
        col = ""
        tbl = ""
        if isinstance(role_val, dict):
            col = role_val.get("column", "")
            tbl = role_val.get("table", "")
        elif isinstance(role_val, str) and "[" in role_val:
            parts = role_val.split("[", 1)
            tbl = parts[0].strip("'\"")
            col = parts[1].rstrip("]")
        elif isinstance(role_val, str):
            col = role_val
        if col:
            if tbl:
                simple_dax = f"SUM('{tbl}'[{col}])"
            else:
                simple_dax = f"SUM([{col}])"
            action["dax"] = simple_dax
            action["dax_name"] = f"Sum {col}"
            logger.info(
                "🔒 _ensure_aggregation_dax: DAX generado para '%s': %s",
                visual_type, simple_dax,
            )
            return action

    logger.warning("⚠️ _ensure_aggregation_dax: no se encontró columna numérica para '%s'", visual_type)
    return action


def _build_numeric_card_dax(action_data: dict[str, Any]) -> dict[str, Any]:
    """
    Fuerza contrato canónico para tarjetas:
    - si visualType=card
    - si no hay dax
    - si hay un binding numérico simple en Values/Y con aggregation

    Entonces sintetiza una medida DAX NUMÉRICA determinista.
    """
    action = dict(action_data)
    operation = str(action.get("operation", "")).upper().strip()
    visual_type = _normalize_visual_type(str(action.get("visualType", "") or ""))
    if operation not in {"CREATE", "CREATE_VISUAL"} or visual_type != "card":
        return action

    if str(action.get("dax", "") or "").strip():
        return action

    data_roles = action.get("dataRoles", {})
    if not isinstance(data_roles, dict):
        return action

    metric_binding = None
    for role_name in ("Values", "Y", "Value", "Measure"):
        candidate = data_roles.get(role_name)
        if isinstance(candidate, dict):
            metric_binding = candidate
            break
    if not isinstance(metric_binding, dict):
        return action

    if metric_binding.get("measure"):
        return action

    table = str(metric_binding.get("table", "") or "").strip()
    column = str(metric_binding.get("column", "") or "").strip()
    aggregation = str(metric_binding.get("aggregation", "") or "Sum").strip().lower()
    if not table or not column:
        return action

    agg_map = {
        "sum": f"SUM('{table}'[{column}])",
        "average": f"AVERAGE('{table}'[{column}])",
        "count": f"COUNT('{table}'[{column}])",
        "min": f"MIN('{table}'[{column}])",
        "max": f"MAX('{table}'[{column}])",
        "distinctcount": f"DISTINCTCOUNT('{table}'[{column}])",
    }
    dax_expression = agg_map.get(aggregation)
    if not dax_expression:
        return action

    raw_title = str(action.get("title", "") or "").strip()
    safe_title = re.sub(r"\s+-\s+ID:\s+[0-9a-fA-F]{4}$", "", raw_title).strip()
    dax_name = safe_title or f"{aggregation.title()} {column}"

    action["dax"] = dax_expression
    action["dax_name"] = dax_name
    return action


def _append_multi_filter_disclaimer(action_data: dict[str, Any]) -> dict[str, Any]:
    """
    Inyecta disclaimer determinista cuando hay múltiples filtros AND.
    """
    action = dict(action_data)
    filters = action.get("filters", [])
    if not isinstance(filters, list) or len(filters) < 2:
        return action
    message = str(action.get("explanation", "") or "").strip()
    disclaimer = (
        "Nota: se aplicaron múltiples filtros en intersección (AND); "
        "si no hay filas compatibles, el visual puede mostrarse vacío."
    )
    if disclaimer not in message:
        action["explanation"] = f"{message}\n\n{disclaimer}".strip()
    # Señal canónica para frontend/observabilidad.
    if str(action.get("error_code", "") or "").strip() == "":
        action["error_code"] = NOTICE_MULTI_FILTER_INTERSECTION
    return action


def _derive_error_code_from_validation_errors(errors: list[str]) -> str:
    """Deriva el error_code canónico dominante desde mensajes de validación."""
    if not errors:
        return ERROR_SCHEMA_VALIDATION
    joined = "\n".join(errors)
    for code in (
        ERROR_TIME_INTELLIGENCE_REQUIRES_DATE_TABLE,
        ERROR_FILTER_TYPE_MISMATCH,
        ERROR_SEMANTIC_FIELD_NOT_FOUND,
        ERROR_TARGET_AMBIGUOUS,
        ERROR_TARGET_NOT_FOUND,
        ERROR_TARGET_MISSING,
        ERROR_SCHEMA_VALIDATION,
    ):
        if code in joined:
            return code
    return ERROR_SCHEMA_VALIDATION


def _parse_column_types_from_semantic_context(semantic_context: str) -> dict[str, dict[str, str]]:
    """
    Extrae tipos de columnas desde el markdown del diccionario semántico.
    Retorna: {table: {column: type}}
    """
    table_types: dict[str, dict[str, str]] = {}
    current_table: str | None = None
    for raw in semantic_context.splitlines():
        line = raw.strip()
        if line.startswith("## Tabla:"):
            current_table = line.replace("## Tabla:", "", 1).strip()
            table_types.setdefault(current_table, {})
            continue
        if not current_table or not line.startswith("|"):
            continue
        if line.startswith("| Columna ") or line.startswith("|---------"):
            continue
        parts = [p.strip() for p in line.strip("|").split("|")]
        if len(parts) < 2:
            continue
        column_name = parts[0]
        data_type = parts[1]
        if column_name and data_type:
            table_types[current_table][column_name] = data_type
    return table_types


def _filter_type_errors(action_data: dict[str, Any], semantic_context: str) -> list[str]:
    """
    Rechaza operadores matemáticos sobre columnas String/categóricas.
    """
    errors: list[str] = []
    filters = action_data.get("filters", [])
    if not isinstance(filters, list) or not filters:
        return errors

    type_map = _parse_column_types_from_semantic_context(semantic_context)
    invalid_ops = {">", ">=", "<", "<=", "greaterthan", "lessthan", "greaterthanorequal", "lessthanorequal"}

    for idx, item in enumerate(filters):
        if not isinstance(item, dict):
            continue
        table = str(item.get("table", "")).strip()
        column = str(item.get("column", "")).strip()
        operator = str(item.get("operator", "")).strip().lower()
        if not table or not column or operator not in invalid_ops:
            continue
        column_type = type_map.get(table, {}).get(column, "")
        if "string" in column_type.lower() or "text" in column_type.lower():
            errors.append(
                f"filters[{idx}] usa operador matemático '{item.get('operator')}' sobre columna String '{table}[{column}]'. "
                "Este caso debe resolverse con DAX (CALCULATE/FILTER/CAST) y no con filtros nativos del SDK."
            )
    return errors


def _sanitize_filter_types(action_data: dict[str, Any], semantic_context: str) -> dict[str, Any]:
    """
    Sanitiza tipos de valores de filtros según el diccionario semántico real.
    - Columnas String/Text => values como string
    - Columnas numéricas => values como int/float cuando sea posible
    """
    filters = action_data.get("filters", [])
    if not isinstance(filters, list) or not filters:
        return action_data

    type_map = _parse_column_types_from_semantic_context(semantic_context)

    def _normalize_dtype(dtype: str) -> str:
        return (dtype or "").strip().lower()

    def _cast_number(value: Any) -> Any:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value
        text = str(value).strip()
        if text == "":
            return value
        try:
            # Preservar enteros cuando aplica
            if re.fullmatch(r"-?\d+", text):
                return int(text)
            return float(text)
        except Exception:
            return value

    for item in filters:
        if not isinstance(item, dict):
            continue
        table = str(item.get("table", "")).strip()
        column = str(item.get("column", "")).strip()
        if not table or not column:
            continue

        values = item.get("values")
        if not isinstance(values, list):
            continue

        column_type = _normalize_dtype(type_map.get(table, {}).get(column, ""))
        if not column_type:
            continue

        if any(token in column_type for token in ("string", "text", "char", "varchar")):
            item["values"] = [str(v) for v in values]
            continue

        if any(token in column_type for token in ("int", "decimal", "double", "float", "number", "numeric")):
            item["values"] = [_cast_number(v) for v in values]

    return action_data


def _has_aggregation_contract_without_dax(action_data: dict[str, Any]) -> bool:
    """
    Contrato moderno: agregación simple definida en cualquier dataRole
    con dax vacío.
    """
    operation = str(action_data.get("operation", "")).upper()
    if operation not in {"CREATE", "CREATE_VISUAL"}:
        return False
    dax = str(action_data.get("dax", "") or "").strip()
    if dax:
        return False
    data_roles = action_data.get("dataRoles", {})
    if not isinstance(data_roles, dict):
        return False

    for role_value in data_roles.values():
        if not isinstance(role_value, dict):
            continue
        agg = str(role_value.get("aggregation", "")).strip().lower()
        if agg in CANONICAL_AGGREGATIONS:
            return True
    return False


# WHY: TypedDict le dice a LangGraph exactamente qué keys existen en
# el estado. Con dict plano, LangGraph 1.0 no propaga keys entre nodos
# correctamente — cada nodo solo recibe lo que retornó el nodo anterior.
# Con TypedDict, LangGraph mantiene TODOS los keys a lo largo del grafo.
class GraphState(TypedDict, total=False):
    """Estado tipado del grafo LangGraph."""
    user_message: str
    report_id: str
    tenant_id: str
    semantic_context: str
    visual_context: list[dict[str, Any]]
    semantic_schema: dict[str, list[str]]
    intent: str
    confidence: float
    actions: list[dict[str, Any]]
    action: dict[str, Any]
    is_valid: bool
    validation_errors: list[str]
    retry_count: int
    max_retries: int
    error_message: str
    forced_action: dict[str, Any]
    forced_actions: list[dict[str, Any]]


def _normalize_text_token(value: str) -> str:
    """Normaliza texto para matching robusto (ids/títulos)."""
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _normalize_visual_type(value: str) -> str:
    """Normaliza tipos de visual entre etiquetas AI y SDK."""
    raw = _normalize_text_token(value)
    aliases = {
        "clusteredcolumnchart": "columnchart",
        "columnchart": "columnchart",
        "clusteredbarchart": "barchart",
        "barchart": "barchart",
        "linechart": "linechart",
        "piechart": "piechart",
        "donutchart": "donutchart",
        "tableex": "table",
        "table": "table",
        "matrix": "matrix",
        "areachart": "areachart",
        "scatterplot": "scatterchart",
        "scatterchart": "scatterchart",
        "card": "card",
    }
    return aliases.get(raw.replace(" ", ""), raw.replace(" ", ""))


def _build_target_resolution_error(
    reason: str,
    candidates: list[dict[str, Any]],
    error_code: str,
) -> dict[str, Any]:
    """Construye respuesta de error amigable para target ambiguo/no resoluble."""
    options = []
    for item in candidates[:5]:
        options.append(
            f"- {item.get('title') or item.get('id')} "
            f"(id: {item.get('id')}, type: {item.get('type')})"
        )
    options_text = "\n".join(options) if options else "- (sin visuales detectados)"
    return _build_error_action(
        explanation=(
            f"{reason}\n\n"
            "Visuales detectados en la página activa:\n"
            f"{options_text}\n\n"
            "Indica el título exacto del visual objetivo para continuar."
        ),
        error_code=error_code,
        follow_up_questions=[
            "¿Cuál es el título exacto del visual objetivo?",
        ],
    )


def _resolve_visual_target_for_mutations(
    action_data: dict[str, Any],
    visual_context: list[dict[str, Any]],
    user_message: str,
) -> dict[str, Any]:
    """
    Resuelve targetVisualName de UPDATE/DELETE/EXPLAIN de forma determinista.
    """
    operation = str(action_data.get("operation", "")).upper()
    if operation not in {"UPDATE", "DELETE", "EXPLAIN"}:
        return action_data

    if not isinstance(visual_context, list) or not visual_context:
        return _build_target_resolution_error(
            "No recibí contexto de visuales del frontend para resolver la operación.",
            [],
            ERROR_TARGET_MISSING,
        )

    requested_target = str(action_data.get("targetVisualName", "") or "").strip()
    requested_title = str(action_data.get("title", "") or "").strip()
    requested_type = _normalize_visual_type(str(action_data.get("visualType", "") or ""))

    candidates: list[dict[str, Any]] = []
    for raw in visual_context:
        if not isinstance(raw, dict):
            continue
        visual_id = str(raw.get("id", "") or "").strip()
        if not visual_id:
            continue
        visual_type = _normalize_visual_type(str(raw.get("type", "") or ""))
        title = str(raw.get("title", "") or "").strip()
        page = str(raw.get("page", "") or "").strip()
        candidates.append({"id": visual_id, "type": visual_type, "title": title, "page": page})

    if requested_type:
        typed = [c for c in candidates if c["type"] == requested_type]
        if typed:
            candidates = typed

    search_terms = [requested_target, requested_title, user_message]
    for term in search_terms:
        target_norm = _normalize_text_token(term)
        if not target_norm:
            continue

        id_exact = [c for c in candidates if _normalize_text_token(c["id"]) == target_norm]
        if len(id_exact) == 1:
            resolved = dict(action_data)
            resolved["targetVisualName"] = id_exact[0]["id"]
            return resolved

        title_exact = [c for c in candidates if _normalize_text_token(c["title"]) == target_norm]
        if len(title_exact) == 1:
            resolved = dict(action_data)
            resolved["targetVisualName"] = title_exact[0]["id"]
            return resolved

        title_partial = [
            c for c in candidates
            if target_norm and target_norm in _normalize_text_token(c["title"])
        ]
        if len(title_partial) == 1:
            resolved = dict(action_data)
            resolved["targetVisualName"] = title_partial[0]["id"]
            return resolved
        if len(title_partial) > 1:
            return _build_target_resolution_error(
                "Encontré múltiples visuales que coinciden con la referencia solicitada.",
                title_partial,
                ERROR_TARGET_AMBIGUOUS,
            )

    if requested_target or requested_title:
        return _build_target_resolution_error(
            "No encontré un visual que coincida con la referencia solicitada.",
            candidates,
            ERROR_TARGET_NOT_FOUND,
        )

    if len(candidates) == 1:
        resolved = dict(action_data)
        resolved["targetVisualName"] = candidates[0]["id"]
        return resolved

    return _build_target_resolution_error(
        "La solicitud es ambigua: faltó identificar el visual objetivo.",
        candidates,
        ERROR_TARGET_AMBIGUOUS,
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║                         NODOS                                 ║
# ╚══════════════════════════════════════════════════════════════════╝


async def router_node(state: dict[str, Any]) -> dict[str, Any]:
    """
    Nodo 1 — Clasifica la intención del usuario.

    WHY: Clasificar primero permite usar prompts especializados por
    tipo de operación. Un prompt genérico que maneje CREATE + FILTER
    + EXPLAIN simultáneamente sería más largo, más costoso en tokens,
    y menos preciso.
    """
    user_msg = str(state.get("user_message", ""))
    semantic_schema = state.get("semantic_schema", {})
    if _requests_virtual_time_grouping(
        user_msg,
        semantic_schema if isinstance(semantic_schema, dict) else {},
    ):
        explanation, follow_ups = _build_virtual_time_guidance(
            semantic_schema if isinstance(semantic_schema, dict) else {}
        )
        error_action = _build_error_action(
            explanation,
            ERROR_SEMANTIC_FIELD_NOT_FOUND,
            follow_up_questions=follow_ups,
        )
        return {
            "intent": "ERROR",
            "confidence": 1.0,
            "forced_action": error_action,
            "forced_actions": [error_action],
        }

    def _heuristic_intent(user_text: str) -> str:
        t = user_text.lower()
        if any(k in t for k in ("filtra", "filtro", "solo ", "donde ", "where ")):
            return "FILTER"
        if any(k in t for k in ("ve a", "ir a", "navega", "página", "pagina")):
            return "NAVIGATE"
        if any(k in t for k in ("explica", "analiza", "qué significa", "que significa")):
            return "EXPLAIN"
        if any(k in t for k in ("actualiza", "cambia", "oculta", "mueve", "tamaño", "tamano", "leyenda", "título", "titulo")):
            return "UPDATE_VISUAL"
        if any(k in t for k in ("gráfico", "grafico", "chart", "tabla", "matriz", "matrix", "donut", "anillos", "barras", "columnas", "línea", "linea", "kpi", "tarjeta", "card")):
            return "CREATE_VISUAL"
        return "UNKNOWN"

    def _coerce_router_payload(payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        data = {k: v for k, v in payload.items() if k != "_token_usage"}
        if "reason" in data and "reasoning" not in data:
            data["reasoning"] = data.pop("reason")
        if "confidence" in data and isinstance(data["confidence"], str):
            try:
                data["confidence"] = float(data["confidence"])
            except ValueError:
                data["confidence"] = 0.0
        if "intent" in data:
            raw = str(data["intent"] or "").strip().upper()
            alias = {
                "CREATE": "CREATE_VISUAL",
                "UPDATE": "UPDATE_VISUAL",
            }
            data["intent"] = alias.get(raw, raw)
        return data

    system = build_system_prompt(
        state["semantic_context"],
        state.get("visual_context", []),
    )
    full_prompt = f"{system}\n\n{ROUTER_PROMPT}"

    try:
        result = await call_gemini(
            system_prompt=full_prompt,
            user_message=state["user_message"],
            temperature=0.1,  # Muy determinista para clasificación
            timeout_seconds=20,
            max_retries=2,
            required_keys={"intent", "confidence", "reasoning"},
        )
    except Exception as exc:
        # Nunca bloquear el flujo por clasificación: degradar a heurística.
        logger.warning("⚠️ Router LLM falló (%s). Usando heurística.", exc)
        return {"intent": _heuristic_intent(user_msg), "confidence": 0.1}

    cleaned_result: Any = result
    if isinstance(cleaned_result, str):
        cleaned_result = cleaned_result.strip()
        cleaned_result = re.sub(r"^```json\s*", "", cleaned_result, flags=re.IGNORECASE)
        cleaned_result = re.sub(r"^```\s*", "", cleaned_result)
        cleaned_result = re.sub(r"\s*```$", "", cleaned_result)
        cleaned_result = json.loads(cleaned_result.strip())

    try:
        classification = IntentClassification(**_coerce_router_payload(cleaned_result))
    except Exception as exc:
        logger.warning("⚠️ Router inválido (%s). Usando clasificación heurística.", exc)
        classification = IntentClassification(
            intent=_heuristic_intent(user_msg),
            confidence=0.2,
            reasoning="Fallback heurístico por error de clasificación",
        )

    logger.info(
        "🔀 Router: intent=%s, confidence=%.2f, reason=%s",
        classification.intent,
        classification.confidence,
        classification.reasoning,
    )

    return {
        "intent": classification.intent,
        "confidence": classification.confidence,
    }


async def generator_node(state: dict[str, Any]) -> dict[str, Any]:
    """
    Nodo 2 — Genera DAX y configuración visual según la intención.

    WHY: Cada tipo de intención tiene su propio prompt de generación
    con la estructura JSON exacta esperada. Esto reduce ambigüedad
    y mejora la calidad del output.
    """
    intent = state["intent"]
    forced_action = state.get("forced_action")
    if isinstance(forced_action, dict) and forced_action.get("operation") == "ERROR":
        return {"actions": [forced_action], "action": forced_action}

    # ═══════════════════════════════════════════════════════════════
    # FASE 5.2: BYPASS DETERMINÍSTICO — Temporal sin LLM
    # ═══════════════════════════════════════════════════════════════
    # Gemini ignora sistemáticamente la prohibición de DAX complejo
    # para comparaciones temporales. Para evitar el ciclo infinito
    # Generator→Validator→Retry→Timeout, interceptamos aquí y
    # construimos la acción determinísticamente.
    # ── SPY: Log bypass conditions ──
    _bypass_intent_ok = intent in ("CREATE_VISUAL", "CREATE", "UNKNOWN")
    _bypass_retry_ok = state.get("retry_count", 0) == 0
    logger.debug(
        "🔍 BYPASS SPY: intent='%s' (match=%s), retry_count=%s (zero=%s)",
        intent, _bypass_intent_ok, state.get("retry_count", 0), _bypass_retry_ok,
    )

    if _bypass_intent_ok and _bypass_retry_ok:
        user_msg = str(state.get("user_message", "")).lower()
        temporal_triggers = [
            "mes anterior", "último mes", "ultimo mes", "mes pasado",
            "año anterior", "año pasado", "ultimo año", "último año",
            "periodo anterior", "mes previo",
        ]
        # Detectar meses específicos: "mes de mayo", "stock de julio", etc.
        _MONTH_NUM_ES = {
            "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
            "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
            "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
        }
        is_temporal = any(trigger in user_msg for trigger in temporal_triggers)
        specific_month_num = ""
        if not is_temporal:
            for month_name, month_num in _MONTH_NUM_ES.items():
                if month_name in user_msg:
                    specific_month_num = month_num
                    is_temporal = True
                    break
        logger.debug("🔍 BYPASS SPY: is_temporal=%s, specific_month=%s, user_msg='%.60s'",
                     is_temporal, specific_month_num or "RELATIVO", user_msg)

        if is_temporal:
            semantic_schema = state.get("semantic_schema", {})
            schema_keys = list(semantic_schema.keys()) if isinstance(semantic_schema, dict) else "NOT_DICT"
            logger.debug("🔍 BYPASS SPY: schema keys=%s, type=%s", schema_keys, type(semantic_schema).__name__)

            periodo_samples = _extract_periodo_samples(semantic_schema)
            logger.debug("🔍 BYPASS SPY: periodo_samples=%s", periodo_samples)

            if periodo_samples:
                # Determinar tabla y columna métrica del schema
                tables = semantic_schema.get("tables", {})
                table_name = ""
                metric_column = ""
                first_numeric = ""  # fallback
                for tbl, cols in (tables.items() if isinstance(tables, dict) else []):
                    table_name = tbl
                    col_list = cols if isinstance(cols, list) else []
                    for c in col_list:
                        c_obj = c if isinstance(c, dict) else (c.__dict__ if hasattr(c, "__dict__") else {})
                        dtype = str(c_obj.get("data_type", "")).lower()
                        cname = str(c_obj.get("column_name", ""))
                        if dtype in ("int64", "float64", "decimal", "double", "number", "numeric", "numérico", "numerico") and cname != "Mes_Index":
                            if not first_numeric:
                                first_numeric = cname
                            # Priorizar columna mencionada en el mensaje
                            if cname.lower().replace(" ", "") in user_msg.replace(" ", ""):
                                metric_column = cname
                                break
                            # Buscar palabras del nombre de columna en el mensaje
                            words = cname.lower().split()
                            if any(w in user_msg for w in words if len(w) > 3):
                                metric_column = cname
                                break
                    if metric_column:
                        break
                if not metric_column:
                    metric_column = first_numeric

                logger.debug("🔍 BYPASS SPY: table='%s', metric='%s'", table_name, metric_column)

                if table_name and metric_column:
                    unique_periods = list(dict.fromkeys(periodo_samples))

                    if specific_month_num:
                        # Buscar el periodo que coincide con el mes específico
                        target_period = ""
                        for p in unique_periods:
                            if p.startswith(specific_month_num + "-"):
                                target_period = p
                                break
                        if not target_period:
                            logger.debug("🔍 BYPASS SPY: mes %s no encontrado en periodos %s",
                                         specific_month_num, unique_periods)
                    else:
                        # Temporal relativo: penúltimo periodo
                        if len(unique_periods) >= 2:
                            target_period = unique_periods[-2]
                        else:
                            target_period = _compute_previous_period(unique_periods[-1])

                    if target_period:
                        # ── Smart Title ──
                        card_title = _build_smart_card_title(
                            user_msg, metric_column, target_period
                        )
                        deterministic_action = {
                            "operation": "CREATE",
                            "visualType": "card",
                            "title": card_title,
                            "layout_intent": "kpi_top",
                            "format": {"title": card_title, "showLegend": False, "showDataLabels": True},
                            "dataRoles": {
                                "Values": {
                                    "table": table_name,
                                    "column": metric_column,
                                    "ref": f"{table_name}[{metric_column}]",
                                    "aggregation": "Sum",
                                }
                            },
                            "dax": "",
                            "dax_name": "",
                            "filters": [
                                {
                                    "table": table_name,
                                    "column": "Periodo_Mes",
                                    "operator": "In",
                                    "values": [target_period],
                                }
                            ],
                            "explanation": f"Tarjeta con {metric_column} filtrado por Periodo_Mes='{target_period}' (bypass determinístico).",
                        }
                        logger.info(
                            "🚀 BYPASS TEMPORAL: acción determinística generada — "
                            "Periodo_Mes='%s', metric='%s' (sin llamar a Gemini)",
                            target_period, metric_column,
                        )
                        return {"actions": [deterministic_action], "action": deterministic_action}
            else:
                logger.debug("🔍 BYPASS SPY: periodo_samples VACÍO — bypass no puede ejecutar")

    # ═══════════════════════════════════════════════════════════════

    system = build_system_prompt(
        state["semantic_context"],
        state.get("visual_context", []),
    )

    # Seleccionar el prompt adecuado según la intención
    intent_prompts = {
        "CREATE_VISUAL": GENERATOR_PROMPT_CREATE,
        "UPDATE_VISUAL": GENERATOR_PROMPT_CREATE,
        "FILTER": GENERATOR_PROMPT_FILTER,
        "NAVIGATE": GENERATOR_PROMPT_NAVIGATE,
        "EXPLAIN": GENERATOR_PROMPT_EXPLAIN,
    }

    generator_prompt = intent_prompts.get(intent, GENERATOR_PROMPT_CREATE)

    # Si hay errores de validación previos, incluirlos para self-healing
    retry_context = ""
    validation_errors = state.get("validation_errors", [])
    if validation_errors:
        errors_text = "\n".join(f"- {e}" for e in validation_errors)
        has_filter_type_mismatch = any(
            ERROR_FILTER_TYPE_MISMATCH in str(error)
            for error in validation_errors
        )
        if has_filter_type_mismatch:
            retry_context = (
                "\n\nERROR: FILTER_TYPE_MISMATCH.\n"
                f"Errores detectados:\n{errors_text}\n"
                "La columna filtrada es de tipo texto.\n"
                "TIENES PROHIBIDO usar la propiedad 'filters' para esta comparación matemática.\n"
                "DEBES reescribir la acción utilizando la propiedad 'dax' con CALCULATE y FILTER(VALUE(...)) "
                "para forzar la conversión numérica.\n"
                "NO te rindas. Genera el DAX corregido y una acción válida.\n"
            )
        else:
            retry_context = (
                f"\n\n⚠️ INTENTO ANTERIOR FALLÓ CON ESTOS ERRORES:\n{errors_text}\n"
                f"Corrige estos errores específicos en tu nueva respuesta.\n"
            )

    full_prompt = f"{system}\n\n{generator_prompt}{retry_context}"

    result = await call_gemini(
        system_prompt=full_prompt,
        user_message=state["user_message"],
        temperature=0.1,
    )

    # Parsear acciones generadas (contrato maestro AIResponse)
    try:
        ai_payload = _coerce_ai_response_payload(result, intent)
        raw_actions = ai_payload.get("actions", [])
        sanitized_actions = [
            _canonicalize_action_contract(_coerce_action_item(item), intent)
            for item in raw_actions
            if isinstance(item, dict)
        ]
        sanitized_actions = [_build_numeric_card_dax(item) for item in sanitized_actions]
        sanitized_actions = [_inject_snapshot_dax(item, state.get("user_message", "")) for item in sanitized_actions]
        sanitized_actions = _ensure_unique_create_titles(sanitized_actions)
        sanitized_actions = [_normalize_matrix_roles(item) for item in sanitized_actions]
        sanitized_actions = [_enforce_measure_aggregation(item, state.get("user_message", "")) for item in sanitized_actions]
        sanitized_actions = [_prefer_native_aggregation_over_dax(item) for item in sanitized_actions]
        sanitized_actions = [_sanitize_dax_for_simple_visuals(item) for item in sanitized_actions]
        sanitized_actions = [_extract_topn_intent(item, state.get("user_message", "")) for item in sanitized_actions]
        sanitized_actions = [_ensure_aggregation_dax(item) for item in sanitized_actions]
        
        resolved_actions = [
            _resolve_visual_target_for_mutations(
                item,
                state.get("visual_context", []),
                state.get("user_message", ""),
            )
            for item in sanitized_actions
        ]
        ai_response = AIResponse(actions=[VisualAction(**item) for item in resolved_actions])
        actions = [item.model_dump() for item in ai_response.actions]
    except Exception as exc:
        logger.error("❌ Error parseando acciones generadas: %s", exc)
        fallback = VisualAction(**_build_error_action(
            explanation=f"Se procesó tu solicitud pero hubo un error de formato: {exc}",
            error_code=ERROR_SCHEMA_VALIDATION,
        ))
        actions = [fallback.model_dump()]

    primary_action = actions[0] if actions else {"operation": "ERROR", "explanation": "Sin acciones generadas"}
    logger.info(
        "⚙️ Generator: operation=%s, visualType=%s, dax=%s",
        primary_action.get("operation", ""),
        primary_action.get("visualType", ""),
        (primary_action.get("dax", "")[:50] if primary_action.get("dax") else "(none)"),
    )

    return {"actions": actions, "action": primary_action}


async def validator_node(state: dict[str, Any]) -> dict[str, Any]:
    """
    Nodo 3 — Valida el DAX y la coherencia de la acción generada.

    WHY: La IA puede generar DAX sintácticamente correcto pero
    semánticamente inválido (ej: SUM sobre texto, columna inexistente).
    Este nodo actúa como "code review" automático antes de entregar
    al frontend.

    Self-Healing: Si hay errores, incrementa retry_count y retorna
    is_valid=False. El grafo reenviará al Generator con los errores.
    """
    raw_actions = state.get("actions")
    if not isinstance(raw_actions, list) or not raw_actions:
        single_action = state.get("action", {})
        raw_actions = [single_action] if isinstance(single_action, dict) else []

    actions_data = [a for a in raw_actions if isinstance(a, dict)]
    intent = state.get("intent", "")
    semantic_schema = state.get("semantic_schema", {})

    # Para UNKNOWN no aplica validación contractual; entrega mensaje amigable.
    if intent == "UNKNOWN":
        logger.info("✅ Validator: auto-aprobado para intent=%s", intent)
        primary = actions_data[0] if actions_data else {}
        return {"is_valid": True, "validation_errors": [], "actions": actions_data, "action": primary}

    if not actions_data:
        retry_count = state.get("retry_count", 0)
        return {
            "is_valid": False,
            "validation_errors": ["No se generaron acciones válidas."],
            "retry_count": retry_count + 1,
        }

    aggregated_errors: list[str] = []
    validated_actions: list[dict[str, Any]] = []
    system = build_system_prompt(
        state["semantic_context"],
        state.get("visual_context", []),
    )

    for idx, action_data in enumerate(actions_data):
        operation = str(action_data.get("operation", "")).upper()

        # Bypass temprano por acción en error (graceful degradation)
        if operation == "ERROR":
            validated_actions.append(action_data)
            continue

        dax = action_data.get("dax", "")
        semantic_errors = _semantic_errors(action_data, semantic_schema)
        filter_errors = _filter_type_errors(action_data, state.get("semantic_context", ""))
        contract_errors = semantic_errors + filter_errors
        if contract_errors:
            if semantic_errors and _repair_metric_aggregation_hallucinations(action_data, semantic_schema):
                semantic_errors = _semantic_errors(action_data, semantic_schema)
                filter_errors = _filter_type_errors(action_data, state.get("semantic_context", ""))
                contract_errors = semantic_errors + filter_errors
                if not contract_errors:
                    validated_actions.append(action_data)
                    continue

            semantic_tagged = [
                f"{ERROR_SEMANTIC_FIELD_NOT_FOUND}: {err}" for err in semantic_errors
            ]
            filter_tagged = [
                f"{ERROR_FILTER_TYPE_MISMATCH}: {err}" for err in filter_errors
            ]
            aggregated_errors.extend(
                [f"actions[{idx}]: {err}" for err in semantic_tagged + filter_tagged]
            )
            continue

        # Conducto seguro para operaciones no analíticas (mutaciones UI / navegación / filtros)
        if operation in {"UPDATE", "EXPLAIN", "DELETE", "NAVIGATE", "FILTER"}:
            validated_actions.append(action_data)
            continue

        # FASE 14: Bypass para acciones con TopN.
        # Cuando top_n existe, el DAX fue purgado deliberadamente y el
        # frontend maneja la agregación con aggregationFunction: "Sum".
        # El Validator NO debe re-inyectar DAX.
        if action_data.get("top_n"):
            validated_actions.append(action_data)
            continue

        # Agregación canónica en dataRoles + dax vacío => válido
        if operation in {"CREATE", "CREATE_VISUAL"} and _has_aggregation_contract_without_dax(action_data):
            validated_actions.append(action_data)
            continue

        validation_input = (
            f"DAX a validar: {dax}\n\n"
            f"Acción completa: {action_data}\n\n"
            f"Intención: {intent}"
        )
        result = await call_gemini(
            system_prompt=f"{system}\n\n{VALIDATOR_PROMPT}",
            user_message=validation_input,
            temperature=0.1,
        )

        try:
            clean_result = {k: v for k, v in result.items() if k != "_token_usage"}
            validation = ValidationResult(**clean_result)
        except Exception:
            logger.warning("⚠️ No se pudo parsear validación, aprobando por defecto")
            validation = ValidationResult(is_valid=True)

        if validation.is_valid:
            validated_actions.append(action_data)
            continue

        logger.warning("❌ Validator: DAX rechazado en actions[%d] — %s", idx, validation.errors)
        if validation.corrected_dax:
            corrected_action = dict(action_data)
            corrected_action["dax"] = validation.corrected_dax

            # ═════════════════════════════════════════════════════════
            # FASE 5.2: SAFETY NET — Rescatar intención temporal
            # ═════════════════════════════════════════════════════════
            # Si el DAX original tenía patrones temporales (EDATE, PREVIOUSMONTH,
            # Periodo_Mes, MAX(...[Fecha...])) y el corrected_dax es un SUM simple,
            # la intención temporal se perdió. Intentamos rescatarla inyectando
            # un filtro sobre Periodo_Mes.
            original_dax = str(dax or "")
            corrected = str(validation.corrected_dax or "")
            temporal_keywords = ["EDATE", "PREVIOUSMONTH", "DATEADD", "SAMEPERIODLASTYEAR",
                                 "Periodo_Mes", "Mes_Index", "Fecha"]
            original_has_temporal = any(
                kw.lower() in original_dax.lower() for kw in temporal_keywords
            )
            corrected_is_simple = bool(
                re.match(r"^(SUM|AVERAGE|COUNT|MIN|MAX)\s*\(", corrected.strip(), re.IGNORECASE)
                or corrected.strip() == ""
            )
            existing_filters = corrected_action.get("filters") or []
            has_periodo_filter = any(
                isinstance(f, dict) and str(f.get("column", "")).strip() == "Periodo_Mes"
                for f in existing_filters
            )

            if original_has_temporal and corrected_is_simple and not has_periodo_filter:
                # Intentar extraer valor de Periodo_Mes del schema
                periodo_samples = _extract_periodo_samples(semantic_schema)
                if periodo_samples:
                    # Usar el penúltimo periodo DISTINTO disponible en los samples
                    # (no calcular matemáticamente, porque pueden faltar meses).
                    unique_periods = list(dict.fromkeys(periodo_samples))
                    if len(unique_periods) >= 2:
                        prev_period = unique_periods[-2]
                    else:
                        prev_period = _compute_previous_period(unique_periods[-1])
                    if prev_period:
                        temporal_filter = {
                            "table": str(corrected_action.get("dataRoles", {}).get(
                                "Values", corrected_action.get("dataRoles", {}).get("Y", {})
                            ).get("table", "") if isinstance(
                                corrected_action.get("dataRoles", {}).get(
                                    "Values", corrected_action.get("dataRoles", {}).get("Y", {})
                                ), dict) else ""),
                            "column": "Periodo_Mes",
                            "operator": "In",
                            "values": [prev_period],
                        }
                        if not temporal_filter["table"]:
                            # Fallback: primera tabla del schema
                            for tbl_name in (semantic_schema.get("tables", {}) or {}):
                                temporal_filter["table"] = tbl_name
                                break
                        if temporal_filter["table"]:
                            if not isinstance(existing_filters, list):
                                existing_filters = []
                            existing_filters.append(temporal_filter)
                            corrected_action["filters"] = existing_filters
                            corrected_action["dax"] = ""
                            corrected_action["dax_name"] = ""
                            logger.info(
                                "🛡️ FASE 5.2 Safety Net: inyectado filtro temporal "
                                "Periodo_Mes='%s' para rescatar intención temporal",
                                prev_period,
                            )

            validated_actions.append(corrected_action)
            logger.info("🔧 Validator: DAX corregido aplicado en actions[%d]", idx)
            continue

        aggregated_errors.extend([f"actions[{idx}]: {err}" for err in validation.errors])

    if aggregated_errors:
        logger.warning("🧭 Validator: contrato inválido detectado: %s", aggregated_errors)
        retry_count = state.get("retry_count", 0)
        return {
            "is_valid": False,
            "validation_errors": aggregated_errors,
            "retry_count": retry_count + 1,
        }

    primary_action = validated_actions[0] if validated_actions else {}
    logger.info("✅ Validator: acciones aprobadas (%d)", len(validated_actions))
    return {
        "is_valid": True,
        "validation_errors": [],
        "actions": validated_actions,
        "action": primary_action,
    }


async def deliverer_node(state: dict[str, Any]) -> dict[str, Any]:
    """
    Nodo 4 — Prepara la respuesta final para el frontend.

    WHY: Este nodo es el "último checkpoint" antes de enviar la
    respuesta. Asegura que el formato sea consistente y agrega
    metadata útil (retries usados, etc.).
    """
    raw_actions = state.get("actions")
    if not isinstance(raw_actions, list) or not raw_actions:
        forced_action = state.get("forced_action")
        if isinstance(forced_action, dict):
            raw_actions = [forced_action]
        else:
            single_action = state.get("action", {})
            raw_actions = [single_action] if isinstance(single_action, dict) else []
    actions_data = [a for a in raw_actions if isinstance(a, dict)]

    if not state.get("is_valid", True):
        errors = state.get("validation_errors", [])
        details = "\n".join(f"- {item}" for item in errors[:6])
        error_action = _build_error_action(
            explanation=(
                "No pude generar una acción válida con el esquema de datos actual.\n\n"
                f"Detalles:\n{details}" if details else
                "No pude generar una acción válida con el esquema de datos actual."
            ),
            error_code=_derive_error_code_from_validation_errors(errors),
            follow_up_questions=[
                "¿Quieres que lo intente con una visual más simple?",
                "¿Debemos resincronizar el diccionario con /api/v1/sync-schema?",
            ],
        )
        error_action["validation_errors"] = errors
        actions_data = [error_action]

    # Si la intención es UNKNOWN, generar respuesta amigable
    if state.get("intent") == "UNKNOWN" and not any(
        str(a.get("operation", "")).upper() == "ERROR" for a in actions_data
    ):
        unknown_action = {
            "operation": "UNKNOWN",
            "explanation": (
                "No pude entender tu solicitud en el contexto de este reporte. "
                "¿Podrías ser más específico? Por ejemplo:\n"
                "• 'Muéstrame ventas por región'\n"
                "• 'Filtra por el año 2024'\n"
                "• 'Explícame el KPI de Total Ventas'"
            ),
            "follow_up_questions": [
                "¿Qué datos te gustaría visualizar?",
                "¿Necesitas filtrar por algún criterio?",
            ],
        }
        actions_data = [unknown_action]

    sanitized_actions: list[dict[str, Any]] = []
    for action_data in actions_data:
        typed_action = _sanitize_filter_types(
            action_data,
            state.get("semantic_context", ""),
        )
        final_action = _append_multi_filter_disclaimer(typed_action)
        sanitized_actions.append(final_action)
    primary_action = sanitized_actions[0] if sanitized_actions else {
        "operation": "ERROR",
        "explanation": "Sin acciones para entregar.",
    }

    logger.info("📦 Deliverer: respuesta lista, operation=%s", primary_action.get("operation"))
    return {"actions": sanitized_actions, "action": primary_action}


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    ROUTING LOGIC                               ║
# ╚══════════════════════════════════════════════════════════════════╝


def should_retry_or_deliver(state: dict[str, Any]) -> str:
    """
    Decide si reenviar al Generator (retry) o ir al Deliverer.

    WHY: Lógica condicional del grafo. Si el Validator rechazó el DAX
    y aún tenemos reintentos disponibles, volvemos al Generator con
    los errores. Si se acabaron los reintentos, entregamos con error.
    """
    if state.get("is_valid", False):
        return "deliverer"

    max_retries = state.get("max_retries", 2)
    retry_count = state.get("retry_count", 0)

    if retry_count < max_retries:
        logger.info(
            "🔄 Retry %d/%d — reenviando al Generator",
            retry_count,
            max_retries,
        )
        return "generator"

    logger.warning("⚠️ Max retries alcanzado — entregando con errores")
    return "deliverer"


def should_generate_or_deliver(state: dict[str, Any]) -> str:
    """
    Decide si generar acción o entregar directamente con UNKNOWN.

    WHY: Si la intención es UNKNOWN, no tiene sentido enviar al
    Generator — vamos directo al Deliverer con un mensaje amigable.
    """
    if state.get("intent") == "UNKNOWN":
        return "deliverer"
    return "generator"


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    GRAPH BUILDER                               ║
# ╚══════════════════════════════════════════════════════════════════╝


def build_orchestrator_graph() -> StateGraph:
    """
    Construye y compila el grafo de LangGraph.

    WHY: El grafo se construye una vez y se reutiliza para todas
    las requests. La compilación valida que todos los nodos y edges
    estén conectados correctamente antes de ejecutar.

    Returns:
        Grafo compilado listo para .ainvoke()
    """
    graph = StateGraph(GraphState)

    # Añadir nodos
    graph.add_node("router", router_node)
    graph.add_node("generator", generator_node)
    graph.add_node("validator", validator_node)
    graph.add_node("deliverer", deliverer_node)

    # Entry point
    graph.set_entry_point("router")

    # Router → Generator (si intención conocida) o Deliverer (si UNKNOWN)
    graph.add_conditional_edges(
        "router",
        should_generate_or_deliver,
        {"generator": "generator", "deliverer": "deliverer"},
    )

    # Generator → Validator
    graph.add_edge("generator", "validator")

    # Validator → Generator (retry) o Deliverer (success/max retries)
    graph.add_conditional_edges(
        "validator",
        should_retry_or_deliver,
        {"generator": "generator", "deliverer": "deliverer"},
    )

    # Deliverer → END
    graph.add_edge("deliverer", END)

    compiled = graph.compile()
    logger.info("🏗️ Grafo LangGraph compilado exitosamente")

    return compiled
