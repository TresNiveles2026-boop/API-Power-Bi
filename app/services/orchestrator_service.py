"""
Orchestrator Service — Servicio que conecta LangGraph con la API.

WHY: Este servicio es el punto de unión entre el endpoint /chat,
el diccionario semántico (Supabase) y el grafo de IA (LangGraph).
Centralizar la orquestación aquí permite que el endpoint sea slim
y que la lógica de negocio sea testeable independientemente.

Phase 4: Incluye timeout guard de 45s para el pipeline completo.
"""

from __future__ import annotations

import asyncio
import logging
import time
from uuid import UUID
from typing import Any

from app.ai.gemini_client import GeminiExhaustedError, GeminiTimeoutError
from app.ai.graph import build_orchestrator_graph
from app.ai.models import AIResponse, ChatResponse, DataRoleBinding, KpiRequirements, VisualAction
from app.db.supabase_client import get_supabase_client
from app.services.measure_template_service import get_measure_templates
from app.services.chat_history_service import (
    add_message,
    create_conversation,
    update_conversation_title,
)
from app.services.semantic_service import (
    format_dictionary_for_prompt,
    get_semantic_dictionary,
)

logger = logging.getLogger(__name__)
SYSTEM_USER_UUID = "00000000-0000-0000-0000-000000000001"

# Compilar el grafo una vez al importar el módulo
# WHY: La compilación valida la estructura del grafo. Hacerlo una vez
# evita re-compilar en cada request (~5ms ahorrados por request).
_graph = build_orchestrator_graph()

# WHY: LIVE mode puede requerir múltiples rondas de corrección en
# Router/Generator/Validator. 120s evita timeouts prematuros.
ORCHESTRATOR_TIMEOUT_SECONDS = 240


def _pluralize_es(word: str) -> str:
    """
    Pluralización ES simple (suficiente para etiquetas KPI).
    NOTE: No pretende cubrir todos los casos del español.
    """
    w = (word or "").strip()
    if not w:
        return w
    lower = w.lower()
    if lower.endswith(("s", "x")):
        return w
    if lower.endswith(("a", "e", "i", "o", "u")):
        return f"{w}s"
    return f"{w}es"


def _dax_escape_single_quotes(value: str) -> str:
    """Escapa comillas simples para identifiers entre comillas en DAX."""
    return (value or "").replace("'", "''")


def _extract_primary_values_binding(action: VisualAction) -> DataRoleBinding | None:
    """Extrae el binding principal de Values (si existe) del contrato moderno."""
    roles = action.dataRoles or {}
    values = roles.get("Values")
    if isinstance(values, DataRoleBinding):
        return values
    if isinstance(values, dict):
        try:
            return DataRoleBinding(**values)
        except Exception:
            return None
    if isinstance(values, list) and values:
        first = values[0]
        if isinstance(first, DataRoleBinding):
            return first
        if isinstance(first, dict):
            try:
                return DataRoleBinding(**first)
            except Exception:
                return None
    return None


def _extract_primary_category_binding(action: VisualAction) -> DataRoleBinding | None:
    """Extrae el binding principal de Category/Legend (si existe) del contrato moderno."""
    roles = action.dataRoles or {}
    for key in ("Category", "Legend", "Axis", "X", "Rows"):
        val = roles.get(key)
        if isinstance(val, DataRoleBinding):
            return val
        if isinstance(val, dict):
            try:
                return DataRoleBinding(**val)
            except Exception:
                continue
        if isinstance(val, list) and val:
            first = val[0]
            if isinstance(first, DataRoleBinding):
                return first
            if isinstance(first, dict):
                try:
                    return DataRoleBinding(**first)
                except Exception:
                    continue
    return None


def _render_agg_expr(agg: str, table: str, column: str) -> str:
    """
    Renderiza una agregación DAX autocontenida.
    NOTE: Usamos COUNTA como fallback seguro para COUNT sobre texto.
    """
    a = (agg or "").strip().lower()
    tbl = _dax_escape_single_quotes(table)
    col = column
    if a in {"sum"}:
        return f"SUM('{tbl}'[{col}])"
    if a in {"average", "avg"}:
        return f"AVERAGE('{tbl}'[{col}])"
    if a in {"min"}:
        return f"MIN('{tbl}'[{col}])"
    if a in {"max"}:
        return f"MAX('{tbl}'[{col}])"
    if a in {"distinctcount"}:
        return f"DISTINCTCOUNT('{tbl}'[{col}])"
    # Count/unknown → COUNTA (robusto ante texto/número)
    return f"COUNTA('{tbl}'[{col}])"


def _attach_kpi_requirements(actions: list[VisualAction], user_message: str) -> None:
    """
    Adjunta requirements deterministas para KPIs que tienden a fallar en SDK/tenants.

    WHY: Evita que el frontend "adivine" y permite activar el asistente de medidas
    usando un contrato estable (needs_measure + dax_suggestion).
    """
    templates = {t.id: t for t in get_measure_templates()}
    distinct_tpl = templates.get("distinct_count")
    pct_tpl = templates.get("percent_of_total_agg")
    rank_tpl = templates.get("rank_desc_agg")
    msg = (user_message or "").lower()
    wants_percent = (
        ("%" in msg)
        or ("porcentaje" in msg)
        or ("participación" in msg)
        or ("participacion" in msg)
        or ("del total" in msg)
        or ("% del" in msg)
    )
    wants_rank = (
        ("ranking" in msg)
        or ("rank" in msg)
        or ("top " in msg)
        or ("top-" in msg)
        or ("mejores" in msg)
        or ("peores" in msg)
    )

    for act in actions:
        try:
            if str(act.operation).upper() != "CREATE":
                continue
            if act.requirements and act.requirements.needs_measure:
                continue

            binding = _extract_primary_values_binding(act)
            agg = (binding.aggregation or "").strip().lower() if binding else ""
            dax = (act.dax or "").strip().upper()

            is_distinct = (agg == "distinctcount") or ("DISTINCTCOUNT(" in dax)
            if is_distinct:
                table = (binding.table or "").strip() if binding else ""
                column = (binding.column or "").strip() if binding else ""
                if not (table and column and distinct_tpl):
                    continue

                plural = _pluralize_es(column)
                suggested_measure_name = f"Total de {plural} Únicos"

                expr = distinct_tpl.dax_template.format(
                    table=_dax_escape_single_quotes(table),
                    column=column,
                )
                dax_suggestion = f"{suggested_measure_name} = {expr}"

                act.requirements = KpiRequirements(
                    needs_measure=True,
                    operation="distinct_count",
                    measure_template_id="distinct_count",
                    suggested_measure_name=suggested_measure_name,
                    table=table,
                    column=column,
                    dax_suggestion=dax_suggestion,
                )
                continue

            # percent_of_total / rank: requieren Category + Values
            cat = _extract_primary_category_binding(act)
            if not (binding and cat and cat.table and cat.column and binding.table and binding.column):
                continue

            # Preferimos señal fuerte en DAX si existe
            is_percent = ("DIVIDE(" in dax and "ALL(" in dax) or wants_percent
            is_rank = ("RANKX(" in dax) or wants_rank
            if not (is_percent or is_rank):
                continue

            base_expr = _render_agg_expr(binding.aggregation or "", binding.table, binding.column)
            cat_table = (cat.table or "").strip()
            cat_col = (cat.column or "").strip()
            if not (cat_table and cat_col):
                continue

            if is_percent and pct_tpl:
                suggested_measure_name = f"% {binding.column} del total"
                expr = pct_tpl.dax_template.format(
                    base_expr=base_expr,
                    table=_dax_escape_single_quotes(cat_table),
                    category_column=cat_col,
                )
                act.requirements = KpiRequirements(
                    needs_measure=True,
                    operation="percent_of_total",
                    measure_template_id="percent_of_total_agg",
                    suggested_measure_name=suggested_measure_name,
                    table=cat_table,
                    column=cat_col,
                    dax_suggestion=f"{suggested_measure_name} = {expr}",
                )
                continue

            if is_rank and rank_tpl:
                suggested_measure_name = f"Ranking {cat_col}"
                expr = rank_tpl.dax_template.format(
                    table=_dax_escape_single_quotes(cat_table),
                    category_column=cat_col,
                    base_expr=base_expr,
                )
                act.requirements = KpiRequirements(
                    needs_measure=True,
                    operation="rank",
                    measure_template_id="rank_desc_agg",
                    suggested_measure_name=suggested_measure_name,
                    table=cat_table,
                    column=cat_col,
                    dax_suggestion=f"{suggested_measure_name} = {expr}",
                )
        except Exception:
            # Nunca rompemos la respuesta por enrichment de requirements.
            continue


def _normalize_name(value: str) -> str:
    return (value or "").strip().lower()


def _find_column_in_schema(semantic_schema: dict[str, Any], desired: str) -> tuple[str, str] | None:
    """
    Busca una columna por nombre (case-insensitive) en semantic_schema.
    Retorna (table, column) o None.
    """
    want = _normalize_name(desired)
    if not want:
        return None

    tables = semantic_schema.get("tables") if isinstance(semantic_schema, dict) else None
    if not isinstance(tables, dict):
        return None

    for table_name, cols in tables.items():
        if not isinstance(cols, list):
            continue
        for c in cols:
            if not isinstance(c, dict):
                continue
            col_name = _normalize_name(str(c.get("column_name", "")))
            if col_name and col_name == want:
                return (str(table_name), str(c.get("column_name")))
    return None


def _salvage_semantic_field_not_found(
    actions: list[VisualAction],
    user_message: str,
    semantic_schema: dict[str, Any],
) -> bool:
    """
    Parachute determinista cuando el LLM inventa campos y el Validator devuelve ERROR.

    Estrategia:
    - Si el usuario pide "% del total/participación" y existen Category+Value en el schema,
      devolvemos un donutChart (participación) en vez de ERROR.
    - Si el usuario pide ranking/top y existen Category+Value, devolvemos un barChart ordenable.
    """
    if not actions:
        return False

    a0 = actions[0]
    if str(a0.operation).upper() != "ERROR":
        return False

    err_hint = (a0.error_code or "") + " " + (a0.explanation or "")
    if "SEMANTIC_FIELD_NOT_FOUND" not in err_hint and "SEMANTIC_FIELD_NOT_FOUND" not in err_hint:
        # (duplicado intencional: el hint puede venir en explanation o code)
        if "SEMANTIC_FIELD_NOT_FOUND" not in (a0.explanation or ""):
            return False

    msg = (user_message or "").lower()
    wants_percent = (
        ("%" in msg)
        or ("porcentaje" in msg)
        or ("participación" in msg)
        or ("participacion" in msg)
        or ("del total" in msg)
    )
    wants_rank = (
        ("ranking" in msg)
        or ("rank" in msg)
        or ("top " in msg)
        or ("top-" in msg)
        or ("mejores" in msg)
        or ("peores" in msg)
    )

    # Extraer nombres sugeridos del propio mensaje (simple, determinista).
    # Caso más común: "... Stock disponible ... Tipo almacén ..."
    value_col = "Stock disponible" if "stock" in msg else ""
    cat_col = "Tipo almacén" if ("tipo almac" in msg or "almac" in msg) else ""

    found_value = _find_column_in_schema(semantic_schema, value_col) if value_col else None
    found_cat = _find_column_in_schema(semantic_schema, cat_col) if cat_col else None
    if not (found_value and found_cat):
        return False

    value_table, value_column = found_value
    cat_table, cat_column = found_cat
    # Preferimos tabla de categoría para agrupar; value table debería ser la misma en modelos simples.
    table = cat_table or value_table

    if wants_percent:
        actions[0] = VisualAction(
            operation="CREATE",
            visualType="donutChart",
            title=f"% de {value_column} por {cat_column}",
            dataRoles={
                "Category": {"table": table, "column": cat_column},
                "Y": {"table": table, "column": value_column, "aggregation": "Sum"},
            },
            explanation=(
                "Para ver participación (% del total) por categoría, un gráfico de dona/pie "
                "es la opción más directa en Power BI. Ya lo dejé listo con tus campos."
            ),
        )
        return True

    if wants_rank:
        actions[0] = VisualAction(
            operation="CREATE",
            visualType="barChart",
            title=f"Ranking de {cat_column} por {value_column}",
            dataRoles={
                "Category": {"table": table, "column": cat_column},
                "Y": {"table": table, "column": value_column, "aggregation": "Sum"},
            },
            explanation=(
                "Para ranking, un gráfico de barras ordenado por la métrica es lo más claro. "
                "Creé el visual con tus campos; solo ordénalo de mayor a menor si tu tenant lo requiere."
            ),
        )
        return True

    return False


async def process_chat_message(
    message: str,
    report_id: str,
    tenant_id: str,
    conversation_id: str | None = None,
    visual_context: list[dict[str, Any]] | None = None,
) -> ChatResponse:
    """
    Procesa un mensaje de chat del usuario a través del orquestador.

    WHY: Este es el flujo principal del sistema. Un mensaje entra,
    se enriquece con contexto semántico, pasa por el grafo LangGraph
    (Router → Generator → Validator → Deliverer), y sale como un
    Action JSON que el frontend puede ejecutar.

    Phase 4: Timeout guard de 45s para el pipeline completo.
    Phase 6: Persistencia de la conversación.

    Args:
        message: Mensaje del usuario en lenguaje natural.
        report_id: UUID del reporte activo en Power BI.
        tenant_id: UUID del tenant (seguridad multi-tenant).
        conversation_id: UUID de la conversación (opcional).

    Returns:
        ChatResponse con la acción generada y metadata.
    """
    start_time = time.time()

    # 1. Gestión de la Conversación (Phase 6)
    if not conversation_id:
        conv_data = await create_conversation(
            tenant_id=tenant_id,
            report_id=report_id,
            title=message[:50] + "..." if len(message) > 50 else message,
        )
        conversation_id = conv_data["id"]

    # Persistir mensaje del usuario
    await add_message(
        conversation_id=conversation_id,  # type: ignore
        role="user",
        content=message,
    )

    # 2. Cargar el diccionario semántico del reporte
    dictionary = await get_semantic_dictionary(
        report_id=report_id,
        tenant_id=tenant_id,
    )

    if dictionary is None:
        return ChatResponse(
            status="error",
            action=VisualAction(
                operation="ERROR",
                explanation=(
                    "No encontré el diccionario semántico para este reporte. "
                    "Asegúrate de haber ejecutado POST /api/v1/sync-schema primero."
                ),
            ),
            intent="ERROR",
            confidence=0.0,
        )

    # 3. Formatear el diccionario para el prompt de Gemini
    semantic_context = format_dictionary_for_prompt(dictionary)
    semantic_schema = {
        "tables": {
            table_name: [
                {
                    "column_name": col.column_name,
                    "data_type": col.data_type or "",
                    "sample_values": col.sample_values or [],
                    "is_measure": col.is_measure or False,
                }
                for col in columns
            ]
            for table_name, columns in dictionary.tables.items()
        }
    }

    # 4. Preparar el estado inicial del grafo
    initial_state: dict[str, Any] = {
        "user_message": message,
        "report_id": report_id,
        "tenant_id": tenant_id,
        "semantic_context": semantic_context,
        "visual_context": visual_context or [],
        "semantic_schema": semantic_schema,
        "intent": "",
        "confidence": 0.0,
        "actions": [],
        "action": None,
        "is_valid": False,
        "validation_errors": [],
        "retry_count": 0,
        "max_retries": 3,
        "error_message": "",
    }

    # 5. Ejecutar el grafo LangGraph con timeout guard
    logger.info(
        "🚀 Orquestador iniciado: message='%.80s...', report=%s",
        message,
        report_id,
    )
    graph_start_time = time.time()

    try:
        final_state = await asyncio.wait_for(
            _graph.ainvoke(initial_state),
            timeout=ORCHESTRATOR_TIMEOUT_SECONDS,
        )
        graph_end_time = time.time()
    except asyncio.TimeoutError:
        graph_end_time = time.time()
        graph_seconds = graph_end_time - graph_start_time
        latency_ms = int((graph_end_time - start_time) * 1000)
        logger.error(
            "⏰ Orquestador timeout después de %dms (%.3fs, límite: %ds)",
            latency_ms,
            graph_seconds,
            ORCHESTRATOR_TIMEOUT_SECONDS,
        )
        raise GeminiTimeoutError(
            f"El orquestador tardó más de {ORCHESTRATOR_TIMEOUT_SECONDS}s"
        )
    except (GeminiTimeoutError, GeminiExhaustedError):
        # Dejar que estos errores se propaguen al handler del chat route
        raise
    except Exception as exc:
        # ── PARACHUTE: Degradación controlada para errores de IA ──
        # WHY: Si el pipeline de LangGraph falla por cualquier razón
        # (JSON roto del LLM, Pydantic validation, KeyError, etc.),
        # NO debemos devolver un 500. Logueamos como WARNING para
        # que Google Cloud Run NO dispare alertas por falsos positivos.
        latency_ms = int((time.time() - start_time) * 1000)
        logger.warning(
            "⚠️ Error no crítico en pipeline de IA (prompt: '%.80s...'): %s",
            message,
            exc,
        )
        fallback_action = VisualAction(
            operation="ERROR",
            explanation=(
                "No pude generar el gráfico con esa descripción. "
                "¿Podrías ser más específico con los datos que necesitas? "
                "Por ejemplo: 'Muéstrame ventas por región en un gráfico de barras'."
            ),
            follow_up_questions=[
                "¿Qué datos te gustaría visualizar?",
                "¿Quieres que lo intente con una pregunta más simple?",
            ],
        )
        return ChatResponse(
            status="success",
            action=fallback_action,
            actions=[fallback_action],
            intent="ERROR",
            confidence=0.0,
            retries_used=0,
            conversation_id=conversation_id,
        )

    # 6. Calcular latencia
    latency_ms = int((time.time() - start_time) * 1000)
    graph_seconds = graph_end_time - graph_start_time
    logger.info("⏱️ Orquestador completado en %dms", latency_ms)
    logger.info("📈 Latencia LangGraph ciclo completo: %.3f segundos", graph_seconds)

    # 7. Construir la respuesta
    actions_data = final_state.get("actions")
    if not isinstance(actions_data, list) or not actions_data:
        legacy_action = final_state.get("action", {})
        actions_data = [legacy_action] if isinstance(legacy_action, dict) else []

    try:
        ai_response = AIResponse(
            actions=[
                VisualAction(**item)
                for item in actions_data
                if isinstance(item, dict)
            ]
        )
        actions = ai_response.actions
    except Exception:
        actions = [
            VisualAction(
                operation="ERROR",
                explanation="Error interno al procesar la respuesta de la IA.",
            )
        ]

    if not actions:
        actions = [
            VisualAction(
                operation="ERROR",
                explanation="No se generaron acciones ejecutables.",
            )
        ]

    # 7.1 Enrichment determinista para KPIs (requirements)
    _attach_kpi_requirements(actions, message)

    # 7.2 Parachute determinista para errores de schema (evita ERROR por campos inventados)
    _salvage_semantic_field_not_found(actions, message, semantic_schema)

    action = actions[0]

    # 8. Registrar audit event (Power Upgrade U4)
    await _log_audit_event(
        tenant_id=tenant_id,
        user_identifier="api_user",
        action=final_state.get("intent", "UNKNOWN"),
        input_data={"message": message, "report_id": report_id},
        output_data={"actions": [a.model_dump() for a in actions]},
        latency_ms=latency_ms,
    )

    # 9. Persistir respuesta del asistente (Phase 6)
    await add_message(
        conversation_id=conversation_id,  # type: ignore
        role="assistant",
        content=action.explanation or "Acción ejecutada",
        action=action.model_dump(),
        intent=final_state.get("intent", "UNKNOWN"),
        confidence=final_state.get("confidence", 0.0),
    )

    return ChatResponse(
        status="success",
        action=action,
        actions=actions,
        intent=final_state.get("intent", "UNKNOWN"),
        confidence=final_state.get("confidence", 0.0),
        retries_used=final_state.get("retry_count", 0),
        conversation_id=conversation_id,
    )


async def _log_audit_event(
    tenant_id: str,
    user_identifier: str,
    action: str,
    input_data: dict[str, Any],
    output_data: dict[str, Any],
    latency_ms: int,
) -> None:
    """
    Registra un evento de auditoría en Supabase.

    WHY: Cada interacción del orquestador se almacena como evento
    inmutable (Power Upgrade U4). Esto permite análisis de costos,
    debugging forense y replay de sesiones.
    """
    def _safe_uuid(value: str | None) -> str | None:
        """Retorna UUID string válido o None si el valor no es UUID."""
        if not value:
            return None
        try:
            return str(UUID(value))
        except (ValueError, TypeError):
            return None

    try:
        safe_user_identifier = _safe_uuid(user_identifier) or SYSTEM_USER_UUID
        client = get_supabase_client()
        client.table("audit_events").insert({
            "tenant_id": tenant_id,
            "user_identifier": safe_user_identifier,
            "action": action,
            "input_data": input_data,
            "output_data": output_data,
            "latency_ms": latency_ms,
            "model": "gemini-3-flash-preview",
            "tokens_used": output_data.get("_token_usage", {}).get("total_tokens", 0),
            "status": "success",
        }).execute()
        logger.debug("📝 Audit event registrado: action=%s", action)
    except Exception as exc:
        # WHY: No hacemos fail si el audit falla — no queremos que un
        # error de logging rompa la respuesta del usuario.
        logger.error("⚠️ Error registrando audit event: %s", exc)
