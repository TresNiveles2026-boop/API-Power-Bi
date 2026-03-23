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
from app.ai.models import AIResponse, ChatResponse, VisualAction
from app.db.supabase_client import get_supabase_client
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
        # Dejar que estos errores se propaguen al global handler de main.py
        raise

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
