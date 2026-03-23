"""
Chat History Service — Persistence layer for conversations and messages.

WHY: Manages CRUD operations for chat history in Supabase.
Separated from orchestrator_service to keep concerns clean:
Orchestrator handles AI logic, this handles storage.
"""

from __future__ import annotations

import logging
from typing import Any

from app.db.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)


async def create_conversation(
    tenant_id: str,
    report_id: str,
    title: str = "Nueva conversación",
) -> dict[str, Any]:
    """Crea una nueva conversación."""
    client = get_supabase_client()
    try:
        result = (
            client.table("conversations")
            .insert({
                "tenant_id": tenant_id,
                "report_id": report_id,
                "title": title,
            })
            .execute()
        )
        return result.data[0]
    except Exception as exc:
        logger.error("Error creating conversation: %s", exc)
        raise


async def get_conversations(
    tenant_id: str,
    report_id: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Obtiene las conversaciones recientes de un tenant."""
    client = get_supabase_client()
    query = (
        client.table("conversations")
        .select("*")
        .eq("tenant_id", tenant_id)
        .order("updated_at", desc=True)
        .limit(limit)
    )

    if report_id:
        query = query.eq("report_id", report_id)

    result = query.execute()
    return result.data


async def get_conversation_messages(
    conversation_id: str,
) -> list[dict[str, Any]]:
    """Obtiene todos los mensajes de una conversación."""
    client = get_supabase_client()
    result = (
        client.table("messages")
        .select("*")
        .eq("conversation_id", conversation_id)
        .order("created_at", desc=False)  # Chronological order
        .execute()
    )
    return result.data


async def get_conversation(
    conversation_id: str,
) -> dict[str, Any] | None:
    """Obtiene una conversación por ID."""
    client = get_supabase_client()
    result = (
        client.table("conversations")
        .select("id, tenant_id, report_id, title")
        .eq("id", conversation_id)
        .limit(1)
        .execute()
    )
    if not result.data:
        return None
    return result.data[0]


async def add_message(
    conversation_id: str,
    role: str,
    content: str,
    action: dict[str, Any] | None = None,
    intent: str | None = None,
    confidence: float | None = None,
) -> dict[str, Any]:
    """Agrega un mensaje a una conversación y actualiza su timestamp."""
    client = get_supabase_client()
    
    # 1. Insert message
    msg_result = (
        client.table("messages")
        .insert({
            "conversation_id": conversation_id,
            "role": role,
            "content": content,
            "action": action,
            "intent": intent,
            "confidence": confidence,
        })
        .execute()
    )

    # 2. Touch conversation updated_at
    client.table("conversations").update({
        "updated_at": "now()"
    }).eq("id", conversation_id).execute()

    return msg_result.data[0]


async def update_conversation_title(
    conversation_id: str,
    tenant_id: str,
    title: str,
) -> bool:
    """Actualiza el título de una conversación del tenant indicado."""
    client = get_supabase_client()
    result = (
        client.table("conversations")
        .update({"title": title})
        .eq("id", conversation_id)
        .eq("tenant_id", tenant_id)
        .execute()
    )
    return bool(result.data)
