"""
Contract Test — /chat Endpoint.

WHY: Estos tests validan el "contrato" del endpoint /chat:
- Que la estructura de la respuesta es correcta (ChatResponse schema)
- Que errores internos devuelven JSON amigable, no 500 crudo
- Que los timeouts se manejan con HTTP 504

Los tests usan mocks de Gemini para no depender de la API real.

Ejecutar con: python -m pytest tests/test_chat_contract.py -v
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest


def test_chat_requires_body(client):
    """
    POST /chat sin body devuelve 422 (validation error), no 500.

    WHY: Validar que FastAPI intercepta requests inválidos antes
    de que lleguen al orquestador.
    """
    response = client.post("/api/v1/chat")
    assert response.status_code == 422


def test_chat_timeout_returns_friendly_error(client):
    """
    Si Gemini/el orquestador hace timeout, el endpoint devuelve
    HTTP 200 con un ChatResponse amigable (operation=ERROR).

    WHY: El Parachute pattern captura timeouts en la ruta y devuelve
    un ChatResponse graceful en lugar de propagar al global handler.
    """
    from app.ai.gemini_client import GeminiTimeoutError

    with patch(
        "app.api.v1.routes.process_chat_message",
        new_callable=AsyncMock,
    ) as mock_orchestrator:
        mock_orchestrator.side_effect = GeminiTimeoutError(
            "Gemini no respondió en 30s"
        )

        response = client.post(
            "/api/v1/chat",
            json={
                "message": "Muestra las ventas",
                "report_id": "test-report",
                "tenant_id": "test-tenant",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["action"]["operation"] == "ERROR"
        assert "tardó demasiado" in data["action"]["explanation"]
        assert data["intent"] == "ERROR"


def test_chat_exhausted_returns_friendly_error(client):
    """
    Si Gemini agota los reintentos, el endpoint devuelve HTTP 200
    con un ChatResponse amigable (operation=ERROR).
    """
    from app.ai.gemini_client import GeminiExhaustedError

    with patch(
        "app.api.v1.routes.process_chat_message",
        new_callable=AsyncMock,
    ) as mock_orchestrator:
        mock_orchestrator.side_effect = GeminiExhaustedError(
            "Gemini falló después de 3 intentos"
        )

        response = client.post(
            "/api/v1/chat",
            json={
                "message": "Muestra las ventas",
                "report_id": "test-report",
                "tenant_id": "test-tenant",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["action"]["operation"] == "ERROR"
        assert data["intent"] == "ERROR"


def test_unexpected_error_returns_graceful_chat_response(client):
    """
    Cualquier error inesperado en el pipeline de IA devuelve HTTP 200
    con un ChatResponse amigable (operation=ERROR), no un 500 crudo.

    WHY: El Parachute pattern garantiza que errores de procesamiento
    de IA (JSON roto, Pydantic validation, etc.) nunca generen alertas
    de severity=ERROR en Google Cloud Run. Se loguean como WARNING.
    """
    with patch(
        "app.api.v1.routes.process_chat_message",
        new_callable=AsyncMock,
    ) as mock_orchestrator:
        mock_orchestrator.side_effect = RuntimeError("Algo falló inesperadamente")

        response = client.post(
            "/api/v1/chat",
            json={
                "message": "Muestra las ventas",
                "report_id": "test-report",
                "tenant_id": "test-tenant",
            },
        )

        # Parachute: devuelve 200 con error controlado, no 500
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["action"]["operation"] == "ERROR"
        assert data["intent"] == "ERROR"
        # Verify no stack trace is leaked
        assert "Traceback" not in str(data)


def test_chat_parse_error_returns_graceful_response(client):
    """
    Si Gemini responde con basura no parseable y el error escapa,
    el Parachute devuelve HTTP 200 con un mensaje amigable.

    WHY: Un usuario que escribe 'asdfghjkl' no debe disparar una
    alerta de producción.
    """
    with patch(
        "app.api.v1.routes.process_chat_message",
        new_callable=AsyncMock,
    ) as mock_orchestrator:
        mock_orchestrator.side_effect = ValueError(
            "Gemini devolvió texto plano en lugar de JSON"
        )

        response = client.post(
            "/api/v1/chat",
            json={
                "message": "asdfghjkl sin sentido",
                "report_id": "test-report",
                "tenant_id": "test-tenant",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["action"]["operation"] == "ERROR"
        assert "follow_up_questions" in data["action"]

