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
    HTTP 504 con un JSON amigable, no un 500 crudo.

    WHY: Este es el escenario más común de fallo en producción.
    El usuario debe ver "La IA tardó demasiado" en vez de
    "Internal Server Error".
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

        assert response.status_code == 504
        data = response.json()
        assert "detail" in data
        assert data["error_type"] == "GEMINI_TIMEOUT"


def test_chat_exhausted_returns_503(client):
    """
    Si Gemini agota los reintentos, el endpoint devuelve HTTP 503.
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

        assert response.status_code == 503
        data = response.json()
        assert "detail" in data
        assert data["error_type"] == "GEMINI_EXHAUSTED"


def test_unexpected_error_returns_friendly_json(client):
    """
    Cualquier error inesperado devuelve HTTP 500 con JSON amigable,
    no un stack trace crudo.

    WHY: La regla de oro del plan dice "nunca devolver Internal
    Server Error". Este test lo verifica.
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

        assert response.status_code == 500
        data = response.json()
        assert "detail" in data
        assert data["error_type"] == "INTERNAL_ERROR"
        # Verify no stack trace is leaked
        assert "Traceback" not in data["detail"]
