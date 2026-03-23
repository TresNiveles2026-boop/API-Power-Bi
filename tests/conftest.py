"""
Pytest Fixtures — Shared test configuration.

WHY: Centralizar el setup de tests para que todos los test files
usen el mismo TestClient y los mismos mocks. Esto evita duplicación
y garantiza que los tests no dependan de servicios externos (Supabase,
Gemini).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """
    TestClient de FastAPI con mocks de dependencias externas.

    WHY: Los smoke tests no deben depender de Supabase ni de Gemini.
    Mockeamos las conexiones externas y el auth para que los tests
    sean rápidos, deterministas y ejecutables sin credenciales.
    """
    # Mock Supabase client before importing the app
    with patch("app.db.supabase_client.get_supabase_client") as mock_sb:
        mock_sb.return_value = MagicMock()

        # Import app and auth dependency inside the patch context
        from app.main import app
        from app.auth.auth_middleware import CurrentUser, get_current_user

        # Override auth to bypass API key requirement
        mock_user = CurrentUser(
            tenant_id="test-tenant",
            api_key_id="test-key",
            key_name="test",
        )

        app.dependency_overrides[get_current_user] = lambda: mock_user

        # raise_server_exceptions=False allows testing custom exception handlers
        # instead of having the client receive the raised exception directly.
        with TestClient(app, raise_server_exceptions=False) as test_client:
            yield test_client

        # Cleanup
        app.dependency_overrides.clear()


@pytest.fixture
def mock_gemini():
    """
    Mock del cliente Gemini que devuelve una respuesta válida.

    WHY: Los tests de contrato del /chat necesitan que Gemini devuelva
    algo válido sin llamar a la API real. Este mock simula una respuesta
    típica del orquestador.
    """
    mock_response = {
        "intent": "CREATE",
        "confidence": 0.95,
        "action": {
            "operation": "CREATE_VISUAL",
            "visualType": "barChart",
            "title": "Ventas por Categoría",
            "dataRoles": {"category": "Categoría", "values": "Total Ventas"},
            "dax": "SUMMARIZE('Ventas', 'Ventas'[Categoría], \"Total\", SUM('Ventas'[Monto]))",
            "dax_name": "MedidaVentas",
            "filters": [],
            "target_page": "",
            "explanation": "He creado un gráfico de barras mostrando las ventas por categoría.",
            "suggested_visuals": [],
            "follow_up_questions": [
                "¿Quieres filtrar por período?",
                "¿Agregar una línea de tendencia?"
            ],
        },
        "_token_usage": {
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "total_tokens": 150,
        },
    }

    with patch("app.ai.gemini_client.call_gemini", new_callable=AsyncMock) as mock:
        mock.return_value = mock_response
        yield mock
