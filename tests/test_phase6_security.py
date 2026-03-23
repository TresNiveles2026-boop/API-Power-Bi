"""
Security Contract Tests — History ownership + embed error mapping.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch


def test_list_messages_blocks_cross_tenant_access(client):
    """GET messages must reject conversations from another tenant."""
    with patch(
        "app.api.v1.routes.get_conversation",
        new_callable=AsyncMock,
    ) as mock_get_conversation:
        mock_get_conversation.return_value = {
            "id": "conv-1",
            "tenant_id": "other-tenant",
            "report_id": "report-1",
            "title": "x",
        }

        response = client.get("/api/v1/conversations/conv-1/messages")

        assert response.status_code == 403
        assert response.json()["detail"] == "No tienes acceso a esta conversación."


def test_update_conversation_blocks_cross_tenant_access(client):
    """PATCH conversation title must reject conversations from another tenant."""
    with patch(
        "app.api.v1.routes.get_conversation",
        new_callable=AsyncMock,
    ) as mock_get_conversation:
        mock_get_conversation.return_value = {
            "id": "conv-1",
            "tenant_id": "other-tenant",
            "report_id": "report-1",
            "title": "x",
        }

        response = client.patch(
            "/api/v1/conversations/conv-1",
            json={"title": "Nuevo título"},
        )

        assert response.status_code == 403
        assert response.json()["detail"] == "No tienes acceso a esta conversación."


def test_embed_config_runtime_error_maps_to_503(client):
    """Runtime errors in embed generation should map to 503 friendly response."""
    with patch(
        "app.api.v1.routes.get_embed_config",
        new_callable=AsyncMock,
    ) as mock_get_embed:
        mock_get_embed.side_effect = RuntimeError("auth down")

        response = client.post(
            "/api/v1/embed-config?report_id=test-report&tenant_id=test-tenant"
        )

        assert response.status_code == 503
        assert "No se pudo autenticar contra Power BI" in response.json()["detail"]


def test_embed_config_value_error_maps_to_404(client):
    """Missing report for tenant should map to 404 friendly response."""
    with patch(
        "app.api.v1.routes.get_embed_config",
        new_callable=AsyncMock,
    ) as mock_get_embed:
        mock_get_embed.side_effect = ValueError("report missing")

        response = client.post(
            "/api/v1/embed-config?report_id=test-report&tenant_id=test-tenant"
        )

        assert response.status_code == 404
        assert "No se encontró el reporte solicitado" in response.json()["detail"]
