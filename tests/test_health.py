"""
Smoke Test — Health Endpoint.

WHY: Este es el test más básico posible. Si el servidor arranca
y responde a /health, sabemos que los imports, la configuración,
y la compilación del grafo LangGraph no tienen errores fatales.

Ejecutar con: python -m pytest tests/test_health.py -v
"""

from __future__ import annotations


def test_health_endpoint(client):
    """
    El endpoint /health responde con status 200 y estructura correcta.

    Validators:
    - HTTP 200
    - JSON con "status" == "healthy"
    - Incluye "pbi_mode" (puede ser MOCK o LIVE)
    - Incluye "auth_required"
    """
    response = client.get("/health")

    assert response.status_code == 200

    data = response.json()
    assert data["status"] == "healthy"
    assert "pbi_mode" in data
    assert "auth_required" in data


def test_health_response_format(client):
    """
    El response de /health tiene exactamente los campos esperados.

    WHY: Si alguien agrega campos al health check, este test lo
    detectará como breaking change para cualquier monitor que lo
    consuma (e.g., Docker HEALTHCHECK, K8s readiness probe).
    """
    response = client.get("/health")
    data = response.json()

    expected_keys = {"status", "pbi_mode", "environment", "auth_required"}
    assert set(data.keys()) == expected_keys
