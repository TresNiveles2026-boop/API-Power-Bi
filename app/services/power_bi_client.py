"""
Power BI Client — Patrón Strategy/Adapter (Mock / Live).

WHY: Desacoplamos la lógica de negocio del proveedor de API.
Durante las Fases 1-2 usamos MockPowerBIClient que simula respuestas
y logea el payload exacto en consola. En la Fase 3, con el Trial de
Power BI Pro, activamos LivePowerBIClient sin cambiar una sola línea
de la lógica del orquestador — solo cambiamos PBI_API_MODE en .env.

Este patrón aplica el principio Open/Closed de ANTIGRAVITY_RULES:
agregar un nuevo modo (ej: "STAGING") no requiere modificar los
clientes existentes, solo agregar una nueva clase.
"""

from __future__ import annotations

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    INTERFAZ ABSTRACTA                          ║
# ╚══════════════════════════════════════════════════════════════════╝


class PowerBIClientBase(ABC):
    """
    Contrato que todo cliente de Power BI debe cumplir.

    WHY: Al definir una interfaz abstracta, cualquier componente que
    dependa de Power BI (LangGraph Validator, Action Handler, etc.)
    programa contra la abstracción, no contra la implementación concreta.
    """

    @abstractmethod
    async def execute_dax_query(
        self,
        dataset_id: str,
        dax_query: str,
    ) -> dict[str, Any]:
        """
        Ejecuta una consulta DAX contra un dataset de Power BI.

        WHY: Usado por el Nodo Validator de LangGraph para verificar
        que el DAX generado por Gemini realmente se ejecuta sin errores
        antes de inyectarlo en el reporte del usuario.
        """

    @abstractmethod
    async def generate_embed_token(
        self,
        report_id: str,
        workspace_id: str,
    ) -> dict[str, Any]:
        """
        Genera un Embed Token para incrustar un reporte en el frontend.

        WHY: Power BI Embedded requiere un token de corta duración (~1h)
        para cada sesión de visualización. Este método lo obtiene via
        la REST API y el frontend lo usa para inicializar el SDK JS.
        """

    @abstractmethod
    async def get_report_pages(
        self,
        report_id: str,
        workspace_id: str,
    ) -> list[dict[str, Any]]:
        """
        Obtiene las páginas de un reporte para navegación programática.

        WHY: El orquestador necesita saber qué páginas existen para
        poder responder a intenciones como "ve a la página de Ventas".
        """

    @abstractmethod
    async def get_report_metadata(
        self,
        report_id: str,
        workspace_id: str,
    ) -> dict[str, Any]:
        """
        Obtiene metadata del reporte (nombre, dataset vinculado, etc.).

        WHY: Necesario para la sincronización del Diccionario Semántico.
        Vincula el report_id con su dataset_id para las queries DAX.
        """


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    MOCK CLIENT (Fases 1-2)                     ║
# ╚══════════════════════════════════════════════════════════════════╝


class MockPowerBIClient(PowerBIClientBase):
    """
    Cliente simulado que NO contacta la API de Microsoft.

    WHY: Permite desarrollar y probar todo el pipeline (LangGraph,
    validación semántica, generación DAX) sin licencia Power BI Pro.
    Simula latencia real (1s) y logea el payload exacto que se habría
    enviado, permitiendo validación manual del DAX en Power BI Desktop.
    """

    MOCK_DELAY_SECONDS: float = 1.0

    def _log_mock_operation(
        self,
        operation: str,
        payload: dict[str, Any],
    ) -> None:
        """Imprime un log visual detallado en consola para debugging."""
        timestamp = datetime.now(tz=timezone.utc).isoformat()
        separator = "═" * 60
        payload_json = json.dumps(payload, indent=2, ensure_ascii=False)

        log_message = (
            f"\n╔{separator}╗\n"
            f"║ 🧪 MOCK Power BI — {operation}\n"
            f"║ ⏱️  {timestamp}\n"
            f"╠{separator}╣\n"
            f"║ PAYLOAD QUE SE ENVIARÍA A api.powerbi.com:\n"
            f"{payload_json}\n"
            f"╚{separator}╝"
        )
        logger.info(log_message)
        # También imprimimos en stdout para visibilidad inmediata en dev
        print(log_message)  # noqa: T201

    async def execute_dax_query(
        self,
        dataset_id: str,
        dax_query: str,
    ) -> dict[str, Any]:
        """Simula la ejecución de una query DAX con respuesta exitosa."""
        self._log_mock_operation(
            operation="executeQueries",
            payload={
                "dataset_id": dataset_id,
                "queries": [{"query": dax_query}],
                "target_endpoint": (
                    f"https://api.powerbi.com/v1.0/myorg/datasets/"
                    f"{dataset_id}/executeQueries"
                ),
            },
        )

        await asyncio.sleep(self.MOCK_DELAY_SECONDS)

        return {
            "status": "ok",
            "mock": True,
            "results": [
                {
                    "tables": [
                        {
                            "rows": [
                                {"[Resultado Mock]": "Valor simulado — "
                                 "valida este DAX en Power BI Desktop"},
                            ],
                        },
                    ],
                },
            ],
            "dax_submitted": dax_query,
        }

    async def generate_embed_token(
        self,
        report_id: str,
        workspace_id: str,
    ) -> dict[str, Any]:
        """Simula la generación de un Embed Token."""
        self._log_mock_operation(
            operation="generateToken",
            payload={
                "report_id": report_id,
                "workspace_id": workspace_id,
                "access_level": "View",
                "target_endpoint": (
                    f"https://api.powerbi.com/v1.0/myorg/groups/"
                    f"{workspace_id}/reports/{report_id}/GenerateToken"
                ),
            },
        )

        await asyncio.sleep(self.MOCK_DELAY_SECONDS)

        return {
            "status": "ok",
            "mock": True,
            "token": "mock-embed-token-for-development-only",
            "token_id": "mock-token-id",
            "expiration": "2099-12-31T23:59:59Z",
        }

    async def get_report_pages(
        self,
        report_id: str,
        workspace_id: str,
    ) -> list[dict[str, Any]]:
        """Simula la obtención de páginas del reporte."""
        self._log_mock_operation(
            operation="getPages",
            payload={
                "report_id": report_id,
                "workspace_id": workspace_id,
                "target_endpoint": (
                    f"https://api.powerbi.com/v1.0/myorg/groups/"
                    f"{workspace_id}/reports/{report_id}/pages"
                ),
            },
        )

        await asyncio.sleep(self.MOCK_DELAY_SECONDS)

        return [
            {"name": "ReportSection1", "displayName": "Resumen Ejecutivo", "order": 0},
            {"name": "ReportSection2", "displayName": "Análisis de Ventas", "order": 1},
            {"name": "ReportSection3", "displayName": "KPIs Operativos", "order": 2},
        ]

    async def get_report_metadata(
        self,
        report_id: str,
        workspace_id: str,
    ) -> dict[str, Any]:
        """Simula la obtención de metadata del reporte."""
        self._log_mock_operation(
            operation="getReport",
            payload={
                "report_id": report_id,
                "workspace_id": workspace_id,
                "target_endpoint": (
                    f"https://api.powerbi.com/v1.0/myorg/groups/"
                    f"{workspace_id}/reports/{report_id}"
                ),
            },
        )

        await asyncio.sleep(self.MOCK_DELAY_SECONDS)

        return {
            "status": "ok",
            "mock": True,
            "id": report_id,
            "name": "Mock Report — Ventas Demo",
            "datasetId": "mock-dataset-id-12345",
            "webUrl": "https://app.powerbi.com/mock",
        }


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    LIVE CLIENT (Fase 3+)                       ║
# ╚══════════════════════════════════════════════════════════════════╝


class LivePowerBIClient(PowerBIClientBase):
    """
    Cliente real que ejecuta peticiones HTTP contra api.powerbi.com.

    WHY: Separado del Mock para cumplir el Single Responsibility Principle.
    Este cliente se activa con PBI_API_MODE=LIVE en .env, requiere un
    access_token válido obtenido via PowerBIAuthManager.

    NOTA: Los métodos están estructurados pero lanzan NotImplementedError
    hasta la Fase 3 como protección contra ejecución accidental.
    """

    PBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"

    def __init__(self, access_token: str) -> None:
        """
        WHY: El token se inyecta en el constructor (Dependency Injection)
        en lugar de obtenerlo internamente. Esto permite testear el
        cliente con tokens mock y desacopla auth de operaciones.
        """
        self._access_token = access_token
        self._http_client = httpx.AsyncClient(
            base_url=self.PBI_API_BASE,
            headers={
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

    async def _request(
        self,
        method: str,
        endpoint: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Método HTTP centralizado con manejo de errores coherente.

        WHY: Un único punto de salida para todas las peticiones HTTP.
        Esto permite agregar retry logic, circuit breaker y logging
        en un solo lugar sin duplicar código en cada método.
        """
        response = await self._http_client.request(
            method=method,
            url=endpoint,
            json=payload,
        )

        if response.status_code >= 400:
            logger.error(
                "Power BI API error: %s %s -> %d: %s",
                method,
                endpoint,
                response.status_code,
                response.text,
            )
            return {
                "status": "error",
                "http_code": response.status_code,
                "detail": response.text,
            }

        return response.json()

    async def execute_dax_query(
        self,
        dataset_id: str,
        dax_query: str,
    ) -> dict[str, Any]:
        """Ejecuta DAX real contra el dataset via REST API."""
        endpoint = f"/datasets/{dataset_id}/executeQueries"
        payload = {
            "queries": [{"query": dax_query}],
            "serializerSettings": {"includeNulls": True},
        }
        return await self._request("POST", endpoint, payload)

    async def generate_embed_token(
        self,
        report_id: str,
        workspace_id: str,
    ) -> dict[str, Any]:
        """Genera un Embed Token real via REST API."""
        endpoint = (
            f"/groups/{workspace_id}/reports/{report_id}/GenerateToken"
        )
        payload = {"accessLevel": "View"}
        return await self._request("POST", endpoint, payload)

    async def get_report_pages(
        self,
        report_id: str,
        workspace_id: str,
    ) -> list[dict[str, Any]]:
        """Obtiene las páginas reales del reporte."""
        endpoint = f"/groups/{workspace_id}/reports/{report_id}/pages"
        result = await self._request("GET", endpoint)
        return result.get("value", [])

    async def get_report_metadata(
        self,
        report_id: str,
        workspace_id: str,
    ) -> dict[str, Any]:
        """Obtiene la metadata real del reporte."""
        endpoint = f"/groups/{workspace_id}/reports/{report_id}"
        return await self._request("GET", endpoint)

    async def close(self) -> None:
        """Cierra el cliente HTTP. Llamar al finalizar el ciclo de vida."""
        await self._http_client.aclose()


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    FACTORY FUNCTION                            ║
# ╚══════════════════════════════════════════════════════════════════╝


def get_power_bi_client(access_token: str | None = None) -> PowerBIClientBase:
    """
    Factory que retorna el cliente correcto según PBI_API_MODE.

    WHY: El resto de la aplicación llama a esta función sin saber
    qué implementación existe detrás. Esto es inversión de dependencias
    (Dependency Inversion Principle) — los módulos de alto nivel
    (LangGraph, Router) no dependen de los detalles (Mock vs Live).

    Args:
        access_token: Token de Azure AD. Requerido en modo LIVE,
                      ignorado en modo MOCK.

    Returns:
        Instancia del cliente de Power BI apropiado.

    Raises:
        ValueError: Si modo LIVE y no se proporciona access_token.
    """
    if settings.pbi_api_mode == "MOCK":
        logger.info("🧪 Power BI Client inicializado en modo MOCK")
        return MockPowerBIClient()

    if settings.pbi_api_mode == "LIVE":
        if not access_token:
            raise ValueError(
                "PBI_API_MODE=LIVE requiere un access_token válido. "
                "Obtén uno via PowerBIAuthManager.acquire_token()."
            )
        logger.info("🔴 Power BI Client inicializado en modo LIVE")
        return LivePowerBIClient(access_token=access_token)

    # WHY: Este branch es inalcanzable gracias al Literal type en config,
    # pero lo dejamos como defensa en profundidad.
    raise ValueError(f"PBI_API_MODE inválido: {settings.pbi_api_mode}")
