"""
AI-BI Orchestrator — FastAPI Entry Point.

WHY: Este es el punto de entrada de la aplicación. Registra todos los
routers, configura CORS, y expone la documentación Swagger automática.

Phase 4: Incluye global exception handlers para garantizar que la API
NUNCA devuelva un 500 crudo con stack trace. Todo error se captura y
se devuelve como JSON amigable.

La aplicación se levanta con:
    uvicorn app.main:app --reload --port 8002
"""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.v1.discovery import router as discovery_router
from app.api.v1.routes import router as v1_router
from app.core.config import settings

# Configurar logging global
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="AI-BI Orchestrator",
    description=(
        "Capa de Interoperabilidad Agéntica entre lenguaje natural "
        "y Microsoft Power BI. Genera visualizaciones y DAX en tiempo real."
    ),
    version="0.6.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# WHY: CORS configurable. En desarrollo, permite localhost:3002.
# En producción, se configura via CORS_ORIGINS en .env.
_cors_origins: list[str] = []
if settings.cors_origins:
    _cors_origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
elif settings.app_debug:
    _cors_origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ╔══════════════════════════════════════════════════════════════════╗
# ║              GLOBAL EXCEPTION HANDLERS (Phase 4)               ║
# ╚══════════════════════════════════════════════════════════════════╝


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Captura CUALQUIER excepción no manejada y devuelve JSON amigable.

    WHY: La regla de oro del plan dice "La API nunca debe devolver un
    Internal Server Error (500) al frontend". Este handler garantiza
    que incluso errores inesperados se devuelven como JSON estructurado
    con un mensaje útil, no un stack trace crudo.
    """
    # Importar aquí para evitar circular imports
    from app.ai.gemini_client import GeminiExhaustedError, GeminiTimeoutError

    # ── Gemini Timeout → HTTP 504 ────────────────────────────────
    if isinstance(exc, GeminiTimeoutError):
        logger.warning("⏰ Gemini timeout capturado: %s", exc)
        return JSONResponse(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            content={
                "detail": (
                    "La IA tardó demasiado en responder. "
                    "Esto puede pasar en horarios de alta demanda. "
                    "Intenta de nuevo en unos segundos."
                ),
                "error_type": "GEMINI_TIMEOUT",
            },
        )

    # ── Gemini Exhausted → HTTP 503 ──────────────────────────────
    if isinstance(exc, GeminiExhaustedError):
        logger.error("🔄 Gemini reintentos agotados: %s", exc)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "detail": (
                    "El servicio de IA no está disponible temporalmente. "
                    "Se intentó múltiples veces sin éxito. "
                    "Intenta de nuevo en un momento."
                ),
                "error_type": "GEMINI_EXHAUSTED",
            },
        )

    # ── Cualquier otro error → HTTP 500 con mensaje amigable ─────
    logger.error(
        "💥 Error no manejado en %s %s: %s",
        request.method,
        request.url.path,
        exc,
        exc_info=True,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": (
                "Ocurrió un error inesperado procesando tu solicitud. "
                "Nuestro equipo ha sido notificado. "
                "Intenta de nuevo o contacta soporte si el problema persiste."
            ),
            "error_type": "INTERNAL_ERROR",
        },
    )


# Registrar routers
app.include_router(v1_router)
app.include_router(discovery_router)


@app.get("/health", tags=["System"])
async def health_check() -> dict[str, str]:
    """
    Health check endpoint.

    WHY: Necesario para que cualquier orquestador (Docker, K8s, load balancer)
    pueda verificar que el servicio está vivo. Incluye el modo PBI para
    diagnosticar rápidamente en qué modo está corriendo la instancia.
    """
    return {
        "status": "healthy",
        "pbi_mode": settings.pbi_api_mode,
        "environment": settings.app_env,
        "auth_required": str(settings.api_key_required),
    }
