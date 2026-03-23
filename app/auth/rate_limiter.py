"""
Rate Limiter — Sliding window rate limiter per tenant.

WHY: Sin rate limiting, un solo cliente puede saturar la API con
requests masivos, agotando los recursos de Gemini y Azure.
Este módulo implementa un sliding window counter en memoria
que limita los requests por tenant por minuto.

DECISIÓN: In-memory en lugar de Redis porque en esta etapa no hay
múltiples instancias del backend. Si se escala horizontalmente,
migrar a Redis con la misma interfaz.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field

from fastapi import HTTPException, status

from app.core.config import settings

logger = logging.getLogger(__name__)


@dataclass
class RateLimitConfig:
    """Configuración de límite por tipo de endpoint."""

    max_requests: int
    window_seconds: int = 60


# ── Default limits per endpoint category ──────────────────────
RATE_LIMITS: dict[str, RateLimitConfig] = {
    "chat": RateLimitConfig(max_requests=30, window_seconds=60),
    "embed": RateLimitConfig(max_requests=10, window_seconds=60),
    "default": RateLimitConfig(max_requests=60, window_seconds=60),
}


class SlidingWindowRateLimiter:
    """
    In-memory sliding window rate limiter.

    WHY: El sliding window es más justo que un fixed window porque
    no permite ráfagas al inicio de cada ventana. Almacena timestamps
    de requests recientes y cuenta cuántos caen en la ventana actual.
    """

    def __init__(self) -> None:
        # tenant_id:category → list of timestamps
        self._requests: dict[str, list[float]] = defaultdict(list)

    def _cleanup_old(self, key: str, window_seconds: int) -> None:
        """Elimina timestamps fuera de la ventana actual."""
        cutoff = time.time() - window_seconds
        self._requests[key] = [
            ts for ts in self._requests[key] if ts > cutoff
        ]

    def check(self, tenant_id: str, category: str = "default") -> None:
        """
        Verifica si el tenant puede hacer otro request.

        Args:
            tenant_id: UUID del tenant.
            category: Tipo de endpoint (chat, embed, default).

        Raises:
            HTTPException 429 si se excede el límite.
        """
        if not settings.rate_limit_enabled:
            return

        config = RATE_LIMITS.get(category, RATE_LIMITS["default"])
        key = f"{tenant_id}:{category}"

        # Limpiar timestamps viejos
        self._cleanup_old(key, config.window_seconds)

        # Verificar límite
        current_count = len(self._requests[key])
        if current_count >= config.max_requests:
            logger.warning(
                "⚠️ Rate limit exceeded: tenant=%s category=%s count=%d/%d",
                tenant_id,
                category,
                current_count,
                config.max_requests,
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=(
                    f"Rate limit exceeded: {config.max_requests} requests "
                    f"per {config.window_seconds}s for {category}. "
                    f"Please wait and try again."
                ),
                headers={
                    "Retry-After": str(config.window_seconds),
                    "X-RateLimit-Limit": str(config.max_requests),
                    "X-RateLimit-Remaining": "0",
                },
            )

        # Registrar el request
        self._requests[key].append(time.time())

        remaining = config.max_requests - current_count - 1
        logger.debug(
            "📊 Rate limit: tenant=%s category=%s %d/%d remaining",
            tenant_id,
            category,
            remaining,
            config.max_requests,
        )


# Singleton — se reutiliza en toda la aplicación
rate_limiter = SlidingWindowRateLimiter()
