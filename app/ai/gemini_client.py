"""
Gemini Client — Wrapper del SDK de Google AI con salida estructurada.

WHY: Encapsulamos las llamadas a Gemini en un cliente dedicado para:
1. Centralizar la configuración del modelo (temperature, tokens, etc.)
2. Parsear la salida JSON de forma robusta con fallback a regex
3. Loguear tokens usados para audit_events (Power Upgrade U4)
4. Timeout y retry con backoff para resiliencia de producción (Phase 4)

DECISIÓN: Usamos google-generativeai SDK en lugar de llamadas HTTP
directas porque el SDK maneja automáticamente retry, rate limiting,
y streaming. Es la librería oficial mantenida por Google.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from threading import Lock
from typing import Any

import google.generativeai as genai

from app.core.config import settings

logger = logging.getLogger(__name__)

# ── Custom Exceptions ─────────────────────────────────────────
# WHY: Excepciones tipadas permiten que main.py devuelva HTTP codes
# específicos (504 para timeout, 503 para reintentos agotados) en
# lugar de un genérico 500.


class GeminiTimeoutError(Exception):
    """Gemini no respondió dentro del tiempo límite."""
    pass


class GeminiExhaustedError(Exception):
    """Se agotaron los reintentos a Gemini."""
    pass


class GeminiParseError(Exception):
    """Gemini respondió, pero no devolvió JSON parseable."""
    pass


class GeminiConfigError(Exception):
    """Gemini no está configurado (API key faltante u otra config inválida)."""


# ── Configuration ─────────────────────────────────────────────

GEMINI_TIMEOUT_SECONDS = 45
GEMINI_MAX_RETRIES = 3
GEMINI_BACKOFF_BASE = 1  # seconds: 1s → 2s → 4s

_genai_configured = False
_genai_config_lock = Lock()


def _configure_genai() -> None:
    """
    Configura el SDK de Google AI con la API key.

    WHY: La configuración es global (a nivel de módulo), no por
    instancia. Esto es un requisito del SDK de Google: configure()
    se llama una vez y aplica a todas las llamadas subsecuentes.
    """
    if not settings.google_ai_api_key:
        raise GeminiConfigError(
            "GOOGLE_AI_API_KEY no está configurada en .env. "
            "Obtén una en https://aistudio.google.com"
        )
    genai.configure(api_key=settings.google_ai_api_key)


def ensure_genai_configured() -> None:
    """
    Configura Gemini de forma perezosa (lazy).

    WHY: En Cloud Run el contenedor debe poder arrancar y exponer /health aun si
    la API key no está configurada. La validación/errores se manejan al invocar
    /chat, no al importar el módulo.
    """
    global _genai_configured
    if _genai_configured:
        return
    with _genai_config_lock:
        if _genai_configured:
            return
        _configure_genai()
        _genai_configured = True


def _extract_json_from_text(text: str) -> Any:
    """
    Extrae JSON de la respuesta de Gemini, incluso si viene envuelto en markdown.

    WHY: Aunque pedimos response_mime_type="application/json", Gemini
    a veces envuelve la respuesta en ```json ... ```. Este parser
    maneja ambos casos de forma robusta sin fallar.
    """
    # Intento 1: Parsear directamente
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Intento 2: Extraer de bloque markdown ```json ... ```
    json_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass

    # Intento 3: Buscar el primer { ... } en el texto
    brace_match = re.search(r"\{.*\}", text, re.DOTALL)
    if brace_match:
        try:
            return json.loads(brace_match.group(0))
        except json.JSONDecodeError:
            pass

    logger.error("❌ No se pudo extraer JSON de la respuesta: %s", text[:200])
    return {"error": "No se pudo parsear la respuesta de Gemini", "raw": text[:500]}


def _coerce_result_to_dict(payload: Any) -> dict[str, Any]:
    """
    Normaliza payloads JSON de Gemini a dict.

    WHY: Gemini puede devolver listas JSON válidas. El orquestador espera
    dict, por lo que esta función evita fallos tipo `list indices must be integers`.
    """
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        return {"actions": payload}
    return {"error": "Formato de respuesta no soportado", "raw": str(payload)[:500]}


def _is_retryable_error(exc: Exception) -> bool:
    """
    Determina si un error de Gemini es transitorio y merece reintento.

    WHY: No todos los errores justifican retry. Un API key inválido
    (401) no se va a arreglar solo. Pero un 429 (rate limit) o 503
    (servicio no disponible) sí son transitorios.
    """
    error_str = str(exc).lower()
    retryable_patterns = [
        "429",           # Rate limit
        "500",           # Internal server error
        "503",           # Service unavailable
        "resource exhausted",
        "deadline exceeded",
        "unavailable",
        "internal",
        "overloaded",
    ]
    return any(pattern in error_str for pattern in retryable_patterns)


async def call_gemini(
    system_prompt: str,
    user_message: str,
    temperature: float = 0.2,
    *,
    timeout_seconds: int | None = None,
    max_retries: int | None = None,
    required_keys: set[str] | None = None,
) -> dict[str, Any]:
    """
    Llama a Gemini con structured output, timeout y retry con backoff.

    WHY: Temperature baja (0.2) porque queremos respuestas deterministas
    y consistentes para la generación de DAX. No queremos "creatividad"
    en código que debe ser sintácticamente correcto.

    Phase 4: Ahora incluye:
    - Timeout de 45s para evitar requests colgados
    - Retry con backoff exponencial (1s, 2s, 4s) para errores transitorios
    - Excepciones tipadas para manejo diferenciado en main.py

    Args:
        system_prompt: Prompt del sistema con rol + diccionario semántico.
        user_message: Mensaje del usuario en lenguaje natural.
        temperature: Control de aleatoriedad (0.0 = determinista, 1.0 = creativo).

    Returns:
        Dict con la respuesta de Gemini parseada desde JSON.

    Raises:
        GeminiTimeoutError: Si Gemini no responde en 45s.
        GeminiExhaustedError: Si se agotan los reintentos.
    """
    timeout_limit = int(timeout_seconds or GEMINI_TIMEOUT_SECONDS)
    retries_limit = int(max_retries or GEMINI_MAX_RETRIES)

    ensure_genai_configured()

    model = genai.GenerativeModel(
        model_name=settings.gemini_model,
        system_instruction=system_prompt,
        generation_config=genai.GenerationConfig(
            temperature=temperature,
            response_mime_type="application/json",
        ),
    )

    logger.info(
        "🤖 Llamando a Gemini (%s) | Temperature: %.1f | Mensaje: %.80s...",
        settings.gemini_model,
        temperature,
        user_message,
    )

    last_exception: Exception | None = None
    guarded_message = user_message

    for attempt in range(retries_limit):
        try:
            # WHY: asyncio.wait_for() cancela la coroutine si excede el
            # timeout. Sin esto, un Gemini lento bloquea el worker de
            # uvicorn indefinidamente.
            response = await asyncio.wait_for(
                asyncio.to_thread(model.generate_content, guarded_message),
                timeout=timeout_limit,
            )

            # Extraer texto de la respuesta
            response_text = response.text if response.text else ""

            # Loguear uso de tokens para audit
            token_info = {
                "prompt_tokens": getattr(response.usage_metadata, "prompt_token_count", 0),
                "completion_tokens": getattr(response.usage_metadata, "candidates_token_count", 0),
                "total_tokens": getattr(response.usage_metadata, "total_token_count", 0),
            }
            logger.info("📊 Tokens usados: %s (intento %d)", token_info, attempt + 1)

            # Parsear JSON
            parsed = _extract_json_from_text(response_text)
            if isinstance(parsed, dict) and parsed.get("error") == "No se pudo parsear la respuesta de Gemini":
                raise GeminiParseError(str(parsed.get("raw") or "")[:500])
            result = _coerce_result_to_dict(parsed)
            if required_keys:
                missing = [k for k in required_keys if k not in result]
                if missing:
                    raise GeminiParseError(
                        f"Faltan claves requeridas {missing}. Raw: {response_text[:500]}"
                    )
            result["_token_usage"] = token_info

            return result

        except asyncio.TimeoutError:
            logger.warning(
                "⏰ Gemini timeout (intento %d/%d, límite: %ds)",
                attempt + 1,
                retries_limit,
                timeout_limit,
            )
            last_exception = GeminiTimeoutError(
                f"Gemini no respondió en {timeout_limit}s"
            )
            # Timeouts siempre se reintentan

        except GeminiParseError as exc:
            logger.warning(
                "🧩 Gemini devolvió salida no parseable (intento %d/%d). Forzando JSON estricto.",
                attempt + 1,
                retries_limit,
            )
            last_exception = exc
            # Siguiente intento: refuerza formato sin cambiar intención.
            keys_hint = ""
            if required_keys:
                keys_hint = (
                    " Debes incluir exactamente estas claves obligatorias: "
                    + ", ".join(sorted(required_keys))
                    + "."
                )
            guarded_message = (
                "IMPORTANTE: responde SOLO con JSON válido (sin texto extra, sin markdown). "
                f"Si no puedes, responde con un JSON de error.{keys_hint}\n\n"
                f"{user_message}"
            )

        except Exception as exc:
            last_exception = exc
            if not _is_retryable_error(exc):
                logger.error("❌ Error no recuperable de Gemini: %s", exc)
                raise
            logger.warning(
                "⚠️ Error transitorio de Gemini (intento %d/%d): %s",
                attempt + 1,
                GEMINI_MAX_RETRIES,
                exc,
            )

        # Backoff exponencial antes del siguiente reintento
        if attempt < retries_limit - 1:
            wait_time = GEMINI_BACKOFF_BASE * (2 ** attempt)
            logger.info("⏳ Esperando %ds antes de reintentar...", wait_time)
            await asyncio.sleep(wait_time)

    # Si llegamos aquí, todos los reintentos fallaron
    raise GeminiExhaustedError(
        f"Gemini falló después de {retries_limit} intentos. "
        f"Último error: {last_exception}"
    )
