"""
Supabase Client — Conexión centralizada con service_role_key.

WHY: Usamos el service_role_key (no el anon_key) porque nuestra API
actúa como backend trusted. El service_role bypasea RLS, lo cual es
necesario porque la lógica de filtrado por tenant_id la hacemos
explícitamente en cada query (defensa en código) + RLS como segunda
línea de defensa para accesos directos no autorizados.

DECISIÓN: Usamos el cliente sync de supabase-py envuelto en funciones
async porque el SDK oficial de Python no tiene un cliente async nativo
maduro. Las operaciones de DB son rápidas (<50ms) y no bloquean el
event loop de forma significativa.
"""

from __future__ import annotations

import logging
from functools import lru_cache

from supabase import Client, create_client

from app.core.config import settings

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_supabase_client() -> Client:
    """
    Crea y cachea una instancia del cliente Supabase.

    WHY: Singleton via lru_cache. Crear un cliente por request es
    innecesario y desperdicia conexiones. Una sola instancia se
    reutiliza durante toda la vida del proceso.

    Returns:
        Cliente Supabase autenticado con service_role_key.

    Raises:
        ValueError: Si las credenciales de Supabase no están configuradas.
    """
    if not settings.supabase_url or not settings.supabase_service_role_key:
        raise ValueError(
            "SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY son obligatorios. "
            "Configúralos en .env"
        )

    client = create_client(
        supabase_url=settings.supabase_url,
        supabase_key=settings.supabase_service_role_key,
    )

    logger.info("✅ Cliente Supabase inicializado para: %s", settings.supabase_url)
    return client
