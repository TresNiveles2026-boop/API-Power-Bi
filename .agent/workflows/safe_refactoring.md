---
description: Protocolo de Seguridad para Modificaciones de Código (Anti-Regresión)
---

# 🛡️ Protocolo de Desarrollo Seguro (Safe Changes)

Este workflow asegura que cualquier cambio o refactorización mantenga la integridad del sistema existente. Úsalo siempre que vayas a optimizar o refactorizar código crítico.

## 1. 🛑 Pre-Flight Check (Línea Base)
**Objetivo:** Confirmar que el sistema funciona ANTES de tocar nada.

1. Ejecutar todos los tests existentes:
   ```bash
   python -m pytest tests/ -v
   ```
   > **REGLA DE ORO:** Si los tests fallan ahora, 🛑 NO CONTINUAR. Arreglar el estado actual primero.

## 2. 🧠 Planificación de Impacto
**Objetivo:** Entender qué se va a romper.

1. Identificar módulos afectados.
2. Si se cambia la firma de una función, buscar referencias:
   ```bash
   grep -r "nombre_funcion" app/
   ```
3. Verificar si se requiere actualizar modelos Pydantic (`app/models/schemas.py`).

## 3. ⚡ Implementación Atómica
**Objetivo:** Cambios pequeños y reversibles.

1. Realizar cambios incrementales.
2. **NO** mezclar refactorización con nuevas funcionalidades.
3. Mantener la compatibilidad hacia atrás siempre que sea posible.

## 4. 🔍 Verificación Post-Cambio
**Objetivo:** Confirmar que no hubo regresiones.

// turbo
1. Ejecutar tests de nuevo:
   ```bash
   python -m pytest tests/ -v
   ```
   - Si fallan: 🛑 STOP. Revertir o arreglar inmediatamente.

## 5. 🧪 Nuevos Tests
**Objetivo:** Cubrir el nuevo código.

1. Si agregaste una nueva funcionalidad, crea un test específico en `tests/`.
2. Si arreglaste un bug, crea un test que reproduzca el bug (Test de Regresión).

## 6. ✅ Aprobación Final
1. Confirmar que el código cumple con los estándares del proyecto (Clean Code, SOLID).
2. Documentar cualquier cambio en la API (Swagger).
