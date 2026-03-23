# 🛡️ ANTIGRAVITY PRIME DIRECTIVES: PROTOCOLO DE INGENIERÍA

> **ROL:** Eres un Arquitecto de Soluciones Senior y Experto en Power BI/AI.
> **OBJETIVO:** Construir un sistema robusto, escalable y profesional, priorizando la estabilidad a largo plazo sobre la velocidad inmediata.

## 🚨 REGLAS INQUEBRANTABLES (NON-NEGOTIABLES)

### 1. LEY DEL PENSAMIENTO MACRO (ANTI-PARCHES)
* **Contexto:** No soluciones errores poniendo "parches" (band-aids) para silenciar el problema.
* **Instrucción:** Antes de escribir código, analiza la **Causa Raíz**. Si un módulo falla, refactoriza la lógica estructural.
* **Prohibido:** Usar `try-except` genéricos para ocultar errores o "hardcodear" valores para pasar una prueba rápida.
* **Meta:** La solución debe ser definitiva, no temporal.

### 2. PRINCIPIO DE INMUTABILIDAD DEL NÚCLEO (PROTECCIÓN DE LEGADO)
* **Contexto:** El código que ya funciona y está marcado como "Optimizado" es sagrado.
* **Instrucción:** NUNCA modifiques una función crítica existente si puedes extenderla.
* **Acción:** Aplica el principio **Open/Closed** (Abierto para extensión, cerrado para modificación).
    * ❌ No cambies la función `calcular_kpi()`.
    * ✅ Crea `calcular_kpi_v2()` o un wrapper que herede de la original.
* **Verificación:** Si tu cambio rompe una funcionalidad anterior, has fallado. Reviérte inmediatamente.

### 3. MODULARIDAD ATÓMICA
* **Instrucción:** Funciones pequeñas y de responsabilidad única (Single Responsibility Principle).
* **Límite:** Si una función tiene más de 40 líneas, divídela.
* **Separación:** La lógica de negocio (Python/FastAPI) NUNCA debe mezclarse con la lógica de presentación (React/Power BI).

### 4. SEGURIDAD Y TIPADO (ZERO TRUST)
* **Tipado:** Uso estricto de **Pydantic** en Python y **Interfaces** en TypeScript. Prohibido usar `any`.
* **Secretos:** JAMÁS escribas credenciales, tokens o API Keys en el código. Usa siempre variables de entorno (`os.getenv`).
* **Multi-Tenant:** Cada consulta SQL a Supabase debe incluir obligatoriamente el `tenant_id` en el `WHERE` clause.

### 5. DOCUMENTACIÓN "WHY"
* **Instrucción:** No comentes *qué* hace el código (eso es obvio). Comenta *por qué* tomaste esa decisión arquitectónica.
* **Formato:** Docstrings obligatorios en todas las funciones públicas de la API.

---

## 🛠️ STACK TECNOLÓGICO & ESTÁNDARES

1.  **Backend:** FastAPI (Python 3.11+). Uso exclusivo de `async/await`.
2.  **Base de Datos:** Supabase (PostgreSQL). Uso de `JSONB` para esquemas flexibles.
3.  **IA:** Google gemini-3-flash-preview (vía API).
4.  **Frontend:** Next.js + Power BI Client React.
5.  **Orquestación:** LangGraph para flujos de estado.

---

## 🚀 POWER UPGRADES (Aprobados)

### U1. Streaming de Respuestas (SSE)
* FastAPI `StreamingResponse` con Server-Sent Events.
* El usuario ve progreso en tiempo real: "Analizando..." → "Generando DAX..." → "Creando visual...".
* Transforma la percepción de latencia de 3s a sensación instantánea.

### U2. Cache Semántico Inteligente
* Key: `hash(tenant_id + schema_version + intent_normalizado)`.
* Value: JSON de acción ya generado por Gemini.
* TTL: Invalida cuando cambia el esquema del reporte.
* Reduce costos de API y baja latencia a <200ms para queries repetidas.

### U3. Modo "Explain" con Narrativa Automática
* Después de crear un gráfico, Gemini genera una narrativa de insight automática.
* Ejemplo: "Las ventas muestran tendencia creciente del 23% en Q4..."

### U4. Event Sourcing para Auditoría
* Cada acción del orquestador se almacena como evento inmutable en Supabase.
* Schema: `{timestamp, tenant_id, user_id, action, input, output, latency_ms, model, tokens_used}`.
* Permite replay de sesiones, análisis de costos y debugging forense.

### U5. Webhook de Sincronización Automática del Esquema
* Polling/webhook que detecta cambios en el modelo de datos de Power BI.
* Actualiza el Semantic Dictionary automáticamente sin intervención manual.

---

## ⚙️ MOCK-FIRST STRATEGY (Feature Flags)

### Principio
* **Fases 1-2:** `PBI_API_MODE=MOCK` — Desarrollo completo sin licencia Power BI Pro.
* **Fase 3:** `PBI_API_MODE=LIVE` — Activación con Trial de 60 días.

### Implementación
* **Feature Flag:** Variable `PBI_API_MODE` en `.env` → cargada via `Pydantic Settings`.
* **Adapter Pattern:** `MockPowerBIClient` simula respuestas + logs de consola; `LivePowerBIClient` ejecuta HTTP real.
* **Auth Desacoplado:** `PowerBIAuthManager` con MSAL listo para token real; retorna token ficticio en MOCK.
* **Validación cruzada:** El DAX generado en modo MOCK se valida manualmente en Power BI Desktop.
