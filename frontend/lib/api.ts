/**
 * API Client — Wrapper de fetch para comunicarse con el backend FastAPI.
 *
 * WHY: Centralizar las llamadas HTTP en un solo lugar para:
 * 1. Mantener la URL base en un solo sitio.
 * 2. Tipado automático con generics.
 * 3. Manejo de errores consistente con mensajes amigables.
 * 4. Timeout automático con AbortController (Phase 4).
 */

import type { ChatRequest, ChatResponse, Conversation } from "./types";

const RAW_API_BASE = process.env.NEXT_PUBLIC_API_URL || "";
// En browser preferimos same-origin (Next rewrites/proxy) para evitar CORS.
const API_BASE = typeof window === "undefined" ? RAW_API_BASE : "";
const API_KEY = process.env.NEXT_PUBLIC_API_KEY || "";

// WHY: El ciclo LangGraph puede tardar >40s en LIVE mode.
// Usamos 120s para evitar cortes prematuros en frontend/proxy.
const DEFAULT_TIMEOUT_MS = 120_000;

// ── Custom Error Types ──────────────────────────────────────

export class ApiTimeoutError extends Error {
    constructor(message = "La solicitud tardó demasiado. Intenta de nuevo.") {
        super(message);
        this.name = "ApiTimeoutError";
    }
}

export class ApiRateLimitError extends Error {
    retryAfter: number;
    constructor(retryAfter: number = 60) {
        super(
            `Has alcanzado el límite de solicitudes. Espera ${retryAfter} segundos.`
        );
        this.name = "ApiRateLimitError";
        this.retryAfter = retryAfter;
    }
}

export class ApiConnectionError extends Error {
    constructor(
        message = "No se pudo conectar con el servidor. Verifica tu conexión."
    ) {
        super(message);
        this.name = "ApiConnectionError";
    }
}

export class ApiServerError extends Error {
    errorType: string;
    constructor(message: string, errorType: string = "UNKNOWN") {
        super(message);
        this.name = "ApiServerError";
        this.errorType = errorType;
    }
}

export type UploadPbitResult = {
    status: string;
    report_id?: string;
    tenant_id?: string;
    filename?: string;
    columns_synced?: number;
    message?: string;
};

// ── Core Fetch ──────────────────────────────────────────────

async function apiFetch<T>(
    endpoint: string,
    options: RequestInit = {},
    timeoutMs: number = DEFAULT_TIMEOUT_MS
): Promise<T> {
    const url = `${API_BASE}${endpoint}`;

    // Phase 4: AbortController for timeout
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

    const headers: Record<string, string> = {
        "Content-Type": "application/json",
        ...(options.headers as Record<string, string>),
    };

    // Phase 5: Include API key if configured
    if (API_KEY) {
        headers["X-API-Key"] = API_KEY;
    }

    try {
        const response = await fetch(url, {
            ...options,
            headers,
            signal: controller.signal,
        });

        // ── HTTP 429: Rate Limit ────────────────────────────────
        if (response.status === 429) {
            const retryAfter = parseInt(
                response.headers.get("Retry-After") || "60",
                10
            );
            throw new ApiRateLimitError(retryAfter);
        }

        // ── HTTP 504: Gateway Timeout ───────────────────────────
        if (response.status === 504) {
            const body = await response.json().catch(() => ({}));
            throw new ApiTimeoutError(
                body.detail ||
                "La IA tardó demasiado en responder. Intenta de nuevo."
            );
        }

        // ── HTTP 503: Service Unavailable ───────────────────────
        if (response.status === 503) {
            const body = await response.json().catch(() => ({}));
            throw new ApiServerError(
                body.detail ||
                "El servicio no está disponible temporalmente.",
                body.error_type || "SERVICE_UNAVAILABLE"
            );
        }

        // ── Other errors ────────────────────────────────────────
        if (!response.ok) {
            const error = await response
                .json()
                .catch(() => ({ detail: "Error desconocido" }));
            throw new ApiServerError(
                error.detail || `Error HTTP ${response.status}`,
                error.error_type || "HTTP_ERROR"
            );
        }

        return response.json();
    } catch (error) {
        // ── AbortController timeout ─────────────────────────────
        if (error instanceof DOMException && error.name === "AbortError") {
            throw new ApiTimeoutError();
        }

        // ── Network error (offline, DNS, CORS) ──────────────────
        if (error instanceof TypeError && error.message.includes("fetch")) {
            throw new ApiConnectionError();
        }

        // Re-throw our custom errors
        throw error;
    } finally {
        clearTimeout(timeoutId);
    }
}

// ── Chat ─────────────────────────────────────────────────────

export async function sendChatMessage(
    request: ChatRequest
): Promise<ChatResponse> {
    return apiFetch<ChatResponse>("/api/v1/chat", {
        method: "POST",
        body: JSON.stringify(request),
    });
}

// ── History (Phase 6) ────────────────────────────────────────

export async function getConversations(): Promise<Conversation[]> {
    return apiFetch<Conversation[]>("/api/v1/conversations");
}

export async function getConversationMessages(
    conversationId: string
): Promise<any[]> {
    return apiFetch<any[]>(
        `/api/v1/conversations/${conversationId}/messages`
    );
}

export async function updateConversationTitle(
    conversationId: string,
    title: string
): Promise<void> {
    return apiFetch<void>(`/api/v1/conversations/${conversationId}`, {
        method: "PATCH",
        body: JSON.stringify({ title }),
    });
}

// ── Health ───────────────────────────────────────────────────

export async function checkHealth(): Promise<{
    status: string;
    pbi_mode: string;
}> {
    return apiFetch("/health", {}, 5_000); // 5s timeout for health check
}

// ── Dataset Upload (Phase 5) ────────────────────────────────

export interface UploadDatasetResult {
    status: string;
    report_id: string;
    target_table_name: string;
    tables: Array<{
        table_name: string;
        columns: Array<{
            column_name: string;
            data_type: string;
            sample_values: string[];
        }>;
        row_count: number;
    }>;
}

export interface SyncSchemaPowerBiResult {
    status: string;
    report_id: string;
    columns_synced: number;
    message: string;
    mode?: "full" | "operational";
    admin_blocked?: boolean;
    columns: Array<{
        table_name: string;
        column_name: string;
        data_type: string;
        is_measure?: boolean;
    }>;
}

export interface ColumnSchemaPayload {
    table_name: string;
    column_name: string;
    data_type: string;
    description?: string;
    is_measure?: boolean;
    dax_expression?: string;
    sample_values?: any[];
    metadata?: Record<string, any>;
}

export interface SyncSchemaResult {
    status: string;
    report_id: string;
    columns_synced: number;
    message: string;
}

export async function syncSchemaFromPowerBi(
    reportId: string,
    tenantId: string,
    powerbiAccessToken?: string
): Promise<SyncSchemaPowerBiResult> {
    return apiFetch<SyncSchemaPowerBiResult>("/api/v1/sync-schema-powerbi", {
        method: "POST",
        body: JSON.stringify({
            report_id: reportId,
            tenant_id: tenantId,
            powerbi_access_token: powerbiAccessToken || null,
        }),
    });
}

export async function syncSchema(
    reportId: string,
    tenantId: string,
    columns: ColumnSchemaPayload[]
): Promise<SyncSchemaResult> {
    return apiFetch<SyncSchemaResult>("/api/v1/sync-schema", {
        method: "POST",
        body: JSON.stringify({
            report_id: reportId,
            tenant_id: tenantId,
            columns,
        }),
    });
}

export async function uploadPbitTemplate(
    reportId: string,
    tenantId: string,
    file: File
): Promise<UploadPbitResult> {
    const url = `${API_BASE}/api/v1/upload-pbit`;
    const form = new FormData();
    form.append("report_id", reportId);
    form.append("tenant_id", tenantId);
    form.append("file", file, file.name);

    const headers: Record<string, string> = {};
    if (API_KEY) headers["X-API-Key"] = API_KEY;

    const response = await fetch(url, {
        method: "POST",
        body: form,
        headers,
    });

    if (!response.ok) {
        const body = await response.json().catch(() => ({ detail: "Error desconocido" }));
        throw new ApiServerError(body.detail || "No se pudo subir la plantilla .pbit", body.error_type || "UPLOAD_FAILED");
    }
    return response.json();
}

export async function uploadDataset(
    file: File,
    reportId: string,
    targetTableName: string
): Promise<UploadDatasetResult> {
    const url = `${API_BASE}/api/v1/upload-dataset`;

    const formData = new FormData();
    formData.append("file", file);
    formData.append("report_id", reportId);
    formData.append("target_table_name", targetTableName);

    const headers: Record<string, string> = {};
    if (API_KEY) {
        headers["X-API-Key"] = API_KEY;
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);

    try {
        const response = await fetch(url, {
            method: "POST",
            headers,
            body: formData,
            signal: controller.signal,
        });

        if (response.status === 429) {
            const retryAfter = parseInt(response.headers.get("Retry-After") || "60", 10);
            throw new ApiRateLimitError(retryAfter);
        }

        if (!response.ok) {
            const error = await response.json().catch(() => ({ detail: "Error desconocido" }));
            throw new ApiServerError(
                error.detail || `Error HTTP ${response.status}`,
                "UPLOAD_ERROR"
            );
        }

        return response.json();
    } catch (error) {
        if (error instanceof DOMException && error.name === "AbortError") {
            throw new ApiTimeoutError("La subida del archivo tardó demasiado.");
        }
        if (error instanceof TypeError && error.message.includes("fetch")) {
            throw new ApiConnectionError();
        }
        throw error;
    } finally {
        clearTimeout(timeoutId);
    }
}
