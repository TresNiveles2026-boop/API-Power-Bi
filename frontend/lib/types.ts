/**
 * TypeScript Interfaces — Mirror de los Pydantic models del backend.
 *
 * WHY: TypeScript interfaces garantizan tipo-seguridad en el frontend.
 * Cada interfaz aquí corresponde a un Pydantic model en el backend.
 */

// ── Chat ─────────────────────────────────────────────────────

export interface FilterConfig {
    table: string;
    column: string;
    operator: string;
    values: Array<string | number | boolean>;
}

export interface SuggestedVisual {
    description: string;
    visualType: string;
}

export interface DataRoleBinding {
    table?: string;
    column?: string;
    ref?: string;
    measure?: string;
    aggregation?: string;
}

export interface VisualLayout {
    x?: number | null;
    y?: number | null;
    width?: number | null;
    height?: number | null;
}

export interface VisualFormatting {
    title?: string | null;
    showLegend?: boolean | null;
    showDataLabels?: boolean | null;
    // Legacy key (retrocompatibilidad con payloads anteriores)
    theme?: string | null;
    // Legacy key (retrocompatibilidad con payloads anteriores)
    titleText?: string | null;
}

export interface TopNConfig {
    count: number;
    order_by_column: string;
    order_by_table: string;
    category_column: string;
    category_table: string;
    direction: "Top" | "Bottom";
}

export interface VisualAction {
    operation: string;
    visualType?: string | null;
    title?: string | null;
    dataRoles?: Record<string, string | DataRoleBinding> | null;
    dax?: string | null;
    dax_name?: string | null;
    targetVisualName?: string | null;
    layout?: VisualLayout | null;
    layout_intent?: string | null;
    format?: VisualFormatting | null;
    filters?: FilterConfig[] | null;
    target_page?: string | null;
    explanation?: string | null;
    suggested_visuals?: SuggestedVisual[] | null;
    follow_up_questions?: string[] | null;
    error_code?: string | null;
    query_type?: string | null;
    payload?: Record<string, any> | null;
    top_n?: TopNConfig | null;
}

// ── Messages ─────────────────────────────────────────────────

export interface ChatMessage {
    id: string;
    role: "user" | "assistant";
    content: string;
    timestamp: Date;
    action?: VisualAction;
    intent?: string;
    confidence?: number;
    isLoading?: boolean;
    isError?: boolean;
    failedMessage?: string;
    rescueCta?: boolean;
}

export interface Conversation {
    id: string;
    title: string;
    tenant_id: string;
    report_id: string;
    updated_at: string;
    created_at: string;
}

export interface VisualContextItem {
    id: string;
    type: string;
    title: string;
    page?: string;
}

export interface ChatRequest {
    message: string;
    report_id: string;
    tenant_id: string;
    conversation_id?: string;
    visual_context?: VisualContextItem[];
}

export interface ChatResponse {
    status: string;
    action: VisualAction;
    actions?: VisualAction[];
    intent: string;
    confidence: number;
    retries_used: number;
    conversation_id?: string;
}

// ── Report Config ────────────────────────────────────────────

export interface EmbedConfig {
    mode: "MOCK" | "LIVE";
    reportId: string;
    embedUrl: string;
    accessToken: string;
    tokenType: string;
    tokenExpiration: string;
    permissions: string;
    message?: string;
}

// ── Visual Type Icons ────────────────────────────────────────

export const VISUAL_TYPE_LABELS: Record<string, { label: string; icon: string }> = {
    barChart: { label: "Gráfico de Barras", icon: "📊" },
    columnChart: { label: "Gráfico de Columnas", icon: "📊" },
    lineChart: { label: "Gráfico de Líneas", icon: "📈" },
    pieChart: { label: "Gráfico Circular", icon: "🥧" },
    donutChart: { label: "Gráfico de Dona", icon: "🍩" },
    card: { label: "Tarjeta KPI", icon: "🎴" },
    table: { label: "Tabla", icon: "📋" },
    matrix: { label: "Matriz", icon: "🔢" },
    areaChart: { label: "Gráfico de Área", icon: "📉" },
    scatterChart: { label: "Gráfico de Dispersión", icon: "⚬" },
};

export const OPERATION_LABELS: Record<string, { label: string; color: string }> = {
    CREATE: { label: "Crear Visual", color: "#22c55e" },
    CREATE_VISUAL: { label: "Crear Visual", color: "#22c55e" },
    UPDATE: { label: "Actualizar", color: "#06b6d4" },
    FILTER: { label: "Aplicar Filtro", color: "#3b82f6" },
    NAVIGATE: { label: "Navegar", color: "#a855f7" },
    EXPLAIN: { label: "Explicación", color: "#f59e0b" },
    DELETE: { label: "Eliminar", color: "#ef4444" },
    UNKNOWN: { label: "No Reconocido", color: "#6b7280" },
    ERROR: { label: "Error", color: "#ef4444" },
};
