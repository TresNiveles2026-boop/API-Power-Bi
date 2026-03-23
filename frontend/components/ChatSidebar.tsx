"use client";

/**
 * ChatSidebar — Panel de chat con input y respuestas de la IA.
 *
 * WHY: Este es el punto de interacción principal del usuario con
 * el orquestador. Envía mensajes en lenguaje natural al backend,
 * muestra la respuesta de la IA en tiempo real (con indicador de
 * progreso multi-paso), y renderiza los ActionCards con los resultados.
 *
 * Phase 4: Added suggestion chips, multi-step loading states, and
 *          enhanced follow-up question UX.
 */

import { useState, useRef, useEffect, useCallback } from "react";
import {
    sendChatMessage,
    getConversationMessages,
    syncSchemaFromPowerBi,
    ApiTimeoutError,
    ApiRateLimitError,
    ApiConnectionError,
} from "@/lib/api";
import { getActivePowerBiReport, getCanvasVisualContext, scanOperationalSchema } from "@/lib/pbiRuntime";
import { RESCUE_ONBOARDING_MESSAGE } from "@/lib/rescueUx";
import type { ChatMessage, ChatResponse } from "@/lib/types";
import type { ActionResult } from "@/lib/actionHandler";
import ActionCard from "./ActionCard";
import ConversationList from "./ConversationList";
import { useUi } from "@/lib/uiContext";

interface ChatSidebarProps {
    reportId: string;
    tenantId: string;
    onActionGenerated?: (response: ChatResponse) => Promise<ActionResult> | ActionResult | void;
}

// ── Quick Action Suggestions ─────────────────────────────────

const SUGGESTIONS = [
    { icon: "📊", text: "Crea un grafico de ventas por categoria" },
    { icon: "🔍", text: "Filtra por 400" },
    { icon: "📈", text: "Muestra un grafico de tendencias" },
    { icon: "💡", text: "Explica los datos principales" },
];

// ── Loading Step Phases ──────────────────────────────────────

const LOADING_STEPS = [
    { icon: "🔍", label: "Analizando intención..." },
    { icon: "🧠", label: "Generando con Gemini..." },
    { icon: "⚡", label: "Ejecutando acción..." },
];

export default function ChatSidebar({
    reportId,
    tenantId,
    onActionGenerated,
}: ChatSidebarProps) {
    const [messages, setMessages] = useState<ChatMessage[]>([]);
    const [input, setInput] = useState("");
    const [isLoading, setIsLoading] = useState(false);
    const [loadingStep, setLoadingStep] = useState(0);
    const [currentConversationId, setCurrentConversationId] = useState<string | null>(null);
    const { openSync, powerbiAccessToken } = useUi();

    const messagesEndRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLTextAreaElement>(null);
    const loadingTimerRef = useRef<NodeJS.Timeout | null>(null);

    // Auto-scroll al último mensaje
    const scrollToBottom = useCallback(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }, []);

    useEffect(() => {
        scrollToBottom();
    }, [messages, scrollToBottom]);

    // Initial Welcome Message
    useEffect(() => {
        if (!currentConversationId && messages.length === 0) {
            setMessages([
                {
                    id: "welcome",
                    role: "assistant",
                    content:
                        "¡Hola! 👋 Soy tu asistente de BI. Puedo crear visuales, aplicar filtros, navegar páginas y explicar métricas. ¿Qué necesitas?",
                    timestamp: new Date(),
                },
            ]);
        }
    }, [currentConversationId, messages.length]);

    // Cleanup loading timer
    useEffect(() => {
        return () => {
            if (loadingTimerRef.current) clearTimeout(loadingTimerRef.current);
        };
    }, []);

    // Auto-resize del textarea
    const handleInputChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
        setInput(e.target.value);
        e.target.style.height = "auto";
        e.target.style.height = Math.min(e.target.scrollHeight, 120) + "px";
    };

    // Load conversation messages
    const handleSelectConversation = async (id: string) => {
        setCurrentConversationId(id);
        setIsLoading(true);
        try {
            const history = await getConversationMessages(id);
            // Map backend messages to frontend format
            const mappedMessages: ChatMessage[] = history.map((msg: any) => ({
                id: msg.id,
                role: msg.role,
                content: msg.content,
                timestamp: new Date(msg.created_at),
                action: msg.action,
                intent: msg.intent,
                confidence: msg.confidence,
            }));
            setMessages(mappedMessages);
        } catch (error) {
            console.error("Error loading messages:", error);
        } finally {
            setIsLoading(false);
        }
    };

    const handleNewConversation = () => {
        setCurrentConversationId(null);
        setMessages([
            {
                id: "welcome",
                role: "assistant",
                content:
                    "¡Hola! 👋 Soy tu asistente de BI. Puedo crear visuales, aplicar filtros, navegar páginas y explicar métricas. ¿Qué necesitas?",
                timestamp: new Date(),
            },
        ]);
    };

    // Animate through loading steps
    const startLoadingSteps = () => {
        setLoadingStep(0);
        loadingTimerRef.current = setTimeout(() => {
            setLoadingStep(1);
            loadingTimerRef.current = setTimeout(() => {
                setLoadingStep(2);
            }, 1500);
        }, 1200);
    };

    const handleSubmit = async (messageText?: string) => {
        const text = messageText || input.trim();
        if (!text || isLoading) return;

        // Agregar mensaje del usuario
        const userMsg: ChatMessage = {
            id: `user-${Date.now()}`,
            role: "user",
            content: text,
            timestamp: new Date(),
        };

        // Agregar placeholder de loading
        const loadingMsg: ChatMessage = {
            id: `loading-${Date.now()}`,
            role: "assistant",
            content: "",
            timestamp: new Date(),
            isLoading: true,
        };

        setMessages((prev) => [...prev, userMsg, loadingMsg]);
        setInput("");
        setIsLoading(true);
        startLoadingSteps();

        // Reset textarea height
        if (inputRef.current) {
            inputRef.current.style.height = "auto";
        }

        try {
            // Preflight UX rescue: si el reporte está en blanco y el Scanner API está bloqueado,
            // no enviamos nada al LLM para evitar alucinaciones / errores de schema.
            try {
                const report = getActivePowerBiReport();
                const operationalCols = report ? await scanOperationalSchema(report) : [];

                if (!operationalCols || operationalCols.length === 0) {
                    const vip = await syncSchemaFromPowerBi(reportId, tenantId, powerbiAccessToken || undefined);
                    if (vip?.admin_blocked || vip?.mode === "operational") {
                        const rescueMsg: ChatMessage = {
                            id: `rescue-${Date.now()}`,
                            role: "assistant",
                            content: RESCUE_ONBOARDING_MESSAGE,
                            timestamp: new Date(),
                            rescueCta: true,
                        };
                        setMessages((prev) => [
                            ...prev.filter((m) => !m.isLoading),
                            rescueMsg,
                        ]);
                        return;
                    }
                }
            } catch {
                // Rescue preflight best-effort; si falla, seguimos flujo normal.
            }

            const visualContext = await getCanvasVisualContext();
            const response = await sendChatMessage({
                message: text,
                report_id: reportId,
                tenant_id: tenantId,
                conversation_id: currentConversationId || undefined,
                visual_context: visualContext,
            });

            // Update conversation ID if new
            if (response.conversation_id && !currentConversationId) {
                setCurrentConversationId(response.conversation_id);
            }

            const assistantMsg: ChatMessage = {
                id: `assistant-${Date.now()}`,
                role: "assistant",
                content: response.action.explanation,
                timestamp: new Date(),
                action: response.action,
                intent: response.intent,
                confidence: response.confidence,
            };

            // Reemplazar el loading con la respuesta real
            setMessages((prev) => [
                ...prev.filter((m) => !m.isLoading),
                assistantMsg,
            ]);

            // Notificar al padre sobre la acción generada
            if (onActionGenerated) {
                const actionResult = await onActionGenerated(response);
                const hasExplainAction =
                    response.action?.operation === "EXPLAIN" ||
                    (Array.isArray(response.actions) &&
                        response.actions.some((a) => a.operation === "EXPLAIN"));
                if (actionResult && actionResult.success && (actionResult.operation === "EXPLAIN" || hasExplainAction)) {
                    setMessages((prevMessages) => {
                        const newMessages = [...prevMessages];
                        const lastAssistantIndex = newMessages.map((m) => m.role).lastIndexOf("assistant");
                        if (lastAssistantIndex !== -1) {
                            const current = newMessages[lastAssistantIndex];
                            newMessages[lastAssistantIndex] = {
                                ...current,
                                content: actionResult.message,
                                action: current.action
                                    ? { ...current.action, explanation: actionResult.message }
                                    : current.action,
                            };
                        }
                        return newMessages;
                    });
                }
            }
        } catch (error) {
            // Phase 4: Differentiated error messages
            let errorIcon = "❌";
            let errorContent = "Error desconocido. Intenta de nuevo.";

            if (error instanceof ApiTimeoutError) {
                errorIcon = "⏰";
                errorContent = error.message;
            } else if (error instanceof ApiRateLimitError) {
                errorIcon = "🚦";
                errorContent = error.message;
            } else if (error instanceof ApiConnectionError) {
                errorIcon = "📡";
                errorContent = error.message;
            } else if (error instanceof Error) {
                errorContent = error.message;
            }

            const errorMsg: ChatMessage = {
                id: `error-${Date.now()}`,
                role: "assistant",
                content: `${errorIcon} ${errorContent}`,
                timestamp: new Date(),
                isError: true,
                failedMessage: text,  // Store original message for retry
            };

            setMessages((prev) => [
                ...prev.filter((m) => !m.isLoading),
                errorMsg,
            ]);
        } finally {
            setIsLoading(false);
            setLoadingStep(0);
            if (loadingTimerRef.current) {
                clearTimeout(loadingTimerRef.current);
                loadingTimerRef.current = null;
            }
        }
    };

    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            handleSubmit();
        }
    };

    const handleFollowUp = (question: string) => {
        handleSubmit(question);
    };

    // Phase 4: Retry a failed message
    const handleRetry = (failedMessage: string, errorMsgId: string) => {
        // Remove the error message before retrying
        setMessages((prev) => prev.filter((m) => m.id !== errorMsgId));
        handleSubmit(failedMessage);
    };

    // Check if only the welcome message exists (show suggestions)
    const showSuggestions = messages.length === 1 && messages[0].id === "welcome";

    return (
        <div className="flex h-full overflow-hidden">
            {/* Conversation History Sidebar */}
            <ConversationList
                currentConversationId={currentConversationId}
                onSelectConversation={handleSelectConversation}
                onNewConversation={handleNewConversation}
            />

            {/* Main Chat Area */}
            <div className="flex flex-col flex-1 h-full min-w-0">
                {/* Messages Area */}
                <div className="flex-1 overflow-y-auto p-4 space-y-4">
                    {messages.map((msg) => (
                        <div
                            key={msg.id}
                            className={`animate-fade-in-up ${msg.role === "user" ? "flex justify-end" : "flex justify-start"
                                }`}
                        >
                            <div
                                className={`max-w-[90%] ${msg.role === "user"
                                    ? "bg-[var(--color-accent)] text-white rounded-2xl rounded-br-md px-4 py-2.5"
                                    : "text-[var(--color-text-primary)]"
                                    }`}
                            >
                                {/* Loading Indicator — Multi-Step */}
                                {msg.isLoading ? (
                                    <div className="py-2 space-y-2">
                                        {LOADING_STEPS.map((step, i) => (
                                            <div
                                                key={i}
                                                className={`flex items-center gap-2 text-xs transition-all duration-300 ${i < loadingStep
                                                    ? "text-[var(--color-success)] opacity-100"
                                                    : i === loadingStep
                                                        ? "text-[var(--color-accent)] opacity-100"
                                                        : "text-[var(--color-text-muted)] opacity-40"
                                                    }`}
                                            >
                                                <span className={`w-4 h-4 flex items-center justify-center ${i === loadingStep ? "animate-pulse" : ""
                                                    }`}>
                                                    {i < loadingStep ? "✓" : step.icon}
                                                </span>
                                                <span>{step.label}</span>
                                            </div>
                                        ))}
                                    </div>
                                ) : (
                                    <>
                                        <p className={`text-sm leading-relaxed whitespace-pre-wrap ${msg.isError ? "text-[var(--color-error,#ef4444)]" : ""
                                            }`}>
                                            {msg.content}
                                        </p>

                                        {msg.rescueCta && (
                                            <button
                                                onClick={openSync}
                                                disabled={isLoading}
                                                className="mt-2 flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg bg-[var(--color-bg-secondary)] border border-[var(--color-border)] text-[var(--color-accent)] hover:border-[var(--color-accent)] disabled:opacity-40 transition-colors cursor-pointer"
                                            >
                                                ⬆️ Subir Plantilla .pbit
                                            </button>
                                        )}

                                        {/* Phase 4: Retry button for failed messages */}
                                        {msg.isError && msg.failedMessage && (
                                            <button
                                                onClick={() => handleRetry(msg.failedMessage!, msg.id)}
                                                disabled={isLoading}
                                                className="mt-2 flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg bg-[var(--color-bg-secondary)] border border-[var(--color-border)] text-[var(--color-accent)] hover:border-[var(--color-accent)] disabled:opacity-40 transition-colors cursor-pointer"
                                            >
                                                🔄 Reintentar
                                            </button>
                                        )}

                                        {/* Action Card */}
                                        {msg.action && msg.action.operation !== "ERROR" && (
                                            <ActionCard action={msg.action} intent={msg.intent || ""} />
                                        )}

                                        {/* Clickable Follow-up Questions */}
                                        {msg.action?.follow_up_questions &&
                                            msg.action.follow_up_questions.length > 0 && (
                                                <div className="mt-3 space-y-1.5">
                                                    {msg.action.follow_up_questions.map((q, i) => (
                                                        <button
                                                            key={i}
                                                            onClick={() => handleFollowUp(q)}
                                                            className="block w-full text-left text-xs px-3 py-2 rounded-lg bg-[var(--color-bg-secondary)] border border-[var(--color-border)] text-[var(--color-accent)] hover:border-[var(--color-accent)] transition-colors cursor-pointer"
                                                        >
                                                            💡 {q}
                                                        </button>
                                                    ))}
                                                </div>
                                            )}

                                        {/* Confidence Badge */}
                                        {msg.confidence !== undefined && msg.confidence > 0 && (
                                            <div className="mt-2 flex items-center gap-2">
                                                <div className="h-1 flex-1 bg-[var(--color-bg-secondary)] rounded-full overflow-hidden">
                                                    <div
                                                        className="h-full rounded-full transition-all duration-500"
                                                        style={{
                                                            width: `${msg.confidence * 100}%`,
                                                            backgroundColor:
                                                                msg.confidence > 0.8
                                                                    ? "var(--color-success)"
                                                                    : msg.confidence > 0.5
                                                                        ? "var(--color-warning)"
                                                                        : "var(--color-error)",
                                                        }}
                                                    />
                                                </div>
                                                <span className="text-[10px] text-[var(--color-text-muted)]">
                                                    {Math.round(msg.confidence * 100)}%
                                                </span>
                                            </div>
                                        )}
                                    </>
                                )}
                            </div>
                        </div>
                    ))}

                    {/* Quick Action Suggestion Chips — shown only on first load */}
                    {showSuggestions && (
                        <div className="animate-fade-in-up pt-2">
                            <p className="text-[10px] text-[var(--color-text-muted)] uppercase tracking-wider mb-2 font-semibold">
                                Prueba estas acciones
                            </p>
                            <div className="grid grid-cols-1 gap-2">
                                {SUGGESTIONS.map((s, i) => (
                                    <button
                                        key={i}
                                        onClick={() => handleSubmit(s.text)}
                                        className="suggestion-chip text-left text-xs px-3 py-2.5 rounded-xl bg-[var(--color-bg-secondary)]/60 border border-[var(--color-border)] text-[var(--color-text-secondary)] flex items-center gap-2"
                                    >
                                        <span className="text-base">{s.icon}</span>
                                        <span>{s.text}</span>
                                    </button>
                                ))}
                            </div>
                        </div>
                    )}

                    <div ref={messagesEndRef} />
                </div>

                {/* Input Area */}
                <div className="p-4 border-t border-[var(--color-border)]">
                    <div className="flex gap-2 items-end">
                        <textarea
                            ref={inputRef}
                            value={input}
                            onChange={handleInputChange}
                            onKeyDown={handleKeyDown}
                            placeholder="Escribe tu solicitud... (ej: 'Crea un gráfico de ventas por región')"
                            className="chat-input flex-1 bg-[var(--color-bg-secondary)] border border-[var(--color-border)] rounded-xl px-4 py-3 text-sm text-[var(--color-text-primary)] placeholder-[var(--color-text-muted)] resize-none transition-all"
                            rows={1}
                            disabled={isLoading}
                        />
                        <button
                            onClick={() => handleSubmit()}
                            disabled={!input.trim() || isLoading}
                            className="p-3 rounded-xl bg-[var(--color-accent)] text-white hover:bg-[var(--color-accent-hover)] disabled:opacity-30 disabled:cursor-not-allowed transition-all cursor-pointer"
                        >
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                <path d="M22 2L11 13" />
                                <path d="M22 2L15 22L11 13L2 9L22 2Z" />
                            </svg>
                        </button>
                    </div>
                    <p className="text-[10px] text-[var(--color-text-muted)] mt-2 text-center">
                        Presiona Enter para enviar · Shift+Enter para nueva línea
                    </p>
                </div>
            </div>
        </div>
    );
}
