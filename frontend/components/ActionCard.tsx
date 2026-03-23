"use client";

/**
 * ActionCard — Renderiza la acción generada por la IA como una tarjeta visual.
 *
 * WHY: El usuario necesita ver de forma clara y visual lo que la IA
 * generó. Esta tarjeta muestra el tipo de operación, los datos
 * asignados, el DAX (si hay), y la explicación. Es el "resultado
 * tangible" de cada interacción con el orquestador.
 */

import { useState } from "react";
import type { VisualAction } from "@/lib/types";
import { VISUAL_TYPE_LABELS, OPERATION_LABELS } from "@/lib/types";

interface ActionCardProps {
    action: VisualAction;
    intent: string;
}

function formatDataRoleValue(value: VisualAction["dataRoles"][string]): string {
    if (typeof value === "string") return value;
    if (value.ref) return value.ref;
    if (value.table && value.column) return `${value.table}[${value.column}]`;
    return JSON.stringify(value);
}

export default function ActionCard({ action, intent }: ActionCardProps) {
    const [showDax, setShowDax] = useState(false);
    const opInfo = OPERATION_LABELS[action.operation] || OPERATION_LABELS[intent] || OPERATION_LABELS["UNKNOWN"];
    const visualInfo = VISUAL_TYPE_LABELS[action.visualType];

    return (
        <div className="action-card glass rounded-xl p-4 mt-3 space-y-3">
            {/* Header: Operation Badge + Visual Type */}
            <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                    <span
                        className="px-2.5 py-1 rounded-full text-xs font-bold text-white"
                        style={{ backgroundColor: opInfo.color }}
                    >
                        {opInfo.label}
                    </span>
                    {visualInfo && (
                        <span className="text-sm text-[var(--color-text-secondary)]">
                            {visualInfo.icon} {visualInfo.label}
                        </span>
                    )}
                </div>
                {action.title && (
                    <span className="text-xs text-[var(--color-text-muted)]">
                        {action.title}
                    </span>
                )}
            </div>

            {/* Data Roles (for CREATE) */}
            {Object.keys(action.dataRoles).length > 0 && (
                <div className="space-y-1">
                    <p className="text-xs font-semibold text-[var(--color-text-muted)] uppercase tracking-wide">
                        Datos Asignados
                    </p>
                    <div className="flex flex-wrap gap-2">
                        {Object.entries(action.dataRoles).map(([role, value]) => (
                            <span
                                key={role}
                                className="px-2 py-1 rounded-md text-xs bg-[var(--color-bg-secondary)] border border-[var(--color-border)]"
                            >
                                <span className="text-[var(--color-accent)]">{role}:</span>{" "}
                                <span className="text-[var(--color-text-primary)]">{formatDataRoleValue(value)}</span>
                            </span>
                        ))}
                    </div>
                </div>
            )}

            {/* Filters (for FILTER) */}
            {action.filters.length > 0 && (
                <div className="space-y-1">
                    <p className="text-xs font-semibold text-[var(--color-text-muted)] uppercase tracking-wide">
                        Filtros Aplicados
                    </p>
                    {action.filters.map((f, i) => (
                        <div key={i} className="px-2 py-1 rounded-md text-xs bg-[var(--color-bg-secondary)] border border-[var(--color-border)]">
                            <span className="text-[var(--color-info)]">{f.table}[{f.column}]</span>{" "}
                            <span className="text-[var(--color-text-muted)]">{f.operator}</span>{" "}
                            <span className="text-[var(--color-success)]">{f.values.join(", ")}</span>
                        </div>
                    ))}
                </div>
            )}

            {/* DAX Code (collapsible) */}
            {action.dax && (
                <div>
                    <button
                        onClick={() => setShowDax(!showDax)}
                        className="text-xs text-[var(--color-accent)] hover:text-[var(--color-accent-hover)] flex items-center gap-1 cursor-pointer"
                    >
                        {showDax ? "▼" : "▶"} Código DAX
                        {action.dax_name && (
                            <span className="text-[var(--color-text-muted)]">— {action.dax_name}</span>
                        )}
                    </button>
                    {showDax && (
                        <pre className="mt-2 p-3 rounded-lg bg-[#0d1117] text-xs text-[var(--color-success)] overflow-x-auto font-mono">
                            {action.dax}
                        </pre>
                    )}
                </div>
            )}

            {/* Suggested Visuals (for EXPLAIN) */}
            {action.suggested_visuals.length > 0 && (
                <div className="space-y-1">
                    <p className="text-xs font-semibold text-[var(--color-text-muted)] uppercase tracking-wide">
                        Visuales Sugeridos
                    </p>
                    {action.suggested_visuals.map((sv, i) => {
                        const svInfo = VISUAL_TYPE_LABELS[sv.visualType];
                        return (
                            <div key={i} className="flex items-center gap-2 text-xs text-[var(--color-text-secondary)]">
                                <span>{svInfo?.icon || "📊"}</span>
                                <span>{sv.description}</span>
                            </div>
                        );
                    })}
                </div>
            )}

            {/* Follow-up Questions */}
            {action.follow_up_questions.length > 0 && (
                <div className="space-y-1 pt-2 border-t border-[var(--color-border)]">
                    <p className="text-xs font-semibold text-[var(--color-text-muted)] uppercase tracking-wide">
                        Preguntas de Seguimiento
                    </p>
                    {action.follow_up_questions.map((q, i) => (
                        <p key={i} className="text-xs text-[var(--color-accent)] cursor-pointer hover:text-[var(--color-accent-hover)]">
                            💡 {q}
                        </p>
                    ))}
                </div>
            )}
        </div>
    );
}
