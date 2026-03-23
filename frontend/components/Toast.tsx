"use client";

/**
 * Toast — Animated notification component for SDK action feedback.
 *
 * WHY: When the Action Handler executes a command on the Power BI report
 * (filter, create visual, navigate), the user needs immediate, non-intrusive
 * feedback. The toast slides in from the bottom-right, auto-dismisses
 * after 4 seconds, and can be manually closed.
 */

import { useEffect, useState } from "react";
import type { ActionResult } from "@/lib/actionHandler";

interface ToastProps {
    result: ActionResult | null;
    onDismiss: () => void;
}

export default function Toast({ result, onDismiss }: ToastProps) {
    const [visible, setVisible] = useState(false);

    useEffect(() => {
        if (!result) {
            setVisible(false);
            return;
        }

        // Slide in
        setVisible(true);

        // Auto-dismiss after 4 seconds
        const timer = setTimeout(() => {
            setVisible(false);
            setTimeout(onDismiss, 300); // Wait for exit animation
        }, 4000);

        return () => clearTimeout(timer);
    }, [result, onDismiss]);

    if (!result) return null;

    const iconMap: Record<string, string> = {
        FILTER: "🔍",
        CREATE: "📊",
        CREATE_VISUAL: "📊",
        NAVIGATE: "🧭",
        EXPLAIN: "💡",
    };

    const icon = iconMap[result.operation] || "⚡";

    return (
        <div
            className={`toast-container ${visible ? "toast-enter" : "toast-exit"}`}
            role="alert"
        >
            <div
                className={`glass rounded-xl px-4 py-3 flex items-center gap-3 min-w-[300px] max-w-[450px] shadow-2xl ${result.success
                        ? result.appliedToReport
                            ? "border-green-500/40"
                            : "border-blue-500/40"
                        : "border-amber-500/40"
                    }`}
            >
                {/* Icon */}
                <span className="text-xl">{icon}</span>

                {/* Message */}
                <div className="flex-1 min-w-0">
                    <p className="text-xs font-medium text-[var(--color-text-primary)] truncate">
                        {result.message}
                    </p>
                    {result.appliedToReport && (
                        <p className="text-[10px] text-green-400 mt-0.5">
                            Aplicado al reporte en vivo
                        </p>
                    )}
                </div>

                {/* Close btn */}
                <button
                    onClick={() => {
                        setVisible(false);
                        setTimeout(onDismiss, 300);
                    }}
                    className="text-[var(--color-text-muted)] hover:text-[var(--color-text-primary)] transition-colors cursor-pointer"
                >
                    ✕
                </button>
            </div>
        </div>
    );
}
