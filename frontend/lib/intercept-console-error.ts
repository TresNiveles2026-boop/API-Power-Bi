"use client";

declare global {
    interface Window {
        __AI_BI_ORIGINAL_CONSOLE_ERROR__?: (...args: unknown[]) => void;
        __AI_BI_CONSOLE_INTERCEPTOR_INSTALLED__?: boolean;
    }
}

const NOISE_PATTERNS = [
    "<path> attribute d",
    "expected arc flag",
    "expected number",
    "detalle de error power bi: {}",
    "pbi error vacío ignorado",
];

function shouldIgnoreConsoleError(args: unknown[]): boolean {
    const text = args
        .map((a) => {
            if (typeof a === "string") return a;
            if (a instanceof Error) return `${a.message} ${a.stack || ""}`;
            try {
                return JSON.stringify(a);
            } catch {
                return String(a);
            }
        })
        .join(" ")
        .toLowerCase();

    return NOISE_PATTERNS.some((p) => text.includes(p));
}

if (typeof window !== "undefined" && !window.__AI_BI_CONSOLE_INTERCEPTOR_INSTALLED__) {
    const original = console.error.bind(console);
    window.__AI_BI_ORIGINAL_CONSOLE_ERROR__ = original;

    console.error = (...args: unknown[]) => {
        if (shouldIgnoreConsoleError(args)) {
            return;
        }
        original(...args);
    };

    window.__AI_BI_CONSOLE_INTERCEPTOR_INSTALLED__ = true;
}

export { };
