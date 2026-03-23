"use client";

/**
 * Header — Barra de navegación superior con branding y sincronización.
 *
 * WHY: El header proporciona identidad visual al producto, muestra
 * el modo actual (MOCK/LIVE) como indicador de estado del sistema,
 * y ahora incluye el botón de sincronización de datos (Phase 5).
 */

import { useEffect, useState } from "react";
import { checkHealth } from "@/lib/api";
import DataSyncModal from "./DataSyncModal";
import { useUi } from "@/lib/uiContext";
import { acquirePowerBiDelegatedToken } from "@/lib/powerbiDelegatedAuth";

// IDs del reporte demo (sincronizados con page.tsx)
const DEMO_REPORT_ID = "94e97143-fcba-4d04-b871-9e4e3b0c65ed";
const DEMO_TENANT_ID = "9d36ff08-691e-4f7d-b1bf-049abf374860";

export default function Header() {
    const [mode, setMode] = useState<string>("...");
    const [isConnected, setIsConnected] = useState(false);
    const { isSyncOpen, openSync, closeSync, powerbiAccessToken, setPowerbiAccessToken } = useUi();

    useEffect(() => {
        checkHealth()
            .then((data) => {
                setMode(data.pbi_mode);
                setIsConnected(true);
            })
            .catch(() => {
                setMode("OFFLINE");
                setIsConnected(false);
            });
    }, []);

    return (
        <>
            <header className="glass border-b border-[var(--color-border)] px-6 py-3 flex items-center justify-between sticky top-0 z-50">
                {/* Left: Logo + Title */}
                <div className="flex items-center gap-3">
                    <div className="w-9 h-9 rounded-lg bg-gradient-to-br from-[var(--color-accent)] to-purple-600 flex items-center justify-center text-white font-bold text-sm shadow-lg">
                        AI
                    </div>
                    <div>
                        <h1 className="text-base font-bold text-[var(--color-text-primary)] leading-tight">
                            AI-BI Orchestrator
                        </h1>
                        <p className="text-[11px] text-[var(--color-text-muted)]">
                            Lenguaje Natural → Power BI
                        </p>
                    </div>
                </div>

                {/* Right: Sync Button + Status */}
                <div className="flex items-center gap-4">
                    {/* Delegated Token (User Identity) */}
                    <button
                        onClick={async () => {
                            try {
                                const token = await acquirePowerBiDelegatedToken();
                                setPowerbiAccessToken(token);
                            } catch (e) {
                                console.error("Delegated token error:", e);
                            }
                        }}
                        className="sync-btn flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs font-medium text-[var(--color-text-secondary)] bg-[var(--color-bg-secondary)] border border-[var(--color-border)] transition-all cursor-pointer"
                        title="Conectar con Microsoft (token delegado)"
                    >
                        {powerbiAccessToken ? "✅ Microsoft" : "🔐 Conectar"}
                    </button>

                    {/* Sync Data Button */}
                    <button
                        onClick={openSync}
                        className="sync-btn flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs font-medium text-[var(--color-text-secondary)] bg-[var(--color-bg-secondary)] border border-[var(--color-border)] transition-all cursor-pointer"
                        title="Sincronizar archivo de datos"
                    >
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                            <polyline points="17 8 12 3 7 8" />
                            <line x1="12" y1="3" x2="12" y2="15" />
                        </svg>
                        Sincronizar
                    </button>

                    {/* Connection Status */}
                    <div className="flex items-center gap-2">
                        <div
                            className={`w-2 h-2 rounded-full ${isConnected ? "bg-[var(--color-success)]" : "bg-[var(--color-error)]"
                                }`}
                            style={{
                                boxShadow: isConnected
                                    ? "0 0 6px var(--color-success)"
                                    : "0 0 6px var(--color-error)",
                            }}
                        />
                        <span className="text-xs text-[var(--color-text-secondary)]">
                            {isConnected ? "Conectado" : "Desconectado"}
                        </span>
                    </div>
                    <span
                        className={`px-2.5 py-1 rounded-full text-[10px] font-bold uppercase tracking-wider ${mode === "MOCK"
                                ? "bg-amber-900/40 text-amber-400 border border-amber-700/50"
                                : mode === "LIVE"
                                    ? "bg-green-900/40 text-green-400 border border-green-700/50"
                                    : "bg-red-900/40 text-red-400 border border-red-700/50"
                            }`}
                    >
                        {mode}
                    </span>
                </div>
            </header>

            {/* Data Sync Modal */}
            <DataSyncModal
                isOpen={isSyncOpen}
                onClose={closeSync}
                reportId={DEMO_REPORT_ID}
                tenantId={DEMO_TENANT_ID}
                powerbiAccessToken={powerbiAccessToken || undefined}
            />
        </>
    );
}
