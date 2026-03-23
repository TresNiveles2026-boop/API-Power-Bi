"use client";

/**
 * DataSyncModal — Modal de sincronización de esquema desde Power BI.
 *
 * WHY: Sincroniza el esquema real del modelo (tablas/columnas) vía REST API,
 * evitando divergencias entre el dataset publicado y el diccionario local.
 */

import { useRef, useState, useCallback } from "react";
import { syncSchemaFromPowerBi, syncSchema, uploadPbitTemplate, type ColumnSchemaPayload } from "@/lib/api";
import { getActivePowerBiReport, scanOperationalSchema } from "@/lib/pbiRuntime";
import { RESCUE_ONBOARDING_MESSAGE } from "@/lib/rescueUx";

interface DataSyncModalProps {
    isOpen: boolean;
    onClose: () => void;
    reportId: string;
    tenantId: string;
    powerbiAccessToken?: string;
}

type UploadState = "idle" | "uploading" | "success" | "error" | "rescue";

export default function DataSyncModal({
    isOpen,
    onClose,
    reportId,
    tenantId,
    powerbiAccessToken,
}: DataSyncModalProps) {
    const [uploadState, setUploadState] = useState<UploadState>("idle");
    const [resultMessage, setResultMessage] = useState("");
    const [schemaPreview, setSchemaPreview] = useState<any>(null);
    const [pbitUploadState, setPbitUploadState] = useState<"idle" | "uploading" | "success" | "error">("idle");
    const [pbitUploadMessage, setPbitUploadMessage] = useState("");
    const fileInputRef = useRef<HTMLInputElement | null>(null);

    const resetState = useCallback(() => {
        setUploadState("idle");
        setResultMessage("");
        setSchemaPreview(null);
        setPbitUploadState("idle");
        setPbitUploadMessage("");
    }, []);

    const handleClose = () => {
        if (uploadState === "uploading") return; // Don't close while uploading
        resetState();
        onClose();
    };

    const handleSync = async () => {
        setUploadState("uploading");
        setResultMessage("");

        try {
            const result = await syncSchemaFromPowerBi(reportId, tenantId, powerbiAccessToken);
            if (result?.mode === "operational" || result?.admin_blocked) {
                setResultMessage("⚠️ Sin Scanner API. Ejecutando sincronización rápida por SDK...");

                const report = getActivePowerBiReport();
                const operationalColumns = await scanOperationalSchema(report);
                if (!operationalColumns || operationalColumns.length === 0) {
                    setUploadState("rescue");
                    setResultMessage(RESCUE_ONBOARDING_MESSAGE);
                    return;
                }
                await syncSchema(
                    reportId,
                    tenantId,
                    operationalColumns as unknown as ColumnSchemaPayload[]
                );

                setSchemaPreview({ columns: operationalColumns });
                setUploadState("success");
                setResultMessage(
                    `✅ Sincronización rápida completada (${operationalColumns.length} columnas en uso).`
                );

                setTimeout(() => {
                    handleClose();
                }, 3500);
                return;
            }

            setSchemaPreview(result);
            setUploadState("success");

            const columnCount = result.columns_synced || 0;
            setResultMessage(`✅ Sincronizado: ${columnCount} columnas`);

            setTimeout(() => {
                handleClose();
            }, 3000);
        } catch (err: any) {
            setUploadState("error");
            setResultMessage(
                err?.message || "Error al subir el archivo. Verifica el formato e intenta de nuevo."
            );
        }
    };

    const handleSelectPbit = async (file: File) => {
        const name = (file?.name || "").toLowerCase();
        if (!name.endsWith(".pbit")) {
            setPbitUploadState("error");
            setPbitUploadMessage("Solo se permiten archivos .pbit");
            return;
        }
        setPbitUploadState("uploading");
        setPbitUploadMessage("");
        try {
            const res = await uploadPbitTemplate(reportId, tenantId, file);
            setPbitUploadState("success");
            setPbitUploadMessage(res?.message || "Archivo recibido. Esquema listo.");
            setUploadState("success");
            setSchemaPreview({ columns: [] });
            setResultMessage(`✅ Sincronizado: ${res?.columns_synced || 0} columnas`);
            setTimeout(() => {
                handleClose();
            }, 3500);
        } catch (e: any) {
            setPbitUploadState("error");
            setPbitUploadMessage(e?.message || "No se pudo subir la plantilla .pbit.");
        }
    };

    if (!isOpen) return null;

    return (
        <div
            className="sync-modal-overlay"
            onClick={handleClose}
            role="dialog"
            aria-modal="true"
            aria-label="Sincronizar datos"
        >
            <div
                className="sync-modal-content glass"
                onClick={(e) => e.stopPropagation()}
            >
                {/* Header */}
                <div className="flex items-center justify-between mb-6">
                    <div className="flex items-center gap-3">
                        <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-[var(--color-accent)] to-purple-600 flex items-center justify-center text-lg shadow-lg">
                            📤
                        </div>
                        <div>
                            <h2 className="text-base font-bold text-[var(--color-text-primary)]">
                                Sincronizar Datos
                            </h2>
                            <p className="text-[11px] text-[var(--color-text-muted)]">
                                Sincroniza el esquema real del modelo de Power BI
                            </p>
                        </div>
                    </div>
                    <button
                        onClick={handleClose}
                        disabled={uploadState === "uploading"}
                        className="w-8 h-8 rounded-lg flex items-center justify-center text-[var(--color-text-muted)] hover:text-[var(--color-text-primary)] hover:bg-[var(--color-bg-secondary)] transition-all cursor-pointer disabled:opacity-30"
                    >
                        ✕
                    </button>
                </div>

                <div className="mt-4 px-4 py-3 rounded-xl bg-[var(--color-bg-secondary)] border border-[var(--color-border)]">
                    <p className="text-xs text-[var(--color-text-secondary)] leading-relaxed">
                        Este flujo consulta directamente el modelo publicado en Power BI y
                        actualiza el diccionario semántico en Supabase.
                    </p>
                </div>

                {/* Status Message */}
                {resultMessage && (
                    <div
                        className={`mt-4 px-4 py-3 rounded-xl text-sm animate-fade-in-up ${
                            uploadState === "success"
                                ? "bg-green-900/30 border border-green-700/40 text-green-400"
                                : uploadState === "rescue"
                                    ? "bg-amber-900/25 border border-amber-700/40 text-amber-200"
                                : uploadState === "error"
                                    ? "bg-red-900/30 border border-red-700/40 text-red-400"
                                    : ""
                        }`}
                    >
                        {resultMessage}
                    </div>
                )}

                {/* Schema Preview on Success */}
                {uploadState === "success" && schemaPreview?.columns && (
                    <div className="mt-3 px-4 py-3 rounded-xl bg-[var(--color-bg-secondary)] border border-[var(--color-border)] max-h-[120px] overflow-y-auto">
                        <p className="text-[10px] text-[var(--color-text-muted)] uppercase tracking-wider font-semibold mb-2">
                            Columnas detectadas
                        </p>
                        <div className="flex flex-wrap gap-1.5">
                            {schemaPreview.columns.map((col: any, i: number) => (
                                <span
                                    key={i}
                                    className="px-2 py-0.5 rounded-md text-[11px] bg-[var(--color-accent)]/10 border border-[var(--color-accent)]/20 text-[var(--color-accent)]"
                                >
                                    {col.column_name || col.name}
                                </span>
                            ))}
                        </div>
                    </div>
                )}

                {/* Rescue Uploader (.pbit) */}
                {uploadState === "rescue" && (
                    <div className="mt-4">
                        <div
                            className="px-4 py-4 rounded-xl border border-dashed border-[var(--color-border)] bg-[var(--color-bg-secondary)] cursor-pointer hover:border-[var(--color-accent)] transition-colors"
                            onClick={() => fileInputRef.current?.click()}
                            role="button"
                            tabIndex={0}
                        >
                            <div className="text-sm text-[var(--color-text-primary)] font-medium">
                                ⬆️ Subir Plantilla (.pbit)
                            </div>
                            <div className="text-[11px] text-[var(--color-text-muted)] mt-1">
                                Haz clic para seleccionar tu archivo `.pbit`.
                            </div>
                            <div className="mt-2 text-[11px] text-[var(--color-text-muted)] leading-relaxed">
                                💡 ¿Cómo obtener tu archivo?
                                <br />• Solo disponible en Windows: En Power BI Desktop, ve a Archivo &gt; Exportar &gt; Plantilla de Power BI (.pbit).
                            </div>
                            {pbitUploadMessage && (
                                <div
                                    className={`mt-3 px-3 py-2 rounded-lg text-xs ${
                                        pbitUploadState === "success"
                                            ? "bg-green-900/25 border border-green-700/40 text-green-300"
                                            : pbitUploadState === "error"
                                                ? "bg-red-900/25 border border-red-700/40 text-red-300"
                                                : "bg-[var(--color-bg-secondary)] border border-[var(--color-border)] text-[var(--color-text-secondary)]"
                                    }`}
                                >
                                    {pbitUploadMessage}
                                </div>
                            )}
                        </div>

                        <input
                            ref={fileInputRef}
                            type="file"
                            accept=".pbit"
                            className="hidden"
                            onChange={(e) => {
                                const f = e.target.files?.[0];
                                if (f) void handleSelectPbit(f);
                                e.currentTarget.value = "";
                            }}
                        />
                    </div>
                )}

                {/* Action Buttons */}
                <div className="mt-6 flex gap-3">
                    <button
                        onClick={handleClose}
                        disabled={uploadState === "uploading"}
                        className="flex-1 px-4 py-2.5 rounded-xl border border-[var(--color-border)] text-sm text-[var(--color-text-secondary)] hover:border-[var(--color-text-muted)] transition-all cursor-pointer disabled:opacity-30"
                    >
                        Cancelar
                    </button>
                    <button
                        onClick={handleSync}
                        disabled={uploadState === "uploading" || uploadState === "success"}
                        className="flex-1 px-4 py-2.5 rounded-xl bg-gradient-to-r from-[var(--color-accent)] to-purple-600 text-white text-sm font-medium hover:shadow-[0_0_20px_rgba(99,102,241,0.3)] disabled:opacity-30 disabled:cursor-not-allowed transition-all cursor-pointer flex items-center justify-center gap-2"
                    >
                        {uploadState === "uploading" ? (
                            <>
                                <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24" fill="none">
                                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                                </svg>
                                Sincronizando...
                            </>
                        ) : uploadState === "success" ? (
                            <>✅ Completado</>
                        ) : (
                            <>🚀 Sincronizar</>
                        )}
                    </button>
                </div>
            </div>
        </div>
    );
}
