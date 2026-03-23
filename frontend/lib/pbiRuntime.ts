"use client";

import type { models } from "powerbi-client";

let sdkLoadPromise: Promise<typeof import("powerbi-client")> | null = null;
let powerbiService: any | null = null;
let activeReport: any | null = null;

async function loadPowerBiSdk(): Promise<typeof import("powerbi-client")> {
    if (typeof window === "undefined") {
        throw new Error("Power BI runtime is only available in the browser.");
    }

    if (!sdkLoadPromise) {
        sdkLoadPromise = (async () => {
            const sdk = await import("powerbi-client");
            const globalWindow = window as any;

            // Force legacy-global compatibility so authoring can patch
            // the same Power BI runtime instance used by this app.
            if (!globalWindow.powerbi) {
                globalWindow.powerbi = sdk;
            }
            if (!globalWindow["powerbi-client"]) {
                globalWindow["powerbi-client"] = sdk;
            }

            await import("powerbi-report-authoring");
            return sdk;
        })();
    }

    return sdkLoadPromise;
}

export async function embedPowerBiReport(
    container: HTMLDivElement,
    embedConfig: models.IReportEmbedConfiguration
): Promise<any> {
    const sdk = await loadPowerBiSdk();

    if (!powerbiService) {
        powerbiService = new sdk.service.Service(
            sdk.factories.hpmFactory,
            sdk.factories.wpmpFactory,
            sdk.factories.routerFactory
        );
    }

    powerbiService.reset(container);
    const report = powerbiService.embed(container, embedConfig);
    activeReport = report;
    (window as any).report = report;
    return report;
}

export async function resetPowerBiContainer(container: HTMLDivElement): Promise<void> {
    const sdk = await loadPowerBiSdk();

    if (!powerbiService) {
        powerbiService = new sdk.service.Service(
            sdk.factories.hpmFactory,
            sdk.factories.wpmpFactory,
            sdk.factories.routerFactory
        );
    }

    powerbiService.reset(container);
    if ((window as any).report && activeReport === (window as any).report) {
        delete (window as any).report;
    }
    activeReport = null;
}

export function getActivePowerBiReport(): any | null {
    if (activeReport) return activeReport;
    if (typeof window !== "undefined" && (window as any).report) {
        return (window as any).report;
    }
    return null;
}

export async function getCanvasVisualContext(): Promise<
    Array<{ id: string; type: string; title: string; page?: string }>
> {
    const report = getActivePowerBiReport();
    if (!report || typeof report.getActivePage !== "function") return [];

    try {
        const activePage = await report.getActivePage();
        if (!activePage || typeof activePage.getVisuals !== "function") return [];

        const visuals = await activePage.getVisuals();
        if (!Array.isArray(visuals) || visuals.length === 0) return [];

        const context: Array<{ id: string; type: string; title: string; page?: string }> = [];
        for (const visual of visuals) {
            const id = String(visual?.name || "").trim();
            if (!id) continue;
            const type = String(visual?.type || "").trim();
            let title = "";

            if (typeof visual?.title === "string" && visual.title.trim()) {
                title = visual.title.trim();
            }

            if (!title && typeof visual?.getProperty === "function") {
                try {
                    const value = await visual.getProperty({
                        objectName: "title",
                        propertyName: "titleText",
                    });
                    if (typeof value?.value === "string" && value.value.trim()) {
                        title = value.value.trim();
                    }
                } catch {
                    // title extraction best-effort
                }
            }

            if (!title) title = id;
            context.push({
                id,
                type,
                title,
                page: String(activePage?.name || ""),
            });
        }
        return context;
    } catch {
        return [];
    }
}

// ── Dynamic Table Discovery ──────────────────────────────────
// Caché de nombres de tabla reales del modelo de Power BI.
// Se puebla al cargar el reporte vía discoverModelTables().
let _discoveredTables: string[] = [];

/**
 * Introspecciona los visuals existentes del reporte para descubrir
 * los nombres REALES de las tablas en el modelo de Power BI.
 *
 * WHY: El nombre de tabla en PBI es inestable (puede mutar entre sesiones).
 * El LLM usa un diccionario estático que puede quedar desactualizado.
 * Esta función extrae los nombres reales en runtime para corregir
 * cualquier desajuste automáticamente.
 */
export async function discoverModelTables(report: any): Promise<string[]> {
    const tables = new Set<string>();
    const columnsByTable = new Map<string, Set<string>>();

    try {
        let activePage: any = null;
        if (typeof report.getActivePage === "function") {
            activePage = await report.getActivePage();
        }
        if (!activePage && typeof report.getPages === "function") {
            const pages = await report.getPages();
            activePage = pages?.find((p: any) => p.isActive) || pages?.[0];
        }
        if (!activePage || typeof activePage.getVisuals !== "function") {
            console.warn("⚠️ discoverModelTables: No se pudo obtener la página activa.");
            return [];
        }

        const visuals = await activePage.getVisuals();
        if (!Array.isArray(visuals) || visuals.length === 0) return [];

        // Roles comunes donde buscar tabla+columna ya inyectados
        const rolesToProbe = ["Category", "Y", "Values", "X", "Rows", "Columns", "Series", "Legend", "Axis"];

        for (const visual of visuals) {
            if (typeof visual?.getDataFields !== "function") continue;

            for (const role of rolesToProbe) {
                try {
                    const fields = await visual.getDataFields(role);
                    if (!Array.isArray(fields)) continue;

                    for (const field of fields) {
                        const tableName = (field as any)?.table;
                        const colName = (field as any)?.column;
                        if (typeof tableName === "string" && tableName.trim()) {
                            const tName = tableName.trim();
                            tables.add(tName);
                            if (!columnsByTable.has(tName)) {
                                columnsByTable.set(tName, new Set<string>());
                            }
                            if (typeof colName === "string" && colName.trim()) {
                                columnsByTable.get(tName)!.add(colName.trim());
                            }
                        }
                    }
                } catch {
                    // Rol no existe en este visual — continuar
                }
            }
        }
    } catch (err) {
        console.warn("⚠️ discoverModelTables falló:", err);
    }

    _discoveredTables = Array.from(tables);

    if (_discoveredTables.length > 0) {
        console.log("✅ Tablas descubiertas del modelo PBI:", _discoveredTables.join(", "));
        for (const [table, cols] of columnsByTable.entries()) {
            console.log(`   📋 "${table}" columnas: ${Array.from(cols).join(", ")}`);
        }
    } else {
        console.warn("⚠️ No se descubrieron tablas del modelo. Los visuals existentes pueden estar vacíos.");
    }

    return _discoveredTables;
}

/**
 * Resuelve un nombre de tabla del LLM al nombre real en el modelo de PBI.
 *
 * Estrategia:
 * 1. Coincidencia exacta (caché hit directo)
 * 2. Coincidencia case-insensitive
 * 3. Fuzzy: el nombre real "contiene" o "está contenido en" el del LLM
 *    (cubre mutaciones como API-DatosPrueba ↔ API-DatosPrueba_Final)
 * 4. Fallback: devuelve el nombre original del LLM
 */
export function resolveRealTableName(llmTableName: string): string {
    const trimmed = (llmTableName || "").trim();
    if (!trimmed || _discoveredTables.length === 0) return trimmed;

    // 1. Exacta
    if (_discoveredTables.includes(trimmed)) return trimmed;

    // 2. Single-table model: si solo hay UNA tabla en el modelo PBI,
    // usarla SIEMPRE sin importar lo que diga el LLM o Supabase.
    // Esto cubre el caso donde Supabase dice "API-DatosPrueba_Final"
    // pero PBI tiene la tabla como "Tabla".
    if (_discoveredTables.length === 1) {
        const realTable = _discoveredTables[0];
        if (realTable !== trimmed) {
            console.log(`🔄 Table resolved (single-table model): "${trimmed}" → "${realTable}"`);
        }
        return realTable;
    }

    const llmLower = trimmed.toLowerCase();

    // 3. Case-insensitive
    const ciMatch = _discoveredTables.find(t => t.toLowerCase() === llmLower);
    if (ciMatch) return ciMatch;

    // 4. Fuzzy: uno contiene al otro (cubre _Final, _v2, etc.)
    const fuzzyMatch = _discoveredTables.find(t => {
        const realLower = t.toLowerCase();
        return realLower.includes(llmLower) || llmLower.includes(realLower);
    });
    if (fuzzyMatch) {
        console.log(`🔄 Table resolved: "${trimmed}" → "${fuzzyMatch}"`);
        return fuzzyMatch;
    }

    // 5. Fallback
    return trimmed;
}

type OperationalColumnSchema = {
    table_name: string;
    column_name: string;
    data_type: string;
    description: string;
    is_measure: boolean;
    dax_expression: string;
    sample_values: any[];
    metadata: Record<string, any>;
};

function normalizeSchemaKey(tableName: string, columnName: string): string {
    return `${tableName.trim().toLowerCase()}::${columnName.trim().toLowerCase()}`;
}

function mergeOperationalSchema(
    existing: OperationalColumnSchema,
    incoming: OperationalColumnSchema
): OperationalColumnSchema {
    const kindExisting = String(existing.metadata?.kind || "unknown");
    const kindIncoming = String(incoming.metadata?.kind || "unknown");

    const observedA = Array.isArray(existing.metadata?.observed_in) ? existing.metadata.observed_in : [];
    const observedB = Array.isArray(incoming.metadata?.observed_in) ? incoming.metadata.observed_in : [];
    const observed = Array.from(new Set([...observedA, ...observedB]));

    return {
        ...existing,
        is_measure: Boolean(existing.is_measure || incoming.is_measure),
        metadata: {
            ...(existing.metadata || {}),
            ...(incoming.metadata || {}),
            source: "sdk_operational",
            kind: kindExisting === "unknown" && kindIncoming !== "unknown" ? kindIncoming : kindExisting,
            observed_in: observed,
        },
    };
}

function createOperationalScanOverlay() {
    if (typeof window === "undefined") return null;

    const styleId = "pbi-operational-scan-overlay-style";
    if (!document.getElementById(styleId)) {
        const style = document.createElement("style");
        style.id = styleId;
        style.textContent = `
@keyframes pbiSpin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
.pbi-operational-overlay { position: fixed; inset: 0; z-index: 2147483647; background: rgba(10, 14, 24, 0.78); display: flex; align-items: center; justify-content: center; }
.pbi-operational-card { width: min(520px, calc(100vw - 32px)); border-radius: 16px; padding: 20px 18px; background: rgba(15, 20, 34, 0.88); border: 1px solid rgba(255, 255, 255, 0.12); box-shadow: 0 10px 30px rgba(0,0,0,0.35); color: #E7EAF3; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; }
.pbi-operational-row { display: flex; gap: 14px; align-items: center; }
.pbi-operational-spinner { width: 28px; height: 28px; border-radius: 9999px; border: 3px solid rgba(255,255,255,0.18); border-top-color: rgba(99,102,241,0.95); animation: pbiSpin 0.9s linear infinite; flex: 0 0 auto; }
.pbi-operational-title { font-weight: 800; font-size: 14px; letter-spacing: 0.2px; margin: 0; }
.pbi-operational-sub { margin: 2px 0 0; font-size: 12px; color: rgba(231,234,243,0.75); line-height: 1.35; }
`;
        document.head.appendChild(style);
    }

    const overlay = document.createElement("div");
    overlay.className = "pbi-operational-overlay";
    overlay.innerHTML = `
  <div class="pbi-operational-card">
    <div class="pbi-operational-row">
      <div class="pbi-operational-spinner"></div>
      <div>
        <p class="pbi-operational-title">Analizando estructura...</p>
        <p class="pbi-operational-sub" data-sub>Escaneando visuales y filtros para sincronizar columnas en uso.</p>
      </div>
    </div>
  </div>
`;

    document.body.appendChild(overlay);

    return {
        update(subText: string) {
            const el = overlay.querySelector("[data-sub]");
            if (el) el.textContent = subText;
        },
        close() {
            overlay.remove();
        },
    };
}

function extractFilterTargets(filter: any): Array<{ table?: string; column?: string; measure?: string }> {
    const targets: Array<{ table?: string; column?: string; measure?: string }> = [];
    if (!filter || typeof filter !== "object") return targets;

    const maybeTargets = (filter as any).targets;
    const maybeTarget = (filter as any).target;

    if (Array.isArray(maybeTargets)) {
        for (const t of maybeTargets) {
            if (t && typeof t === "object") targets.push(t);
        }
    } else if (maybeTarget && typeof maybeTarget === "object") {
        targets.push(maybeTarget);
    }

    return targets;
}

function getFieldTableAndColumn(field: any): { table?: string; column?: string; isMeasure?: boolean; kind?: string } {
    if (!field || typeof field !== "object") return {};
    const table = typeof field.table === "string" ? field.table : undefined;
    const column =
        typeof field.column === "string"
            ? field.column
            : typeof field.measure === "string"
                ? field.measure
                : undefined;
    const isMeasure = Boolean(field.isMeasure || field.is_measure || typeof field.measure === "string");
    const kind =
        typeof field.kind === "string"
            ? field.kind
            : isMeasure
                ? "measure"
                : "unknown";
    return { table, column, isMeasure, kind };
}

export async function scanOperationalSchema(report: any): Promise<OperationalColumnSchema[]> {
    const overlay = createOperationalScanOverlay();

    const add = (map: Map<string, OperationalColumnSchema>, incoming: OperationalColumnSchema) => {
        const key = normalizeSchemaKey(incoming.table_name, incoming.column_name);
        const existing = map.get(key);
        map.set(key, existing ? mergeOperationalSchema(existing, incoming) : incoming);
    };

    const out = new Map<string, OperationalColumnSchema>();

    let initialPage: any = null;
    let didMovePage = false;

    try {
        if (!report) throw new Error("No hay reporte activo en Power BI.");

        if (typeof report.getActivePage === "function") {
            initialPage = await report.getActivePage();
        }

        overlay?.update("Leyendo filtros del reporte...");
        if (typeof report.getFilters === "function") {
            const reportFilters = await report.getFilters();
            for (const f of reportFilters || []) {
                for (const t of extractFilterTargets(f)) {
                    const table = typeof t.table === "string" ? t.table.trim() : "";
                    const column = typeof (t as any).measure === "string" ? String((t as any).measure) : String(t.column || "");
                    if (!table || !column) continue;
                    add(out, {
                        table_name: table,
                        column_name: column,
                        data_type: "Texto",
                        description: "",
                        is_measure: typeof (t as any).measure === "string",
                        dax_expression: "",
                        sample_values: [],
                        metadata: { source: "sdk_operational", kind: typeof (t as any).measure === "string" ? "measure" : "unknown", observed_in: ["filter:report"] },
                    });
                }
            }
        }

        overlay?.update("Enumerando páginas...");
        const pages: any[] = typeof report.getPages === "function" ? await report.getPages() : [];
        const rolesToProbe = [
            "Category",
            "Axis",
            "X",
            "Y",
            "Y2",
            "Values",
            "Rows",
            "Columns",
            "Series",
            "Legend",
            "Tooltips",
            "Details",
        ];

        for (let pageIndex = 0; pageIndex < (pages?.length || 0); pageIndex++) {
            const page = pages[pageIndex];
            const pageName = String(page?.name || page?.displayName || `page_${pageIndex}`);

            overlay?.update(`Escaneando página: ${pageName}`);

            if (typeof page?.getFilters === "function") {
                try {
                    const pageFilters = await page.getFilters();
                    for (const f of pageFilters || []) {
                        for (const t of extractFilterTargets(f)) {
                            const table = typeof t.table === "string" ? t.table.trim() : "";
                            const column = typeof (t as any).measure === "string" ? String((t as any).measure) : String(t.column || "");
                            if (!table || !column) continue;
                            add(out, {
                                table_name: table,
                                column_name: column,
                                data_type: "Texto",
                                description: "",
                                is_measure: typeof (t as any).measure === "string",
                                dax_expression: "",
                                sample_values: [],
                                metadata: {
                                    source: "sdk_operational",
                                    kind: typeof (t as any).measure === "string" ? "measure" : "unknown",
                                    observed_in: [`filter:page:${pageName}`],
                                },
                            });
                        }
                    }
                } catch {
                    // best-effort
                }
            }

            let visuals: any[] = [];
            if (typeof page?.getVisuals === "function") {
                try {
                    visuals = await page.getVisuals();
                } catch {
                    visuals = [];
                }
            }

            // Fallback: algunas APIs requieren página activa para acceder a visuals/fields.
            if ((!visuals || visuals.length === 0) && typeof page?.setActive === "function") {
                try {
                    await page.setActive();
                    didMovePage = true;
                    visuals = typeof page.getVisuals === "function" ? await page.getVisuals() : [];
                } catch {
                    visuals = [];
                }
            }

            for (const visual of visuals || []) {
                const vName = String(visual?.name || visual?.title || visual?.type || "visual");

                if (typeof visual?.getFilters === "function") {
                    try {
                        const vFilters = await visual.getFilters();
                        for (const f of vFilters || []) {
                            for (const t of extractFilterTargets(f)) {
                                const table = typeof t.table === "string" ? t.table.trim() : "";
                                const column = typeof (t as any).measure === "string" ? String((t as any).measure) : String(t.column || "");
                                if (!table || !column) continue;
                                add(out, {
                                    table_name: table,
                                    column_name: column,
                                    data_type: "Texto",
                                    description: "",
                                    is_measure: typeof (t as any).measure === "string",
                                    dax_expression: "",
                                    sample_values: [],
                                    metadata: {
                                        source: "sdk_operational",
                                        kind: typeof (t as any).measure === "string" ? "measure" : "unknown",
                                        observed_in: [`filter:visual:${pageName}:${vName}`],
                                    },
                                });
                            }
                        }
                    } catch {
                        // best-effort
                    }
                }

                if (typeof visual?.getSlicerState === "function") {
                    try {
                        const state = await visual.getSlicerState();
                        const targets = Array.isArray(state?.targets) ? state.targets : [];
                        for (const t of targets) {
                            const table = typeof (t as any).table === "string" ? String((t as any).table).trim() : "";
                            const column = typeof (t as any).column === "string" ? String((t as any).column).trim() : "";
                            if (!table || !column) continue;
                            add(out, {
                                table_name: table,
                                column_name: column,
                                data_type: "Texto",
                                description: "",
                                is_measure: false,
                                dax_expression: "",
                                sample_values: [],
                                metadata: {
                                    source: "sdk_operational",
                                    kind: "unknown",
                                    observed_in: [`slicer:${pageName}:${vName}`],
                                },
                            });
                        }
                    } catch {
                        // best-effort
                    }
                }

                if (typeof visual?.getDataFields !== "function") continue;
                for (const role of rolesToProbe) {
                    let fields: any[] = [];
                    try {
                        fields = await visual.getDataFields(role);
                    } catch {
                        fields = [];
                    }
                    for (const field of fields || []) {
                        const info = getFieldTableAndColumn(field);
                        const table = typeof info.table === "string" ? info.table.trim() : "";
                        const column = typeof info.column === "string" ? info.column.trim() : "";
                        if (!table || !column) continue;

                        add(out, {
                            table_name: table,
                            column_name: column,
                            data_type: "Texto",
                            description: "",
                            is_measure: Boolean(info.isMeasure),
                            dax_expression: "",
                            sample_values: [],
                            metadata: {
                                source: "sdk_operational",
                                kind: info.kind || "unknown",
                                observed_in: [`datafield:${role}:${pageName}:${vName}`],
                            },
                        });
                    }
                }
            }
        }
    } finally {
        try {
            if (didMovePage && initialPage && typeof initialPage.setActive === "function") {
                await initialPage.setActive();
            }
        } catch {
            // best-effort
        }
        overlay?.close();
    }

    return Array.from(out.values());
}

/** Devuelve las tablas descubiertas (para debugging/logging) */
export function getDiscoveredTables(): string[] {
    return [..._discoveredTables];
}
