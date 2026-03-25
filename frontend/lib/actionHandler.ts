/**
 * Action Handler — Bridges AI-generated VisualAction JSON with Power BI JS SDK.
 */

import type { ChatResponse, VisualAction } from "@/lib/types";
import type { models } from "powerbi-client";
import { getActivePowerBiReport, resolveRealTableName, getDiscoveredTables, discoverModelTables } from "@/lib/pbiRuntime";

export interface ActionResult {
    success: boolean;
    message: string;
    operation: string;
    appliedToReport: boolean;
}

const PBI_VISUAL_TYPE_MAP: Record<string, string> = {
    barChart: "clusteredBarChart",
    columnChart: "clusteredColumnChart",
    lineChart: "lineChart",
    pieChart: "pieChart",
    donutChart: "donutChart",
    card: "card",
    table: "tableEx",
    matrix: "matrix",
    gauge: "gauge",
    areaChart: "areaChart",
    scatterChart: "scatterPlot",
};

const THEME_PRESETS: Record<string, Record<string, unknown>> = {
    light: {
        name: "ai-light",
        foreground: "#1f2937",
        background: "#ffffff",
        tableAccent: "#2563eb",
    },
    dark: {
        name: "ai-dark",
        foreground: "#f3f4f6",
        background: "#111827",
        tableAccent: "#22d3ee",
    },
    corporate: {
        name: "ai-corporate",
        foreground: "#111827",
        background: "#f8fafc",
        tableAccent: "#0ea5e9",
    },
};

type FormatMapEntry = {
    objectName: string;
    propertyName: string;
    formatValue: (value: unknown) => unknown;
};

const FORMAT_MAP: Record<string, FormatMapEntry[]> = {
    title: [
        {
            objectName: "title",
            propertyName: "titleText",
            formatValue: (value) => String(value),
        },
    ],
    titleText: [
        {
            objectName: "title",
            propertyName: "titleText",
            formatValue: (value) => String(value),
        },
    ],
    showLegend: [
        {
            objectName: "legend",
            propertyName: "visible",
            formatValue: (value) => Boolean(value),
        },
    ],
    showDataLabels: [
        {
            objectName: "dataLabels",
            propertyName: "visible",
            formatValue: (value) => Boolean(value),
        },
    ],
};

function supportsFormatProperty(capabilities: any, mapping: FormatMapEntry): boolean {
    const objects = capabilities?.objects;
    if (!objects || typeof objects !== "object") return true;

    const objectNode = objects[mapping.objectName];
    if (!objectNode || typeof objectNode !== "object") return false;

    const properties = objectNode.properties ?? objectNode;
    if (!properties || typeof properties !== "object") return false;

    return Boolean(properties[mapping.propertyName]);
}

function setPropertyReturnedError(result: any): boolean {
    if (!result || typeof result !== "object") return false;
    if ("error" in result && result.error) return true;
    if ("errors" in result && Array.isArray(result.errors) && result.errors.length > 0) return true;
    if ("success" in result && result.success === false) return true;
    return false;
}

function getReportInstance(): any | null {
    return getActivePowerBiReport();
}

async function getActivePage(report: any): Promise<any | null> {
    if (!report) return null;
    if (typeof report.getActivePage === "function") {
        try {
            return await report.getActivePage();
        } catch {
            // fallback below
        }
    }
    if (typeof report.getPages === "function") {
        try {
            const pages = await report.getPages();
            return pages.find((p: any) => p.isActive) || pages[0] || null;
        } catch {
            return null;
        }
    }
    return null;
}

async function resolveVisualByTechnicalName(activePage: any, targetVisualName: string): Promise<any | null> {
    if (!activePage || typeof activePage.getVisuals !== "function") return null;
    const visuals = await activePage.getVisuals();
    if (!Array.isArray(visuals) || visuals.length === 0) return null;
    return visuals.find((v: any) => String(v?.name || "").trim() === targetVisualName) || null;
}

async function resolveVisualByTechnicalNameOrTitle(activePage: any, targetRef: string): Promise<any | null> {
    if (!activePage || typeof activePage.getVisuals !== "function") return null;
    const visuals = await activePage.getVisuals();
    if (!Array.isArray(visuals) || visuals.length === 0) return null;

    const normalizedTarget = String(targetRef || "").trim().toLowerCase();
    if (!normalizedTarget) return null;

    const exactTechnicalMatch =
        visuals.find((v: any) => String(v?.name || "").trim().toLowerCase() === normalizedTarget) || null;
    if (exactTechnicalMatch) return exactTechnicalMatch;

    const visualTitles = await Promise.all(
        visuals.map(async (visual: any) => {
            try {
                if (typeof visual?.getProperty === "function") {
                    const titleResult = await visual.getProperty({
                        objectName: "title",
                        propertyName: "titleText",
                    });
                    const titleText =
                        typeof titleResult === "string"
                            ? titleResult
                            : typeof titleResult?.value === "string"
                                ? titleResult.value
                                : "";
                    return {
                        visual,
                        title: String(titleText || "").trim(),
                    };
                }
            } catch {
                // ignore title lookup failures
            }

            return {
                visual,
                title: String(visual?.title || "").trim(),
            };
        }),
    );

    const exactTitleMatch =
        visualTitles.find((item) => item.title.toLowerCase() === normalizedTarget)?.visual || null;
    if (exactTitleMatch) return exactTitleMatch;

    const partialTitleMatch =
        visualTitles.find((item) => item.title.toLowerCase().includes(normalizedTarget))?.visual || null;
    return partialTitleMatch;
}

function resolveActions(payload: VisualAction | ChatResponse): VisualAction[] {
    if (payload && typeof payload === "object" && "operation" in payload) {
        return [payload as VisualAction];
    }
    const response = payload as ChatResponse;
    if (Array.isArray(response.actions) && response.actions.length > 0) {
        return response.actions;
    }
    return response.action ? [response.action] : [];
}

async function getEditableActivePage(report: any): Promise<any | null> {
    const attempts = 6;
    for (let i = 0; i < attempts; i++) {
        let activePage: any | null = null;

        if (typeof report.getActivePage === "function") {
            try {
                activePage = await report.getActivePage();
            } catch {
                activePage = null;
            }
        }

        if (!activePage && typeof report.getPages === "function") {
            const pages = await report.getPages();
            activePage = pages.find((p: any) => p.isActive) || pages[0] || null;
        }

        if (activePage && typeof activePage.createVisual === "function") {
            return activePage;
        }

        try {
            if (typeof report.switchMode === "function") {
                await report.switchMode("edit");
            }
        } catch {
            // no-op
        }

        await new Promise((resolve) => setTimeout(resolve, 700));
    }
    return null;
}

function parsePowerBiCsvToJson(csvString: string): Array<Record<string, string | number | boolean | null>> {
    const rows: string[][] = [];
    let currentRow: string[] = [];
    let currentCell = "";
    let inQuotes = false;

    for (let i = 0; i < csvString.length; i++) {
        const char = csvString[i];
        const next = csvString[i + 1];

        if (char === '"') {
            if (inQuotes && next === '"') {
                currentCell += '"';
                i += 1;
            } else {
                inQuotes = !inQuotes;
            }
            continue;
        }

        if (char === "," && !inQuotes) {
            currentRow.push(currentCell);
            currentCell = "";
            continue;
        }

        if ((char === "\n" || char === "\r") && !inQuotes) {
            if (char === "\r" && next === "\n") {
                i += 1;
            }
            currentRow.push(currentCell);
            rows.push(currentRow);
            currentRow = [];
            currentCell = "";
            continue;
        }

        currentCell += char;
    }

    if (currentCell.length > 0 || currentRow.length > 0) {
        currentRow.push(currentCell);
        rows.push(currentRow);
    }

    if (rows.length === 0) return [];

    const headers = rows[0].map((header) => String(header || "").trim());
    const bodyRows = rows.slice(1).filter((row) => row.some((cell) => String(cell || "").trim() !== ""));

    return bodyRows.map((row) => {
        const record: Record<string, string | number | boolean | null> = {};

        headers.forEach((header, index) => {
            const rawValue = String(row[index] ?? "").trim();
            if (!header) return;

            if (rawValue === "") {
                record[header] = null;
                return;
            }

            if (/^(true|false)$/i.test(rawValue)) {
                record[header] = rawValue.toLowerCase() === "true";
                return;
            }

            if (/^-?\d+(\.\d+)?$/.test(rawValue)) {
                const numericValue = Number(rawValue);
                record[header] = Number.isFinite(numericValue) ? numericValue : rawValue;
                return;
            }

            record[header] = rawValue;
        });

        return record;
    });
}

type DataRoleBinding = {
    table?: string;
    column?: string;
    ref?: string;
    measure?: string;
    aggregation?: string;
};

function mapAggregationFunction(aggregation: string): string | null {
    const normalized = String(aggregation || "").trim().toLowerCase();
    const mapping: Record<string, string> = {
        sum: "Sum",
        average: "Average",
        avg: "Average",
        count: "Count",
        min: "Min",
        max: "Max",
        distinctcount: "DistinctCount",
    };
    return mapping[normalized] || null;
}

function buildSimpleAggDax(aggregationFunction: string, table: string, column: string): string | null {
    const agg = String(aggregationFunction || "").trim().toLowerCase();
    const t = String(table || "").trim();
    const c = String(column || "").trim();
    if (!t || !c) return null;

    const ref = `'${t}'[${c}]`;
    switch (agg) {
        case "sum":
            return `SUM(${ref})`;
        case "average":
        case "avg":
            return `AVERAGE(${ref})`;
        case "count":
            // COUNT() falla en texto; COUNTA es segura para texto/número.
            return `COUNTA(${ref})`;
        case "distinctcount":
            return `DISTINCTCOUNT(${ref})`;
        case "min":
            return `MIN(${ref})`;
        case "max":
            return `MAX(${ref})`;
        default:
            return null;
    }
}

function parseTableColumnRef(fieldRef: string): { table: string; column: string } | null {
    const match = fieldRef.match(/^\s*'?([^'\[]+?)'?\s*\[\s*([^\]]+)\s*]\s*$/);
    if (!match) return null;
    return {
        table: match[1].trim(),
        column: match[2].trim(),
    };
}

function isLikelyMeasureRole(logicalRole: string): boolean {
    const role = logicalRole.toLowerCase();
    return role === "y" || role === "values" || role === "value" || role === "measure";
}

function isLikelyCategoryRole(logicalRole: string): boolean {
    const role = logicalRole.toLowerCase();
    return role === "category" || role === "x" || role === "axis" || role === "series" || role === "legend";
}

function normalizeDataRoleBinding(
    roleName: string,
    roleValue: unknown
): { table: string; column: string; isMeasureField: boolean } | null {
    if (typeof roleValue === "string") {
        const parsed = parseTableColumnRef(roleValue);
        if (!parsed) return null;
        return {
            table: parsed.table,
            column: parsed.column,
            isMeasureField: isLikelyMeasureRole(roleName),
        };
    }

    if (!roleValue || typeof roleValue !== "object") return null;
    const binding = roleValue as DataRoleBinding;

    if (binding.table && binding.column) {
        return {
            table: String(binding.table).trim(),
            column: String(binding.column).trim(),
            isMeasureField: Boolean(binding.aggregation) || Boolean(binding.measure) || isLikelyMeasureRole(roleName),
        };
    }

    if (binding.ref && typeof binding.ref === "string") {
        const parsed = parseTableColumnRef(binding.ref);
        if (!parsed) return null;
        return {
            table: parsed.table,
            column: parsed.column,
            isMeasureField: Boolean(binding.aggregation) || Boolean(binding.measure) || isLikelyMeasureRole(roleName),
        };
    }

    return null;
}

function normalizeFilterValues(values: Array<string | number | boolean>): Array<string | number | boolean> {
    // No coerción: respetar estrictamente el tipo enviado por backend.
    return values.map((v) => (typeof v === "string" ? v.trim() : v));
}

function mapFilterOperator(op: string): string {
    const map: Record<string, string> = {
        "=": "In",
        "==": "In",
        eq: "In",
        in: "In",
        "!=": "NotIn",
        ne: "NotIn",
        not_in: "NotIn",
        notin: "NotIn",
        contains: "Contains",
        startswith: "StartsWith",
    };
    return map[(op || "In").toLowerCase()] || "In";
}

function buildVisualFilters(action: VisualAction): models.IFilter[] {
    if (!action.filters || action.filters.length === 0) return [];

    return action.filters.map((f) => {
        const normalizedOperator = (f.operator || "In").toLowerCase();
        const rawValues = normalizeFilterValues(
            f.values && f.values.length > 0 ? f.values : [""]
        );
        const target: models.IFilterColumnTarget = {
            table: resolveRealTableName(f.table),
            column: f.column,
        };

        if ([">", ">=", "<", "<=", "!=", "ne", "not_equal"].includes(normalizedOperator)) {
            return {
                $schema: "http://powerbi.com/product/schema#advanced",
                target,
                logicalOperator: "And",
                conditions: [
                    {
                        operator:
                            normalizedOperator === ">" ? "GreaterThan" :
                                normalizedOperator === ">=" ? "GreaterThanOrEqual" :
                                    normalizedOperator === "<" ? "LessThan" :
                                        normalizedOperator === "<=" ? "LessThanOrEqual" :
                                            "NotEquals",
                        value: rawValues[0],
                    },
                ],
                filterType: 0,
            } as models.IAdvancedFilter;
        }

        return {
            $schema: "http://powerbi.com/product/schema#basic",
            target,
            operator: mapFilterOperator(f.operator),
            values: rawValues,
            filterType: 1,
        } as models.IBasicFilter;
    });
}

function getRoleCandidatesForVisual(
    pbiVisualType: string,
    logicalRole: string,
    isMeasureField: boolean
): string[] {
    const visual = (pbiVisualType || "").toLowerCase();
    const normalizedRole = logicalRole.trim();

    if (isMeasureField) {
        // FASE 14-FIX: clusteredBarChart en PBI SDK SOLO acepta "Y" como rol
        // de medida (kind=1). "X" y "Values" no existen como roles válidos.
        // Intentar addDataField("X") o addDataField("Values") causa errores
        // silenciosos que corrompen el estado de renderizado del visual,
        // dejando el gráfico en blanco a pesar de que "Y" luego funcione.
        // Solución: usar Y directamente para TODOS los gráficos de barras/columnas.
        if (visual === "clusteredbarchart" || visual === "barchart") return ["Y"];
        if (visual === "clusteredcolumnchart" || visual === "columnchart" || visual === "linechart" || visual === "areachart") return ["Y"];
        if (visual === "piechart" || visual === "donutchart") return ["Values", "Y"];
        if (visual === "gauge") return ["Value", "Target", "Y", "Values"];
        // Card: en muchos tenants/SDKs el rol efectivo es "Fields" (no "Values").
        if (visual === "card") return ["Fields", "Values", "Y"];
        return [normalizedRole, "Y", "Values"];
    }

    if (visual === "piechart" || visual === "donutchart") return ["Legend", "Category", "Series", "Details"];
    if (visual === "gauge") return ["Target", "Maximum", "Minimum", "Value"];
    if (visual === "card") return ["Fields", "Values", "Y"];
    return [normalizedRole, "Category", "Axis", "Series", "Legend", "Details"];
}

function normalizeDaxExpressionForVisualCalculation(daxExpression: string, daxName: string): string {
    const expression = String(daxExpression || "").trim();
    const measureName = String(daxName || "").trim();
    if (!expression) return "";

    let result = expression;

    // Quitar prefijo de asignación: "MeasureName = SUM(...)" → "SUM(...)"
    if (measureName) {
        const escapedMeasureName = measureName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        const assignmentPattern = new RegExp(`^${escapedMeasureName}\\s*=\\s*`, "i");
        result = result.replace(assignmentPattern, "").trim();
    }

    // FASE 10+: Resolver nombres de tabla dentro del DAX.
    // El backend usa nombres del diccionario semántico (ej: 'API-DatosPrueba_Final')
    // pero PBI puede tener un nombre interno diferente (ej: 'Tabla').
    // Reemplazar TODAS las referencias 'tableName' con el nombre real.
    result = result.replace(/'([^']+)'/g, (_match, tableName: string) => {
        const resolved = resolveRealTableName(tableName);
        if (resolved !== tableName) {
            console.log(`🔄 DAX table resolved: '${tableName}' → '${resolved}'`);
        }
        return `'${resolved}'`;
    });

    return result;
}

async function getSupportedRoleNames(visual: any): Promise<string[]> {
    if (!visual || typeof visual.getCapabilities !== "function") return [];
    try {
        const caps = await visual.getCapabilities();
        const roles = (caps as any)?.dataRoles;
        if (!Array.isArray(roles)) return [];
        const names = roles
            .map((r: any) => String(r?.name || r?.displayName || "").trim())
            .filter(Boolean);
        return Array.from(new Set(names));
    } catch {
        return [];
    }
}

async function applyCardFieldFormatIfNeeded(
    visual: any,
    pbiVisualType: string,
    dataRole: string
): Promise<void> {
    if (String(pbiVisualType || "").trim().toLowerCase() !== "card") return;
    if (typeof visual?.setFieldFormatString !== "function" || typeof visual?.getDataFields !== "function") return;

    try {
        const fields = await visual.getDataFields(dataRole);
        if (!Array.isArray(fields) || fields.length === 0) return;
        const lastIndex = fields.length - 1;
        await visual.setFieldFormatString(dataRole, lastIndex, "#,0");
    } catch {
        // ignore formatting failures for non-supported visuals
    }
}

async function addFieldWithRoleFallback(
    visual: any,
    pbiVisualType: string,
    roleName: string,
    roleValue: unknown,
    action?: VisualAction
): Promise<{ ok: boolean; message?: string }> {
    // FASE 8: Frontend Matrix Role Mapper estricto
    // El SDK exige 'Rows' y 'Columns' para matrices, sobreescribimos las alucinaciones del LLM
    if (String(pbiVisualType || "").trim().toLowerCase() === "matrix") {
        const lowerRole = roleName.toLowerCase();
        if (lowerRole === "category" || lowerRole === "axis") {
            roleName = "Rows";
        } else if (lowerRole === "series" || lowerRole === "legend") {
            roleName = "Columns";
        }
    }

    const normalized = normalizeDataRoleBinding(roleName, roleValue);
    if (!normalized) {
        return {
            ok: false,
            message: `Referencia inválida para rol "${roleName}". Formato esperado: Tabla[Columna] o {table,column}.`,
        };
    }

    // FASE 10: Dynamic Table Resolution — corregir nombre de tabla del LLM
    // al nombre real descubierto del modelo de Power BI en runtime.
    normalized.table = resolveRealTableName(normalized.table);

    const isMeasure = normalized.isMeasureField && !isLikelyCategoryRole(roleName);
    const isCardVisual = String(pbiVisualType || "").trim().toLowerCase() === "card";
    let candidates = getRoleCandidatesForVisual(pbiVisualType, roleName, isMeasure);
    const supported = await getSupportedRoleNames(visual);
    if (supported.length) {
        const supportedSet = new Set(supported.map((s) => s.toLowerCase()));
        const filtered = candidates.filter((c) => supportedSet.has(String(c).toLowerCase()));
        if (filtered.length) candidates = filtered;
    }
    const daxExpression = normalizeDaxExpressionForVisualCalculation(
        String(action?.dax || ""),
        String(action?.dax_name || ""),
    );
    const daxName = String(action?.dax_name || "").trim();

    // FASE 5.2-DEFINITIVO: Visual Calculations del PBI SDK no soportan
    // VAR/CALCULATE/FILTER/RETURN (NativeVisualCalculationError confirmado).
    // Y no existe createMeasure() en el SDK.
    // Time Intelligence se resuelve con filtros sobre Periodo_Mes.
    // → TODO daxExpression complejo se IGNORA en el frontend.
    // → Solo se permite conversión de DAX simple (SUM/AVG) a column binding.
    const SIMPLE_AGG_RE = /^(SUM|AVERAGE|AVG|COUNT|COUNTA|MIN|MAX)\s*\(\s*'([^']+)'\[([^\]]+)\]\s*\)$/i;
    const simpleAggMatch = daxExpression ? SIMPLE_AGG_RE.exec(daxExpression) : null;
    const simpleAggInfo = simpleAggMatch ? {
        fn: simpleAggMatch[1],
        table: simpleAggMatch[2],
        column: simpleAggMatch[3],
        mappedAgg: mapAggregationFunction(simpleAggMatch[1]) || "Sum",
        daxExpression,
    } : null;
    const triedMeasureFallbackRoles = new Set<string>();

    for (const roleCandidate of candidates) {
        let basePayload: any = null;
        try {
            if (simpleAggInfo && isMeasure) {
                // DAX simple (SUM/AVG/COUNT) → Column binding con aggregationFunction
                basePayload = {
                    $schema: "http://powerbi.com/product/schema#column",
                    table: simpleAggInfo.table,
                    column: simpleAggInfo.column,
                    aggregationFunction: simpleAggInfo.mappedAgg,
                };
                console.log(`🔄 DAX simple convertido a column binding: ${simpleAggInfo.daxExpression} → {table: "${simpleAggInfo.table}", column: "${simpleAggInfo.column}", agg: "${simpleAggInfo.mappedAgg}"}`);
            } else if (normalized.isMeasureField && typeof roleValue === "object" && roleValue !== null && mapAggregationFunction(String((roleValue as DataRoleBinding).aggregation || ""))) {
                basePayload = {
                    $schema: "http://powerbi.com/product/schema#column",
                    table: normalized.table,
                    column: normalized.column,
                    aggregationFunction: mapAggregationFunction(String((roleValue as DataRoleBinding).aggregation || "")),
                };
            } else {
                basePayload = {
                    $schema: "http://powerbi.com/product/schema#column",
                    table: normalized.table,
                    column: normalized.column,
                };
            }

            // FASE 11: Forzar agregación para CUALQUIER columna numérica en rol de medida
            if (isMeasure && !basePayload.daxExpression && !basePayload.aggregationFunction) {
                basePayload.aggregationFunction = "Sum";
            }

            // FASE 3/4 (ZERO-TRUST): Interceptor incondicional de Auto-Date
            // Purgamos explícitamente cualquier asunción de jerarquía temporal en CUALQUIER rol
            // para que Power BI jamás aplique "Auto-Date".
            delete basePayload.hierarchyLevel;
            delete basePayload.property;

            // DIAGNÓSTICO: Log del payload exacto que se envía al SDK
            if (process.env.NODE_ENV !== "production") {
                console.log(`📊 addDataField("${roleCandidate}", ${JSON.stringify(basePayload)}) en visual "${pbiVisualType}"`);
            }

            const result = await visual.addDataField(roleCandidate, basePayload);

            // DIAGNÓSTICO: Log del resultado del SDK
            if (process.env.NODE_ENV !== "production") {
                console.log(`✅ addDataField("${roleCandidate}") → resultado:`, result);
            }

            await applyCardFieldFormatIfNeeded(visual, pbiVisualType, roleCandidate);
            return { ok: true };
        } catch (err: any) {
            // Card: algunos tenants/SDKs aceptan "Average" y otros "Avg".
            // Intentar ambas variantes antes de caer al fallback de measure.
            if (
                isCardVisual &&
                basePayload &&
                basePayload.$schema === "http://powerbi.com/product/schema#column" &&
                typeof basePayload.aggregationFunction === "string" &&
                (basePayload.aggregationFunction === "Average" || basePayload.aggregationFunction === "Avg")
            ) {
                const altAgg = basePayload.aggregationFunction === "Average" ? "Avg" : "Average";
                try {
                    await visual.addDataField(roleCandidate, { ...basePayload, aggregationFunction: altAgg });
                    await applyCardFieldFormatIfNeeded(visual, pbiVisualType, roleCandidate);
                    return { ok: true };
                } catch {
                    // continue to existing fallbacks
                }
            }

            // Card macro-fix: el SDK suele rechazar bindings de columna para agregaciones
            // como DistinctCount/Count/Average (aunque vengan con aggregationFunction).
            // Fallback determinista: inyectar como measure inline con DAX simple.
            if (
                isCardVisual &&
                basePayload &&
                basePayload.$schema === "http://powerbi.com/product/schema#column" &&
                typeof basePayload.aggregationFunction === "string" &&
                String(basePayload.aggregationFunction).toLowerCase() !== "sum"
            ) {
                // DistinctCount en cards falla en algunos tenants. Fallback determinista:
                // degradar a Count (no-dedup) para evitar tarjetas vacías.
                // Nota: el backend/UI debe indicar esta degradación si el usuario pidió "únicos".
                if (String(basePayload.aggregationFunction).toLowerCase() === "distinctcount") {
                    try {
                        await visual.addDataField(roleCandidate, { ...basePayload, aggregationFunction: "Count" });
                        await applyCardFieldFormatIfNeeded(visual, pbiVisualType, roleCandidate);
                        return { ok: true };
                    } catch {
                        // continue to measure fallback
                    }
                }

                const key = `${roleCandidate}|${basePayload.aggregationFunction}`;
                if (!triedMeasureFallbackRoles.has(key)) {
                    triedMeasureFallbackRoles.add(key);
                    const expr = buildSimpleAggDax(basePayload.aggregationFunction, basePayload.table, basePayload.column);
                    if (expr) {
                        try {
                            const measurePayload = {
                                $schema: "http://powerbi.com/product/schema#measure",
                                table: basePayload.table,
                                name:
                                    daxName ||
                                    `Medida_${String(basePayload.aggregationFunction)}_${String(basePayload.column)}`.slice(0, 120),
                                expression: expr,
                            };
                            if (process.env.NODE_ENV !== "production") {
                                console.log(`🧩 Card agg fallback measure → addDataField("${roleCandidate}", ${JSON.stringify(measurePayload)})`);
                            }
                            await visual.addDataField(roleCandidate, measurePayload);
                            await applyCardFieldFormatIfNeeded(visual, pbiVisualType, roleCandidate);
                            return { ok: true };
                        } catch (fallbackErr: any) {
                            if (process.env.NODE_ENV !== "production") {
                                console.warn("⚠️ Card agg fallback (measure) falló:", fallbackErr?.message || fallbackErr);
                            }
                        }
                    }
                }
            }

            if (
                isCardVisual &&
                simpleAggInfo &&
                simpleAggInfo.mappedAgg !== "Sum" &&
                !triedMeasureFallbackRoles.has(roleCandidate)
            ) {
                triedMeasureFallbackRoles.add(roleCandidate);
                try {
                    const measurePayload = {
                        $schema: "http://powerbi.com/product/schema#measure",
                        table: simpleAggInfo.table,
                        name: daxName || `Medida_${simpleAggInfo.mappedAgg}_${simpleAggInfo.column}`.slice(0, 120),
                        expression: simpleAggInfo.daxExpression,
                    };
                    if (process.env.NODE_ENV !== "production") {
                        console.log(`🧩 Card fallback measure → addDataField("${roleCandidate}", ${JSON.stringify(measurePayload)})`);
                    }
                    await visual.addDataField(roleCandidate, measurePayload);
                    await applyCardFieldFormatIfNeeded(visual, pbiVisualType, roleCandidate);
                    return { ok: true };
                } catch (fallbackErr: any) {
                    if (process.env.NODE_ENV !== "production") {
                        console.warn("⚠️ Card measure fallback falló:", fallbackErr?.message || fallbackErr);
                    }
                }
            }
            // DIAGNÓSTICO: Log del error exacto del SDK (antes era silencioso)
            if (process.env.NODE_ENV !== "production") {
                console.warn(`⚠️ addDataField("${roleCandidate}") falló:`, err?.message || err);
            }
            // continue con el siguiente candidato
        }
    }

    return {
        ok: false,
        message: `No se pudo inyectar el rol "${roleName}" en el visual "${pbiVisualType}" con candidatos: ${candidates.join(", ")}.`,
    };
}

async function applyCardDisplayUnitsIfNeeded(visual: any, pbiVisualType: string): Promise<void> {
    if (String(pbiVisualType || "").trim().toLowerCase() !== "card") return;
    if (typeof visual?.setProperty !== "function") return;

    let capabilities: any = null;
    if (typeof visual?.getCapabilities === "function") {
        try {
            capabilities = await visual.getCapabilities();
            if (process.env.NODE_ENV !== "production") {
                console.log("Card capabilities:", capabilities);
            }
        } catch {
            capabilities = null;
        }
    }

    const candidates = [
        { objectName: "calloutValue", propertyName: "displayUnits" },
        { objectName: "labels", propertyName: "displayUnits" },
        { objectName: "dataPoint", propertyName: "displayUnits" },
        { objectName: "categoryLabels", propertyName: "displayUnits" },
    ];

    for (const candidate of candidates) {
        if (capabilities && !supportsFormatProperty(capabilities, {
            objectName: candidate.objectName,
            propertyName: candidate.propertyName,
            formatValue: (value: unknown) => value,
        })) {
            continue;
        }

        try {
            await visual.setProperty(
                {
                    objectName: candidate.objectName,
                    propertyName: candidate.propertyName,
                },
                { value: 1 }
            );
            if (process.env.NODE_ENV !== "production") {
                console.log(`Card displayUnits applied using ${candidate.objectName}.${candidate.propertyName}`);
            }
            return;
        } catch (error) {
            if (process.env.NODE_ENV !== "production") {
                console.warn(`Card displayUnits failed on ${candidate.objectName}.${candidate.propertyName}`, error);
            }
        }
    }
}

async function applyFiltersWithVariants(targetVisual: any, action: VisualAction): Promise<boolean> {
    if (!action.filters || action.filters.length === 0) return true;
    const filters = buildVisualFilters(action);
    if (filters.length === 0) return true;
    if (!targetVisual || typeof targetVisual.setFilters !== "function") return false;
    try {
        if (process.env.NODE_ENV !== "production") {
            console.debug("Applying visual filters:", filters);
        }
        await targetVisual.setFilters(filters);
        return true;
    } catch {
        return false;
    }
}

function buildThemePayload(themeKey: string): Record<string, unknown> | null {
    const key = String(themeKey || "").trim().toLowerCase();
    return THEME_PRESETS[key] || null;
}

async function applyThemeIfRequested(report: any, action: VisualAction): Promise<void> {
    const themeKey = String(action.format?.theme || "").trim();
    if (!themeKey || typeof report?.applyTheme !== "function") return;

    const themePayload = buildThemePayload(themeKey);
    if (!themePayload) {
        console.warn(`Tema no soportado: "${themeKey}". Usa: ${Object.keys(THEME_PRESETS).join(", ")}`);
        return;
    }

    try {
        await report.applyTheme({ themeJson: themePayload });
        console.log(`✅ Tema aplicado al reporte: ${themeKey}`);
    } catch (themeError) {
        console.warn(`No se pudo aplicar theme "${themeKey}" en el reporte.`, themeError);
    }
}

async function applyCreateTitleIfRequested(visual: any, action: VisualAction): Promise<void> {
    const requestedTitle = String(action.format?.title || action.title || "").trim();
    if (!requestedTitle || typeof visual?.setProperty !== "function") return;

    try {
        // 1. Habilitar visibilidad del título (PBI usa "visible", no "show")
        await visual.setProperty(
            { objectName: "title", propertyName: "visible" },
            { value: true }
        );

        // 2. Establecer el texto del título
        const result = await visual.setProperty(
            { objectName: "title", propertyName: "titleText" },
            { value: requestedTitle }
        );

        if (setPropertyReturnedError(result)) {
            console.warn("No se pudo aplicar el título post-creación.", result);
            return;
        }
        console.log(`🏷️ Título aplicado: "${requestedTitle}"`);
    } catch (titleError) {
        console.warn("Fallo al forzar título post-creación en el visual.", titleError);
    }
}


/**
 * FASE 14: Aplica un filtro TopN nativo del SDK de Power BI.
 *
 * Usa ITopNFilter (FilterType.TopN = 5) para limitar los N elementos
 * principales o inferiores basados en una medida de orden.
 * Reemplaza el enfoque DAX (RANKX/TOPN) que era purgado por el sanitizer.
 *
 * Referencia SDK: TopNFilter(target, operator, itemCount, orderBy)
 */
async function applyTopNFilter(
    visual: any,
    action: VisualAction,
): Promise<void> {
    if (!action.top_n || typeof visual?.updateFilters !== "function") return;

    const { count, order_by_column, order_by_table, category_column, category_table, direction } = action.top_n;
    const realCatTable = resolveRealTableName(category_table);
    const realOrderTable = resolveRealTableName(order_by_table);

    const pbiModels = await import("powerbi-models");
    const topNFilter = new pbiModels.TopNFilter(
        { table: realCatTable, column: category_column },
        direction as "Top" | "Bottom",
        count,
        { table: realOrderTable, column: order_by_column, aggregationFunction: "Sum" }
    );

    console.log("🏆 TopN filter payload:", JSON.stringify(topNFilter));

    try {
        await visual.updateFilters(2, [topNFilter]);
        console.log(`🏆 TopN filter aplicado nativamente: ${direction} ${count} por "${order_by_column}"`);
    } catch (err) {
        console.warn("⚠️ No se pudo aplicar TopN filter:", err);
        try {
            const existingFilters = await visual.getFilters();
            existingFilters.push(topNFilter);
            await visual.setFilters(existingFilters);
            console.log(`🏆 TopN filter aplicado (fallback setFilters): ${direction} ${count}`);
        } catch (err2) {
            console.warn("⚠️ Fallback setFilters también falló:", err2);
        }
    }
}

/**
 * FASE 13v2: Smart Auto-Layout — Posiciona visuals dinámicamente
 * según el total de visuals en la página.
 *
 * Calcula columnas y filas óptimas para que TODOS los visuals
 * quepan dentro del viewport visible (PAGE_WIDTH × PAGE_HEIGHT),
 * escalando el tamaño de cada celda proporcionalmente.
 *
 * @param index - Índice del visual en la grilla (0-based)
 * @param totalVisuals - Cantidad total de visuals que habrá en la página
 */
function computeGridPosition(
    index: number,
    totalVisuals: number
): { x: number; y: number; width: number; height: number } {
    // Dimensiones del viewport visible del reporte embebido.
    // Usamos 1280×720 como zona segura: es el viewport real que el usuario ve
    // sin necesidad de scroll. La página puede ser más alta (2000px en embed config)
    // pero colocar visuals debajo de ~720px los hace "invisibles" sin scroll.
    const PAGE_WIDTH = 1280;
    const PAGE_HEIGHT = 720;
    const MARGIN = 20;
    const GAP = 10;
    const MIN_CELL_W = 250;
    const MIN_CELL_H = 180;

    const total = Math.max(1, totalVisuals);
    const usableW = PAGE_WIDTH - 2 * MARGIN;
    const usableH = PAGE_HEIGHT - 2 * MARGIN;

    // Determinar número óptimo de columnas:
    // - Para 1 visual: 1 columna (ocupa todo el ancho)
    // - Para 2: 2 columnas
    // - Para 3-4: 2 columnas
    // - Para 5-6: 3 columnas
    // - Para 7-9: 3 columnas
    // - Para 10+: máximo que quepa respetando MIN_CELL_W
    let cols: number;
    if (total <= 1) cols = 1;
    else if (total <= 4) cols = 2;
    else if (total <= 9) cols = 3;
    else cols = Math.min(4, Math.floor((usableW + GAP) / (MIN_CELL_W + GAP)));

    const rows = Math.ceil(total / cols);

    // Calcular tamaño de celda usando todo el espacio disponible
    const cellW = Math.max(MIN_CELL_W, Math.floor((usableW - (cols - 1) * GAP) / cols));
    const cellH = Math.max(MIN_CELL_H, Math.floor((usableH - (rows - 1) * GAP) / rows));

    const col = index % cols;
    const row = Math.floor(index / cols);

    return {
        x: MARGIN + col * (cellW + GAP),
        y: MARGIN + row * (cellH + GAP),
        width: cellW,
        height: cellH,
    };
}

type LayoutIntent = "kpi_top" | "chart_half" | "chart_full";

const KPI_VISUAL_TYPES = new Set(["card", "gauge"]);
const FULL_WIDTH_VISUAL_TYPES = new Set(["table", "matrix"]);

function normalizeLayoutIntent(value: unknown): LayoutIntent | "" {
    const raw = String(value || "").trim().toLowerCase();
    if (raw === "kpi_top" || raw === "chart_half" || raw === "chart_full") {
        return raw as LayoutIntent;
    }
    return "";
}

function getVisualTypeLower(visual: any): string {
    return String(visual?.type || visual?.visualType || "").trim().toLowerCase();
}

function inferVisualSpan(
    visual: any,
    usableW: number
): "kpi" | "chart_full" | "chart_half" {
    const visualType = getVisualTypeLower(visual);
    if (KPI_VISUAL_TYPES.has(visualType)) return "kpi";
    if (FULL_WIDTH_VISUAL_TYPES.has(visualType)) return "chart_full";

    const width = Number(visual?.layout?.width ?? 0);
    if (Number.isFinite(width) && width >= usableW * 0.8) return "chart_full";
    return "chart_half";
}

type IntentRelayout = {
    visual: any;
    layout: { x: number; y: number; width: number; height: number };
};

type IntentLayoutResult = {
    layout: { x: number; y: number; width: number; height: number; displayState: { mode: number } };
    relayouts: IntentRelayout[];
    debug: string;
};

function computeIntentLayout(
    existingVisuals: any[],
    incomingIntent: LayoutIntent
): IntentLayoutResult | null {
    if (!Array.isArray(existingVisuals)) return null;

    const PAGE_WIDTH = 1280;
    const PAGE_HEIGHT = 720;
    const MARGIN = 20;
    const GAP = 10;

    const usableW = PAGE_WIDTH - 2 * MARGIN;
    const kpiCols = 4;
    const kpiHeight = 140;
    const kpiCellW = Math.max(160, Math.floor((usableW - (kpiCols - 1) * GAP) / kpiCols));

    const kpiVisuals: any[] = [];
    const chartVisuals: Array<{ visual: any; span: "full" | "half" }> = [];

    for (const visual of existingVisuals) {
        const span = inferVisualSpan(visual, usableW);
        if (span === "kpi") {
            kpiVisuals.push(visual);
        } else {
            chartVisuals.push({ visual, span: span === "chart_full" ? "full" : "half" });
        }
    }

    const totalKpiCount = kpiVisuals.length + (incomingIntent === "kpi_top" ? 1 : 0);
    const kpiRows = Math.max(0, Math.ceil(totalKpiCount / kpiCols));
    const kpiAreaHeight = kpiRows > 0 ? kpiRows * kpiHeight + (kpiRows - 1) * GAP : 0;
    const chartTop = MARGIN + (kpiAreaHeight > 0 ? kpiAreaHeight + GAP : 0);
    const chartAreaHeight = Math.max(0, PAGE_HEIGHT - chartTop - MARGIN);
    const baseChartRowHeight = chartAreaHeight > 0 ? Math.floor((chartAreaHeight - GAP) / 2) : 260;
    const chartRowHeight = Math.max(220, Math.min(320, baseChartRowHeight));
    const chartColW = Math.max(300, Math.floor((usableW - GAP) / 2));

    const occupied: Array<{ left: boolean; right: boolean }> = [];
    const ensureRow = (row: number) => {
        if (!occupied[row]) {
            occupied[row] = { left: false, right: false };
        }
        return occupied[row];
    };

    const findSlot = (span: "full" | "half") => {
        for (let row = 0; row < 100; row += 1) {
            const state = ensureRow(row);
            if (span === "full") {
                if (!state.left && !state.right) return { row, col: 0 };
            } else {
                if (!state.left) return { row, col: 0 };
                if (!state.right) return { row, col: 1 };
            }
        }
        return { row: 0, col: 0 };
    };

    const markSlot = (row: number, col: number, span: "full" | "half") => {
        const state = ensureRow(row);
        if (span === "full") {
            state.left = true;
            state.right = true;
        } else if (col === 0) {
            state.left = true;
        } else {
            state.right = true;
        }
    };

    const chartLayoutFor = (row: number, col: number, span: "full" | "half") => ({
        x: MARGIN + col * (chartColW + GAP),
        y: chartTop + row * (chartRowHeight + GAP),
        width: span === "full" ? usableW : chartColW,
        height: chartRowHeight,
    });

    const relayouts: IntentRelayout[] = [];

    for (let i = 0; i < kpiVisuals.length; i += 1) {
        const col = i % kpiCols;
        const row = Math.floor(i / kpiCols);
        relayouts.push({
            visual: kpiVisuals[i],
            layout: {
                x: MARGIN + col * (kpiCellW + GAP),
                y: MARGIN + row * (kpiHeight + GAP),
                width: kpiCellW,
                height: kpiHeight,
            },
        });
    }

    for (const item of chartVisuals) {
        const slot = findSlot(item.span);
        const layout = chartLayoutFor(slot.row, slot.col, item.span);
        relayouts.push({ visual: item.visual, layout });
        markSlot(slot.row, slot.col, item.span);
    }

    let layoutForNew: IntentLayoutResult["layout"];
    if (incomingIntent === "kpi_top") {
        const index = kpiVisuals.length;
        const col = index % kpiCols;
        const row = Math.floor(index / kpiCols);
        layoutForNew = {
            x: MARGIN + col * (kpiCellW + GAP),
            y: MARGIN + row * (kpiHeight + GAP),
            width: kpiCellW,
            height: kpiHeight,
            displayState: { mode: 0 },
        };
    } else {
        const span = incomingIntent === "chart_full" ? "full" : "half";
        const slot = findSlot(span);
        const layout = chartLayoutFor(slot.row, slot.col, span);
        layoutForNew = { ...layout, displayState: { mode: 0 } };
        markSlot(slot.row, slot.col, span);
    }

    return {
        layout: layoutForNew,
        relayouts,
        debug: `intent=${incomingIntent} kpi=${kpiVisuals.length} charts=${chartVisuals.length}`,
    };
}

/**
 * FASE 13v2: Redistribuye los visuals EXISTENTES en la página
 * usando el Smart Auto-Layout. Se ejecuta ANTES de crear un nuevo visual
 * para reposicionar los existentes y dejar espacio para el nuevo.
 *
 * WHY: Llamar moveVisual DESPUÉS de inyectar datos en un visual recién
 * creado puede resetear data bindings no confirmados, dejando el visual
 * en blanco. Por eso redistribuimos los existentes ANTES de crear el nuevo.
 *
 * @param activePage - Página activa de Power BI con permisos de edición
 * @param existingVisuals - Lista de visuals existentes a redistribuir
 * @param newTotalVisuals - Cantidad total de visuals que habrá (existentes + 1 nuevo)
 */
async function redistributeExistingVisuals(
    activePage: any,
    existingVisuals: any[],
    newTotalVisuals: number
): Promise<void> {
    if (typeof activePage?.moveVisual !== "function") return;
    if (!Array.isArray(existingVisuals) || existingVisuals.length === 0) return;

    try {
        console.log(`🔄 Redistribuyendo ${existingVisuals.length} visuals existentes (total después: ${newTotalVisuals})...`);

        for (let i = 0; i < existingVisuals.length; i++) {
            const v = existingVisuals[i];
            if (!v?.name) continue;

            const pos = computeGridPosition(i, newTotalVisuals);
            try {
                await activePage.moveVisual(v.name, pos.x, pos.y, pos.width, pos.height);
            } catch (moveErr) {
                console.warn(`⚠️ No se pudo mover visual[${i}] "${v.name}":`, moveErr);
            }
        }

        console.log(`✅ Redistribución completada: ${existingVisuals.length} visuals reposicionados en viewport 1280×720`);
    } catch (err) {
        console.warn("⚠️ Error durante redistribución de visuals:", err);
    }
}

async function redistributeExistingVisualsByLayout(
    activePage: any,
    relayouts: IntentRelayout[]
): Promise<void> {
    if (typeof activePage?.moveVisual !== "function") return;
    if (!Array.isArray(relayouts) || relayouts.length === 0) return;

    try {
        for (const item of relayouts) {
            if (!item?.visual?.name) continue;
            const { x, y, width, height } = item.layout;
            try {
                await activePage.moveVisual(item.visual.name, x, y, width, height);
            } catch (moveErr) {
                console.warn(`⚠️ No se pudo mover visual "${item.visual.name}":`, moveErr);
            }
        }
    } catch (err) {
        console.warn("⚠️ Error durante redistribución intent-based:", err);
    }
}

async function clearRoleDataFields(visual: any, roleName: string): Promise<void> {
    if (typeof visual?.getDataFields !== "function" || typeof visual?.removeDataField !== "function") {
        return;
    }

    let roleFields: any[] = [];
    try {
        const allFields = await visual.getDataFields();
        if (allFields && typeof allFields === "object") {
            const roleValue = (allFields as Record<string, unknown>)[roleName];
            if (Array.isArray(roleValue)) roleFields = roleValue;
        }
    } catch {
        return;
    }

    for (const field of roleFields) {
        const candidates: Array<unknown[]> = [
            [roleName, field],
            [roleName, field?.queryName],
            [roleName, field?.target?.queryName],
            [roleName, field?.index],
            [roleName],
        ];

        let removed = false;
        for (const args of candidates) {
            if (args.some((x) => x === undefined || x === null || x === "")) continue;
            try {
                await visual.removeDataField(...args);
                removed = true;
                break;
            } catch {
                // try next signature
            }
        }

        if (!removed) {
            console.warn(`No se pudo limpiar field previo en rol ${roleName}.`, field);
        }
    }
}

async function syncUpdateDataRoles(
    visual: any,
    pbiVisualType: string,
    action: VisualAction
): Promise<{ ok: boolean; message?: string }> {
    const roleEntries = Object.entries(action.dataRoles || {});
    if (roleEntries.length === 0) return { ok: true };

    if (typeof visual?.addDataField !== "function") {
        return {
            ok: false,
            message: "El visual no soporta addDataField para UPDATE de ejes/métricas.",
        };
    }

    for (const [roleName, roleValue] of roleEntries) {
        const normalized = normalizeDataRoleBinding(roleName, roleValue);
        if (!normalized) {
            return {
                ok: false,
                message: `Binding inválido en dataRoles.${roleName} para operación UPDATE.`,
            };
        }

        const isMeasure = normalized.isMeasureField && !isLikelyCategoryRole(roleName);
        const candidates = getRoleCandidatesForVisual(pbiVisualType, roleName, isMeasure);

        for (const roleCandidate of candidates) {
            await clearRoleDataFields(visual, roleCandidate);
        }

        const injected = await addFieldWithRoleFallback(visual, pbiVisualType, roleName, roleValue, action);
        if (!injected.ok) {
            return {
                ok: false,
                message: injected.message || `No se pudo inyectar dataRoles.${roleName} en UPDATE.`,
            };
        }
    }

    return { ok: true };
}

export async function executeAction(payload: VisualAction | ChatResponse): Promise<ActionResult> {
    const actions = resolveActions(payload);
    const legacyOperation = actions[0]?.operation || "UNKNOWN";
    const report = getReportInstance();

    if (!report) {
        return {
            success: false,
            message: "Power BI report not loaded. Cannot execute action.",
            operation: legacyOperation,
            appliedToReport: false,
        };
    }

    if (actions.length === 0) {
        return {
            success: false,
            message: "No actions received from backend.",
            operation: legacyOperation,
            appliedToReport: false,
        };
    }

    try {
        let explanationResultText: string | null = null;
        for (const action of actions) {
            if (process.env.NODE_ENV !== "production") {
                console.log(`Ejecutando [${action.operation}] en visual [${action.visualType || action.targetVisualName || "n/a"}]...`);
            }

            let result: ActionResult;
            switch (action.operation) {
                case "CREATE":
                case "CREATE_VISUAL":
                    result = await handleCreateVisual(report, action);
                    break;

                case "FILTER":
                case "NAVIGATE":
                    result = action.operation === "FILTER"
                        ? await handleFilter(report, action)
                        : await handleNavigate(report, action);
                    break;

                case "UPDATE":
                    result = await handleUpdateVisual(report, action);
                    break;

                case "EXPLAIN":
                    result = await handleExplainVisual(report, action);
                    if (result && result.success) {
                        explanationResultText = result.message;
                    }
                    break;

                case "DELETE":
                    result = await handleDeleteVisual(report, action);
                    break;

                default:
                    result = {
                        success: false,
                        message: `Unknown operation: ${action.operation}`,
                        operation: action.operation,
                        appliedToReport: false,
                    };
            }

            if (!result.success) {
                return result;
            }
        }

        return {
            success: true,
            message: explanationResultText || `✅ ${actions.length} acción(es) ejecutadas secuencialmente.`,
            operation: actions[actions.length - 1]?.operation || legacyOperation,
            appliedToReport: false,
        };
    } catch (err: any) {
        console.error("Action execution error:", err);
        return {
            success: false,
            message: `Error: ${err.message || "Unknown error executing action"}`,
            operation: legacyOperation,
            appliedToReport: false,
        };
    }
}

async function handleUpdateVisual(
    report: any,
    action: VisualAction
): Promise<ActionResult> {
    try {
        const activePage = await getActivePage(report);
        if (!activePage || typeof activePage.getVisuals !== "function") {
            return {
                success: false,
                message: "No se encontró una página activa para actualizar el visual.",
                operation: action.operation,
                appliedToReport: false,
            };
        }

        const visuals = await activePage.getVisuals();
        if (!Array.isArray(visuals) || visuals.length === 0) {
            return {
                success: false,
                message: "No hay visuales disponibles en la página activa.",
                operation: action.operation,
                appliedToReport: false,
            };
        }

        const targetVisualName = String(action.targetVisualName || "").trim();
        if (!targetVisualName) {
            return {
                success: false,
                message: "No se recibió targetVisualName para UPDATE. Especifica el visual objetivo.",
                operation: action.operation,
                appliedToReport: false,
            };
        }

        const targetVisual = await resolveVisualByTechnicalName(activePage, targetVisualName);
        if (!targetVisual) {
            return {
                success: false,
                message: `No se encontró el visual objetivo "${targetVisualName}" en la página activa.`,
                operation: action.operation,
                appliedToReport: false,
            };
        }

        await applyThemeIfRequested(report, action);

        let mutationApplied = false;
        if (action.layout && typeof activePage.moveVisual === "function") {
            const currentLayout = targetVisual.layout || {};
            const nextX = action.layout.x ?? currentLayout.x ?? 0;
            const nextY = action.layout.y ?? currentLayout.y ?? 0;
            const nextWidth = action.layout.width ?? currentLayout.width ?? 600;
            const nextHeight = action.layout.height ?? currentLayout.height ?? 400;

            try {
                await activePage.moveVisual(targetVisual.name, nextX, nextY, nextWidth, nextHeight);
                mutationApplied = true;
            } catch (layoutError) {
                // PBI SDK errors — loguear detalle completo para diagnóstico
                try {
                    console.warn("⚠️ PBI SDK error:", JSON.stringify(layoutError, null, 2));
                } catch {
                    console.warn("⚠️ PBI SDK error (no-serializable):", (layoutError as any)?.message || (layoutError as any)?.detailedMessage || layoutError);
                }
            }
        }

        const resolvedVisualType =
            (String(targetVisual?.type || "").trim()) ||
            (String(action.visualType || "").trim()) ||
            "unknown";
        const roleSync = await syncUpdateDataRoles(targetVisual, resolvedVisualType, action);
        if (!roleSync.ok) {
            return {
                success: false,
                message: roleSync.message || "No se pudieron actualizar los dataRoles del visual.",
                operation: action.operation,
                appliedToReport: false,
            };
        }
        if ((action.dataRoles && Object.keys(action.dataRoles).length > 0) && roleSync.ok) {
            mutationApplied = true;
        }

        if (action.format && typeof targetVisual.setProperty === "function") {
            let appliedFormatCount = 0;
            let capabilities: any = null;
            if (typeof targetVisual.getCapabilities === "function") {
                try {
                    capabilities = await targetVisual.getCapabilities();
                    console.log("Capabilities del visual:", capabilities);
                } catch (capabilityError) {
                    console.warn("No se pudieron obtener capabilities del visual.", capabilityError);
                }
            }

            for (const [key, rawValue] of Object.entries(action.format)) {
                if (rawValue === null || rawValue === undefined) continue;
                const mappings = FORMAT_MAP[key];
                if (!Array.isArray(mappings) || mappings.length === 0) continue;

                let applied = false;
                for (const mapping of mappings) {
                    if (capabilities && !supportsFormatProperty(capabilities, mapping)) {
                        console.debug(
                            `Skipping ${key}: ${mapping.objectName}.${mapping.propertyName} no existe en capabilities.`,
                        );
                        continue;
                    }

                    try {
                        const finalValue = mapping.formatValue(rawValue);
                        const result = await targetVisual.setProperty(
                            {
                                objectName: mapping.objectName,
                                propertyName: mapping.propertyName,
                            },
                            { value: finalValue }
                        );
                        console.log(`Resultado de setProperty para ${key}:`, result);

                        if (setPropertyReturnedError(result)) {
                            console.warn(
                                `setProperty devolvió error para ${key}.`,
                                result,
                            );
                            continue;
                        }

                        console.log(`✅ Propiedad inyectada en SDK: ${mapping.objectName}.${mapping.propertyName} =`, finalValue);
                        applied = true;
                        appliedFormatCount += 1;
                        break;
                    } catch (formatError) {
                        console.warn(`No se pudo aplicar ${key}.`, formatError);
                    }
                }

                if (!applied) {
                    console.warn(`No se pudo aplicar ${key}; ningún mapeo fue aceptado por el visual.`);
                }
            }
            if (appliedFormatCount > 0) {
                mutationApplied = true;
            }
            if (Object.keys(action.format).length > 0 && appliedFormatCount === 0) {
                return {
                    success: false,
                    message: "No se pudo aplicar ningún cambio de formato al visual objetivo.",
                    operation: action.operation,
                    appliedToReport: false,
                };
            }
        }

        if (!mutationApplied) {
            return {
                success: false,
                message: "No se detectaron cambios aplicables para UPDATE en el visual objetivo.",
                operation: action.operation,
                appliedToReport: false,
            };
        }

        return {
            success: true,
            message: "Visual actualizado correctamente.",
            operation: action.operation,
            appliedToReport: false,
        };
    } catch (err: any) {
        return {
            success: false,
            message: `No se pudo actualizar el visual: ${err?.message || "Error desconocido"}`,
            operation: action.operation,
            appliedToReport: false,
        };
    }
}

async function handleCreateVisual(
    report: any,
    action: VisualAction
): Promise<ActionResult> {
    try {
        const activePage = await getEditableActivePage(report);

        if (!activePage) {
            return {
                success: false,
                message:
                    "No se pudo obtener una página editable del reporte. " +
                    "Verifica que el token de embed tenga acceso Edit y que el usuario/Service Principal tenga permisos de edición.",
                operation: action.operation,
                appliedToReport: false,
            };
        }

        const existingVisuals = await activePage.getVisuals().catch(() => []);
        const currentVisualsCount = Array.isArray(existingVisuals) ? existingVisuals.length : 0;
        const safeVisualType = action.visualType || "";
        const pbiVisualType = PBI_VISUAL_TYPE_MAP[safeVisualType] || safeVisualType;
        const layoutIntent = normalizeLayoutIntent(action.layout_intent);
        const intentLayout = layoutIntent ? computeIntentLayout(existingVisuals, layoutIntent) : null;

        // FASE 13v2: Smart Auto-Layout — posición dinámica basada en total de visuals.
        // El nuevo visual será el (currentVisualsCount)-ésimo (0-indexed).
        const totalAfterCreate = currentVisualsCount + 1;
        const gridPos = computeGridPosition(currentVisualsCount, totalAfterCreate);
        const layout = intentLayout
            ? intentLayout.layout
            : {
                x: gridPos.x,
                y: gridPos.y,
                width: gridPos.width,
                height: gridPos.height,
                displayState: {
                    mode: 0,
                },
            };

        // FASE 13v2: Redistribuir visuals EXISTENTES ANTES de crear el nuevo.
        // WHY: Si redistribuimos DESPUÉS de crear e inyectar datos, moveVisual
        // puede resetear data bindings no confirmados dejando el visual en blanco.
        // Redistribuir ANTES garantiza que el nuevo visual sea creado en su
        // posición final y nunca sea movido después de inyectar datos.
        try {
            if (intentLayout) {
                await redistributeExistingVisualsByLayout(activePage, intentLayout.relayouts);
            } else {
                await redistributeExistingVisuals(activePage, existingVisuals, totalAfterCreate);
            }
            // Pausa para que los eventos de re-render de moveVisual se estabilicen
            if (currentVisualsCount > 0) {
                await new Promise((resolve) => setTimeout(resolve, 500));
            }
        } catch (redistributeError) {
            console.warn("⚠️ No se pudo redistribuir visuals antes de crear:", redistributeError);
        }

        if (intentLayout) {
            console.log(
                `📍 Creando "${pbiVisualType}" con layout_intent (${intentLayout.debug}): x=${layout.x}, y=${layout.y}, w=${layout.width}, h=${layout.height}`
            );
        } else {
            console.log(
                `📍 Creando "${pbiVisualType}" en posición grid[${currentVisualsCount}/${totalAfterCreate}]: x=${gridPos.x}, y=${gridPos.y}, w=${gridPos.width}, h=${gridPos.height}`
            );
        }

        const createResponse = await activePage.createVisual(pbiVisualType, layout);
        const createdVisual = createResponse?.visual;

        if (!createdVisual) {
            return {
                success: false,
                message: "No se pudo obtener la instancia del visual recién creado.",
                operation: action.operation,
                appliedToReport: false,
            };
        }

        await new Promise((resolve) => setTimeout(resolve, 1000));

        const visuals = await activePage.getVisuals();
        const targetVisual = visuals.find((v: any) => v.name === createdVisual.name) || createdVisual;

        if (typeof targetVisual.addDataField !== "function") {
            return {
                success: false,
                message:
                    "No se pudo hidratar el visual para authoring (addDataField no disponible). " +
                    "Verifica que el SDK de autoría esté cargado antes de ejecutar CREATE.",
                operation: action.operation,
                appliedToReport: false,
            };
        }

        await applyThemeIfRequested(report, action);
        await applyCreateTitleIfRequested(targetVisual, action);

        // FASE 12: Garantizar descubrimiento de tablas ANTES de inyectar campos
        // Resuelve el race condition donde el usuario envía un chat antes de que
        // onLoaded haya terminado de ejecutar discoverModelTables.
        const discoveredTables = getDiscoveredTables();
        if (discoveredTables.length === 0) {
            console.log("🔍 Tablas no descubiertas aún, ejecutando descubrimiento forzado...");
            await discoverModelTables(report);
        }

        // DIAGNÓSTICO: Log de capacidades del visual para ver roles REALES
        if (process.env.NODE_ENV !== "production" && typeof targetVisual.getCapabilities === "function") {
            try {
                const caps = await targetVisual.getCapabilities();
                const roleNames = (caps?.dataRoles || []).map((r: any) => `${r.name}(${r.kind})`);
                console.log(`🎯 Roles válidos para "${pbiVisualType}":`, roleNames.join(", "));
            } catch {
                console.warn("No se pudieron obtener capabilities del visual.");
            }
        }

        for (const [roleName, roleValue] of Object.entries(action.dataRoles || {})) {
            const injected = await addFieldWithRoleFallback(targetVisual, pbiVisualType, roleName, roleValue, action);
            if (!injected.ok) {
                return {
                    success: false,
                    message: injected.message || `No se pudo inyectar el rol requerido "${roleName}".`,
                    operation: action.operation,
                    appliedToReport: false,
                };
            }
        }

        await applyCardDisplayUnitsIfNeeded(targetVisual, pbiVisualType);

        // FASE 14-FIX: Esperar a que PBI confirme los data bindings ANTES
        // de aplicar filtros o TopN. Sin esta pausa, el re-render que dispara
        // el filtro puede limpiar bindings no confirmados → visual en blanco.
        if ((action.filters && action.filters.length > 0) || action.top_n) {
            console.log("⏳ Esperando confirmación de data bindings antes de aplicar filtros...");
            await new Promise((resolve) => setTimeout(resolve, 1500));
        }

        // FASE 14-GUARD: Cuando TopN está presente, el LLM a veces genera
        // un filtro básico espurio con el NÚMERO del TopN como valor de texto
        // (ej: "In" values:["5"] sobre la columna de categoría).
        // Ese filtro colisiona con el TopN y blanquea el gráfico porque PBI
        // filtra "materiales cuyo nombre = '5'" → 0 filas.
        // Solución: eliminar filtros básicos que apunten a la misma columna
        // de categoría del TopN ANTES de inyectarlos.
        if (action.top_n && action.filters && action.filters.length > 0) {
            const topNCatCol = (action.top_n.category_column || "").toLowerCase().trim();
            const topNCatTable = (action.top_n.category_table || "").toLowerCase().trim();
            const before = action.filters.length;
            action.filters = action.filters.filter((f: any) => {
                const fCol = String(f?.column || f?.target?.column || "").toLowerCase().trim();
                const fTable = String(f?.table || f?.target?.table || "").toLowerCase().trim();
                // Eliminar si apunta a la misma columna de categoría del TopN
                if (fCol === topNCatCol && (fTable === topNCatTable || !fTable)) {
                    console.log(`🛡️ Filtro espurio eliminado: apuntaba a "${f?.column || f?.target?.column}" con valores ${JSON.stringify(f?.values)} — colisiona con TopN.`);
                    return false;
                }
                return true;
            });
            if (action.filters.length < before) {
                console.log(`🛡️ ${before - action.filters.length} filtro(s) espurio(s) eliminados por colisión con TopN.`);
            }
        }

        if (action.filters && action.filters.length > 0) {
            const filtersApplied = await applyFiltersWithVariants(targetVisual, action);
            if (!filtersApplied) {
                return {
                    success: false,
                    message: "No se pudieron aplicar filtros válidos al visual creado.",
                    operation: action.operation,
                    appliedToReport: false,
                };
            }
        }

        // FASE 14: Aplicar filtro TopN nativo si el backend lo detectó
        if (action.top_n) {
            await applyTopNFilter(targetVisual, action);
        }


        // (La redistribución ya fue hecha ANTES de crear el visual)

        return {
            success: true,
            message: `✅ Visual "${action.title || pbiVisualType}" created on the report.`,
            operation: action.operation,
            appliedToReport: false,
        };
    } catch (createErr: any) {
        const isPermissionError =
            createErr?.message?.includes("permission") ||
            createErr?.message?.includes("authorized") ||
            createErr?.message?.includes("edit") ||
            createErr?.detailedMessage?.includes("edit");

        if (isPermissionError) {
            return {
                success: false,
                message:
                    "⚠️ The embed token has View-only access. " +
                    "Visual creation requires Edit permissions. " +
                    "The visual configuration was generated correctly — " +
                    "it's displayed in the action card below.",
                operation: action.operation,
                appliedToReport: false,
            };
        }

        return {
            success: false,
            message: `Could not create visual: ${createErr.message}`,
            operation: action.operation,
            appliedToReport: false,
        };
    }
}

async function handleDeleteVisual(
    report: any,
    action: VisualAction
): Promise<ActionResult> {
    try {
        const activePage = await getActivePage(report);
        if (!activePage || typeof activePage.getVisuals !== "function") {
            return {
                success: false,
                message: "No se encontró una página activa para eliminar el visual.",
                operation: action.operation,
                appliedToReport: false,
            };
        }

        const targetVisualName = String(action.targetVisualName || "").trim();
        if (!targetVisualName) {
            return {
                success: false,
                message: "No se recibió targetVisualName para DELETE.",
                operation: action.operation,
                appliedToReport: false,
            };
        }

        const targetVisual = await resolveVisualByTechnicalNameOrTitle(activePage, targetVisualName);

        if (!targetVisual) {
            return {
                success: false,
                message: `No se encontró el visual "${targetVisualName}" para eliminar.`,
                operation: action.operation,
                appliedToReport: false,
            };
        }

        if (typeof activePage.deleteVisual !== "function") {
            return {
                success: false,
                message: "La versión del SDK no soporta deleteVisual en esta sesión.",
                operation: action.operation,
                appliedToReport: false,
            };
        }

        await activePage.deleteVisual(targetVisual.name);
        return {
            success: true,
            message: `✅ Visual "${targetVisual.name}" eliminado correctamente.`,
            operation: action.operation,
            appliedToReport: false,
        };
    } catch (err: any) {
        return {
            success: false,
            message: `No se pudo eliminar el visual: ${err?.message || "Error desconocido"}`,
            operation: action.operation,
            appliedToReport: false,
        };
    }
}

async function handleFilter(
    report: any,
    action: VisualAction
): Promise<ActionResult> {
    if (!action.filters || action.filters.length === 0) {
        return {
            success: false,
            message: "No filters specified in the action.",
            operation: action.operation,
            appliedToReport: false,
        };
    }

    try {
        const activePage = await getEditableActivePage(report);
        if (!activePage || typeof activePage.getVisuals !== "function") {
            return {
                success: false,
                message: "No se encontró una página activa editable para aplicar filtros.",
                operation: action.operation,
                appliedToReport: false,
            };
        }

        const visuals = await activePage.getVisuals();
        const selectedVisual = Array.isArray(visuals)
            ? visuals.find((v: any) => Boolean(v?.isSelected))
            : null;
        const targetVisual = selectedVisual || (Array.isArray(visuals) ? visuals[0] : null);

        const filtersApplied = await applyFiltersWithVariants(targetVisual, action);
        if (!filtersApplied) {
            return {
                success: false,
                message: "No se pudieron aplicar filtros válidos al visual objetivo.",
                operation: action.operation,
                appliedToReport: false,
            };
        }

        const filterSummary = action.filters
            .map((f) => `${f.table}[${f.column}] ${f.operator} ${f.values.join(", ")}`)
            .join("; ");

        return {
            success: true,
            message: `✅ Filter applied: ${filterSummary}`,
            operation: action.operation,
            appliedToReport: false,
        };
    } catch (err: any) {
        return {
            success: false,
            message: `Could not apply filter: ${err.message}`,
            operation: action.operation,
            appliedToReport: false,
        };
    }
}

async function handleNavigate(
    report: any,
    action: VisualAction
): Promise<ActionResult> {
    if (!action.target_page) {
        return {
            success: false,
            message: "No target page specified.",
            operation: action.operation,
            appliedToReport: false,
        };
    }

    try {
        const pages = await report.getPages();

        const safeTargetPage = action.target_page || "";
        const targetPage = pages.find(
            (p: any) =>
                p.displayName?.toLowerCase() === safeTargetPage.toLowerCase() ||
                p.name?.toLowerCase() === safeTargetPage.toLowerCase()
        );

        if (!targetPage) {
            const availablePages = pages.map((p: any) => p.displayName || p.name).join(", ");
            return {
                success: false,
                message: `Page "${safeTargetPage}" not found. Available: ${availablePages}`,
                operation: action.operation,
                appliedToReport: false,
            };
        }

        await report.setPage(targetPage.name);

        return {
            success: true,
            message: `✅ Navigated to page "${targetPage.displayName || targetPage.name}".`,
            operation: action.operation,
            appliedToReport: true,
        };
    } catch (err: any) {
        return {
            success: false,
            message: `Could not navigate: ${err.message}`,
            operation: action.operation,
            appliedToReport: false,
        };
    }
}

async function handleExplainVisual(
    report: any,
    action: VisualAction
): Promise<ActionResult> {
    try {
        const activePage = await getActivePage(report);
        if (!activePage || typeof activePage.getVisuals !== "function") {
            return {
                success: false,
                message: "No se encontró una página activa para analizar el visual.",
                operation: action.operation,
                appliedToReport: false,
            };
        }

        const visuals = await activePage.getVisuals();
        if (!Array.isArray(visuals) || visuals.length === 0) {
            return {
                success: false,
                message: "No hay visuales disponibles en la página activa.",
                operation: action.operation,
                appliedToReport: false,
            };
        }

        const targetVisualRef = String(action.targetVisualName || action.title || "").trim();
        if (!targetVisualRef) {
            return {
                success: false,
                message: "EXPLAIN requiere targetVisualName o title para seleccionar el visual objetivo.",
                operation: action.operation,
                appliedToReport: false,
            };
        }
        const targetVisual = await resolveVisualByTechnicalNameOrTitle(activePage, targetVisualRef);

        if (!targetVisual || typeof targetVisual.exportData !== "function") {
            return {
                success: false,
                message: `No se encontró el visual "${targetVisualRef}" o no soporta exportación de datos.`,
                operation: action.operation,
                appliedToReport: false,
            };
        }

        const pbiClient = await import("powerbi-client");
        const exportDataResult = await targetVisual.exportData(
            pbiClient.models.ExportDataType.Summarized,
        );
        const csvData = String(exportDataResult?.data || "");
        const parsedData = parsePowerBiCsvToJson(csvData);

        if (!parsedData.length) {
            return {
                success: false,
                message: "No se pudieron extraer filas para generar la explicación.",
                operation: action.operation,
                appliedToReport: false,
            };
        }

        const headers: Record<string, string> = {
            "Content-Type": "application/json",
        };
        const apiKey = process.env.NEXT_PUBLIC_API_KEY;
        if (apiKey) headers["X-API-Key"] = apiKey;

        const explainResponse = await fetch("/api/v1/explain", {
            method: "POST",
            headers,
            body: JSON.stringify({
                visual_title: String(action.title || targetVisual.title || targetVisual.name || targetVisualRef),
                raw_data: parsedData,
            }),
        });

        if (!explainResponse.ok) {
            const body = await explainResponse.json().catch(() => ({}));
            return {
                success: false,
                message: body?.detail || `No se pudo generar la explicación (${explainResponse.status}).`,
                operation: action.operation,
                appliedToReport: false,
            };
        }

        const explainBody = await explainResponse.json();
        const explanationText = String(explainBody?.explanation || "").trim();

        if (explanationText) {
            action.explanation = explanationText;
        }

        return {
            success: true,
            message: explanationText || "Explicación generada correctamente.",
            operation: action.operation,
            appliedToReport: false,
        };
    } catch (err: any) {
        return {
            success: false,
            message: `No se pudo ejecutar EXPLAIN: ${err?.message || "Error desconocido"}`,
            operation: action.operation,
            appliedToReport: false,
        };
    }
}
