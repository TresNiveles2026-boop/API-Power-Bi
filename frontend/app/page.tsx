"use client";

/**
 * Main Page — Orquesta Header + ReportArea + ChatSidebar + Toast.
 *
 * WHY: Esta es la página raíz de la aplicación. Combina los cuatro
 * componentes principales en un layout split (70%/30%) y maneja
 * el estado compartido (acciones generadas, resultados de ejecución)
 * entre ellos.
 *
 * Phase 4: Added Toast notification for SDK action feedback.
 */

import { useState, useCallback } from "react";
import dynamic from "next/dynamic";
import Header from "@/components/Header";
import ChatSidebar from "@/components/ChatSidebar";
import Toast from "@/components/Toast";
import type { ChatResponse, VisualAction } from "@/lib/types";
import { executeAction, type ActionResult } from "@/lib/actionHandler";
import "@/lib/intercept-console-error";
import { UiProvider } from "@/lib/uiContext";

const ReportArea = dynamic(() => import("@/components/ReportArea"), { ssr: false });

// IDs del reporte demo (de Supabase, sincronizados en Fase 1)
const DEMO_REPORT_ID = "94e97143-fcba-4d04-b871-9e4e3b0c65ed";
const DEMO_TENANT_ID = "9d36ff08-691e-4f7d-b1bf-049abf374860";

export default function Home() {
  const [actions, setActions] = useState<VisualAction[]>([]);
  const [lastAction, setLastAction] = useState<VisualAction | null>(null);
  const [lastResult, setLastResult] = useState<ActionResult | null>(null);
  const [toastResult, setToastResult] = useState<ActionResult | null>(null);

  const handleActionGenerated = useCallback(async (response: ChatResponse): Promise<ActionResult> => {
    const actionList =
      Array.isArray(response.actions) && response.actions.length > 0
        ? response.actions
        : response.action
          ? [response.action]
          : [];
    const firstAction = actionList[0] || null;

    setLastAction(firstAction);
    setLastResult(null);

    // Add to action list for display
    if (actionList.length > 0) {
      setActions((prev) => [
        ...prev,
        ...actionList.filter(
          (action) =>
            action.operation === "CREATE" ||
            action.operation === "CREATE_VISUAL" ||
            action.operation === "FILTER"
        ),
      ]);
    }

    // Execute actions on the live Power BI report
    try {
      const result = await executeAction(response);
      setLastResult(result);
      setToastResult(result); // Show toast notification
      console.log("Action result:", result);
      return result;
    } catch (err) {
      const errorResult: ActionResult = {
        success: false,
        message: `Error: ${err instanceof Error ? err.message : "Unknown"}`,
        operation: firstAction?.operation || "UNKNOWN",
        appliedToReport: false,
      };
      setLastResult(errorResult);
      setToastResult(errorResult);
      return errorResult;
    }
  }, []);

  const handleDismissToast = useCallback(() => {
    setToastResult(null);
  }, []);

  return (
    <UiProvider>
      <div className="h-screen flex flex-col overflow-hidden">
        <Header />

      <main className="flex-1 flex overflow-hidden">
        {/* Report Area (Left — 70%) */}
        <div className="flex-[7] border-r border-[var(--color-border)] overflow-hidden">
          <ReportArea
            lastAction={lastAction}
            actions={actions}
            lastResult={lastResult}
          />
        </div>

        {/* Chat Sidebar (Right — 30%) */}
        <div className="flex-[3] min-w-[340px] max-w-[480px] overflow-hidden">
          <ChatSidebar
            reportId={DEMO_REPORT_ID}
            tenantId={DEMO_TENANT_ID}
            onActionGenerated={handleActionGenerated}
          />
        </div>
      </main>

      {/* Toast Notification (bottom-left) */}
        <Toast result={toastResult} onDismiss={handleDismissToast} />
      </div>
    </UiProvider>
  );
}
