"use client";

import React, { useEffect, useRef } from "react";
import type { models } from "powerbi-client";
import { embedPowerBiReport, resetPowerBiContainer, discoverModelTables } from "@/lib/pbiRuntime";

interface EmbedComponentProps {
  embedConfig: models.IReportEmbedConfiguration;
  cssClassName?: string;
}

export default function PowerBIEmbedComponent({
  embedConfig,
  cssClassName = "h-full w-full",
}: EmbedComponentProps) {
  const reportRef = useRef<any>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);

  const ensureEditMode = async () => {
    const report = reportRef.current;
    if (!report) return;

    try {
      await report.switchMode("edit");
      console.log("✅ Power BI switched to Edit mode");
    } catch (err: any) {
      console.warn(
        "⚠️ Could not switch report to Edit mode. Check workspace/report permissions.",
        err?.message || err
      );
    }
  };

  useEffect(() => {
    let cancelled = false;
    const container = containerRef.current;
    if (!container) return;

    const onLoaded = async () => {
      console.log("Report loaded");
      await ensureEditMode();
      // Descubrir los nombres reales de tablas del modelo PBI
      if (reportRef.current) {
        await discoverModelTables(reportRef.current);
      }
    };

    const onRendered = async () => {
      console.log("Report rendered");
      await ensureEditMode();
    };

    const onError = (event: any) => {
      const detail = event?.detail || event;
      // Serializar detalle completo para diagnóstico
      try {
        console.warn("⚠️ PBI SDK error:", JSON.stringify(detail, null, 2));
      } catch {
        console.warn("⚠️ PBI SDK error:", detail?.message || detail?.detailedMessage || detail);
      }
    };

    const mount = async () => {
      try {
        const report = await embedPowerBiReport(container, embedConfig);
        if (cancelled) return;

        reportRef.current = report;
        report.on("loaded", onLoaded);
        report.on("rendered", onRendered);
        report.on("error", onError);
      } catch (err) {
        console.error("Failed to embed Power BI report", err);
      }
    };

    mount();

    return () => {
      cancelled = true;
      const report = reportRef.current;
      if (report) {
        report.off("loaded", onLoaded);
        report.off("rendered", onRendered);
        report.off("error", onError);
      }
      reportRef.current = null;
      void resetPowerBiContainer(container);
    };
  }, [embedConfig]);

  return (
    <div ref={containerRef} className={cssClassName} />
  );
}
