-- 20260329_runtime_state.sql
-- Persistencia ligera por reporte/tenant para capabilities y onboarding.

CREATE TABLE IF NOT EXISTS report_runtime_state (
  tenant_id UUID NOT NULL,
  report_id UUID NOT NULL,
  blocked_capabilities JSONB NOT NULL DEFAULT '{}'::jsonb,
  suggested_measures_shown JSONB NOT NULL DEFAULT '[]'::jsonb,
  user_acknowledged JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, report_id)
);

CREATE INDEX IF NOT EXISTS idx_report_runtime_state_report
  ON report_runtime_state (report_id);

-- (Opcional) Si usas RLS en tu proyecto Supabase, habilítalo y crea políticas.
-- En PromtBI el backend suele operar con Service Role; si tu DB está protegida,
-- puedes activar RLS y permitir solo al service role.

