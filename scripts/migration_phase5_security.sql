-- ============================================================
-- Phase 5: Security Tables
-- Run this SQL in Supabase SQL Editor
-- ============================================================

-- 1. API Keys table — stores hashed keys per tenant
CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID REFERENCES tenants(id) ON DELETE CASCADE NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    name TEXT DEFAULT 'default',
    is_active BOOLEAN DEFAULT true,
    last_used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys(tenant_id);

-- 2. Audit Log table — records every API call
CREATE TABLE IF NOT EXISTS audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    api_key_id UUID,
    endpoint TEXT NOT NULL,
    method TEXT NOT NULL,
    status_code INT,
    request_summary JSONB DEFAULT '{}',
    ip_address TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_tenant ON audit_log(tenant_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_created ON audit_log(created_at DESC);

-- 3. Insert a dev API key for testing
-- Key: "dev-api-key-2024-orchestrator" → SHA256 hash
-- NOTE: Replace tenant_id with your actual tenant UUID
INSERT INTO api_keys (tenant_id, key_hash, name) VALUES (
    '9d36ff08-691e-4f7d-b1bf-049abf374860', -- Demo tenant
    '60faf12a39dd22932fcdeee0468edf8c5890c705938092d8661f449df286e074', -- SHA256 of 'dev-api-key-2024-orchestrator'
    'dev-key'
) ON CONFLICT (key_hash) DO NOTHING;
