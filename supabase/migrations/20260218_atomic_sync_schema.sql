-- 20260218_atomic_sync_schema.sql
-- Function to sync semantic dictionary and update report version atomically.
-- Complies with Phase 5 "Atomicity" rule.

CREATE OR REPLACE FUNCTION sync_schema_atomic(
    p_report_id UUID,
    p_tenant_id UUID,
    p_columns JSONB
)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_count INTEGER;
    v_current_version INTEGER;
BEGIN
    -- 1. Upsert columns into semantic_dictionaries
    -- We use a temporary table or direct insert with json_to_recordset if possible,
    -- but for simplicity/compatibility we loop or use jsonb_populate_recordset.
    
    WITH inserted AS (
        INSERT INTO semantic_dictionaries (
            report_id,
            tenant_id,
            table_name,
            column_name,
            data_type,
            description,
            is_measure,
            dax_expression,
            sample_values,
            metadata
        )
        SELECT 
            p_report_id,
            p_tenant_id,
            x.table_name,
            x.column_name,
            x.data_type,
            COALESCE(x.description, ''),
            COALESCE(x.is_measure, false),
            COALESCE(x.dax_expression, ''),
            COALESCE(x.sample_values, '[]'::jsonb),
            COALESCE(x.metadata, '{}'::jsonb)
        FROM jsonb_to_recordset(p_columns) AS x(
            table_name TEXT,
            column_name TEXT,
            data_type TEXT,
            description TEXT,
            is_measure BOOLEAN,
            dax_expression TEXT,
            sample_values JSONB,
            metadata JSONB
        )
        ON CONFLICT (report_id, table_name, column_name)
        DO UPDATE SET
            data_type = EXCLUDED.data_type,
            description = EXCLUDED.description,
            is_measure = EXCLUDED.is_measure,
            dax_expression = EXCLUDED.dax_expression,
            sample_values = EXCLUDED.sample_values,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_count FROM inserted;

    -- 2. Increment schema version in reports table
    UPDATE reports
    SET schema_version = COALESCE(schema_version, 0) + 1,
        updated_at = NOW()
    WHERE id = p_report_id AND tenant_id = p_tenant_id;

    RETURN v_count;
END;
$$;
