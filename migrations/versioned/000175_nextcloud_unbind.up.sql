-- 2026-11-09 NextCloud decoupling: retire the provenance-immutability guard
-- so the sanctioned nextcloud-unbind command (cmd/nextcloud-unbind) can clear
-- ever_had_nextcloud_source for knowledge bases whose NextCloud binding is
-- being revoked. The migration only disarms the SQL guard; data changes are
-- made by the operator command in a transaction, after this migration ran.

-- Drop the provenance trigger by its migration-111 name, and defensively any
-- trigger on knowledge_bases whose function body raises the provenance error
-- (later migrations replaced bodies under generated names).
DROP TRIGGER IF EXISTS trg_preserve_kb_nextcloud_provenance ON knowledge_bases;

DO $$
DECLARE
    t RECORD;
BEGIN
    FOR t IN
        SELECT tg.tgname AS trigger_name,
               n.nspname AS schema_name,
               c.relname AS table_name,
               p.proname AS func_name
        FROM pg_trigger tg
        JOIN pg_class c ON c.oid = tg.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_proc p ON p.oid = tg.tgfoid
        WHERE NOT tg.tgisinternal
          AND c.relname = 'knowledge_bases'
          AND n.nspname = current_schema()
          AND p.prokind = 'f'
          AND p.prorettype = 'trigger'::regtype
          AND p.pronargs = 0
          AND p.prosrc ILIKE '%provenance cannot be cleared%'
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I.%I', t.trigger_name, t.schema_name, t.table_name);
    END LOOP;
END $$;

DROP FUNCTION IF EXISTS preserve_kb_nextcloud_provenance();

-- Read stored source directly: pg_get_functiondef rejects aggregates and window
-- entries, and WHERE predicate order cannot safely guard its evaluation. Only
-- zero-argument trigger functions in the migration schema are provenance guards.
DO $$
DECLARE
    f RECORD;
BEGIN
    FOR f IN
        SELECT n.nspname AS schema_name, p.proname AS func_name
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = current_schema()
          AND p.prokind = 'f'
          AND p.prorettype = 'trigger'::regtype
          AND p.pronargs = 0
          AND p.prosrc ILIKE '%provenance cannot be cleared%'
    LOOP
        EXECUTE format('DROP FUNCTION IF EXISTS %I.%I()', f.schema_name, f.func_name);
    END LOOP;
END $$;
