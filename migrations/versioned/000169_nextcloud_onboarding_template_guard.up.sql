CREATE OR REPLACE FUNCTION protect_nextcloud_onboarding_template()
RETURNS trigger AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM nextcloud_onboarding_credentials
        WHERE template_kb_id = OLD.id
          AND enabled = TRUE
    ) THEN
        RAISE EXCEPTION
            'knowledge base is an active Nextcloud onboarding template'
            USING ERRCODE = '23503';
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS protect_nextcloud_onboarding_template_on_update
ON knowledge_bases;

CREATE TRIGGER protect_nextcloud_onboarding_template_on_update
BEFORE UPDATE OF deleted_at ON knowledge_bases
FOR EACH ROW
WHEN (OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL)
EXECUTE FUNCTION protect_nextcloud_onboarding_template();

DROP TRIGGER IF EXISTS protect_nextcloud_onboarding_template_on_delete
ON knowledge_bases;

CREATE TRIGGER protect_nextcloud_onboarding_template_on_delete
BEFORE DELETE ON knowledge_bases
FOR EACH ROW
EXECUTE FUNCTION protect_nextcloud_onboarding_template();
