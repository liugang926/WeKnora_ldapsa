DROP TRIGGER IF EXISTS protect_nextcloud_onboarding_template_on_delete
ON knowledge_bases;

DROP TRIGGER IF EXISTS protect_nextcloud_onboarding_template_on_update
ON knowledge_bases;

DROP FUNCTION IF EXISTS protect_nextcloud_onboarding_template();
