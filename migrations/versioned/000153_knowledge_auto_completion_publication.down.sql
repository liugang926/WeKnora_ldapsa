DROP TRIGGER IF EXISTS knowledge_auto_publication_version ON knowledges;
DROP FUNCTION IF EXISTS knowledge_auto_publication_version_bump();
DROP TABLE IF EXISTS knowledge_auto_publication_transitions;
DROP FUNCTION IF EXISTS knowledge_auto_publication_transition_immutable();
DROP TABLE IF EXISTS knowledge_auto_publication_versions;
