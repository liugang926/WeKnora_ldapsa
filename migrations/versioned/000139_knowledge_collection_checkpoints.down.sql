DROP TRIGGER IF EXISTS knowledge_collection_knowledges ON knowledges;
DROP FUNCTION IF EXISTS bump_knowledge_collection_knowledges();
DROP TRIGGER IF EXISTS knowledge_collection_knowledge_bases ON knowledge_bases;
DROP FUNCTION IF EXISTS bump_knowledge_collection_knowledge_bases();
DROP TABLE IF EXISTS knowledge_collection_scope_versions;
