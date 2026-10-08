DROP TRIGGER IF EXISTS knowledge_span_version_change ON knowledge_processing_spans;
DROP FUNCTION IF EXISTS knowledge_span_version_change();
DROP TRIGGER IF EXISTS knowledge_span_manifest_immutable ON knowledge_span_start_manifests;
DROP FUNCTION IF EXISTS knowledge_span_manifest_immutable();
DROP TABLE IF EXISTS knowledge_span_body_receipts;
DROP TABLE IF EXISTS knowledge_span_row_versions;
DROP TABLE IF EXISTS knowledge_span_start_manifests;
DROP TRIGGER IF EXISTS knowledge_span_knowledge_delete ON knowledges;
DROP FUNCTION IF EXISTS knowledge_span_knowledge_delete();
