-- Producer receipts and monotonic message versions cannot safely be dropped.
SELECT * FROM knowledge_input_provenance_rollback_is_prohibited;
