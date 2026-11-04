DO $$ BEGIN IF EXISTS(SELECT 1 FROM dingtalk_document_publications) OR EXISTS(SELECT 1 FROM dingtalk_work_intents) THEN RAISE EXCEPTION 'DingTalk publication state cannot be discarded'; END IF; END $$;
DROP TABLE dingtalk_work_intents; DROP TABLE dingtalk_document_publications;
