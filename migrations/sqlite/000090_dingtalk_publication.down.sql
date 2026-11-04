CREATE TEMP TABLE dingtalk_down_guard(n INTEGER CHECK(n=0)); INSERT INTO dingtalk_down_guard SELECT (SELECT count(*) FROM dingtalk_document_publications)+(SELECT count(*) FROM dingtalk_work_intents); DROP TABLE dingtalk_down_guard;
DROP TABLE dingtalk_work_intents; DROP TABLE dingtalk_document_publications;
