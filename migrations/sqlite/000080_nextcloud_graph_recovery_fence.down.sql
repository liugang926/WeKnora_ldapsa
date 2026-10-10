SELECT 'Retain recovery graph fences; audited export is required before rollback';
SELECT RAISE(ABORT,'recovery graph fence rollback blocked');
