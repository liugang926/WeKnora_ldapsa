DO $nextcloud_abort_down$
BEGIN
    IF EXISTS (SELECT 1 FROM nextcloud_source_pairing_aborts) THEN
        RAISE EXCEPTION 'cannot drop source pairing abort tombstones';
    END IF;
END
$nextcloud_abort_down$;
DROP TABLE nextcloud_source_pairing_aborts;
