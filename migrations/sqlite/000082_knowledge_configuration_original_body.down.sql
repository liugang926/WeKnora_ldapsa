-- Immutable storage ancestry cannot be removed by downgrade.
CREATE TEMP TABLE config_body_downgrade_denied(v INTEGER CHECK(v=1)); INSERT INTO config_body_downgrade_denied VALUES(0);
