BEGIN;

DROP TABLE library_config;
DROP TABLE subscriptions;
DROP TABLE dataframe;

DROP TYPE datatype CASCADE;
DROP TYPE assettype CASCADE;

DROP FUNCTION adj_close_default();

COMMIT;