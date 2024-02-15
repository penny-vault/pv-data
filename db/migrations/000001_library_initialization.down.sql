BEGIN;

DROP TABLE library_config;
DROP TABLE subscriptions;
DROP TABLE dataframe;

DROP TYPE datatype;
DROP TYPE assettype;

DROP FUNCTION adj_close_default();

COMMIT;