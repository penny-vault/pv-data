BEGIN;

-- Enum Types

CREATE TYPE assettype AS ENUM (
    'Common Stock',
    'Preferred Stock',
    'ETF',  -- Exchange Traded Fund
    'ETN',  -- Exchange Traded Note
    'MF',   -- Mutual Fund
    'CEF',  -- Closed End Fund
    'ADRC', -- American Depository Receipt Common
    'FRED',
    'Synthetic History'
);
CREATE TYPE datatype AS ENUM ('asset-description', 'analyst-rating', 'eod', 'fundamental', 'earnings-estimate', 'custom');

-- Tables

CREATE TABLE library (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name text,
    owner text
);

CREATE TABLE subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,

    provider TEXT NOT NULL,
    dataset TEXT NOT NULL,
    config JSONB NOT NULL,

    data_tables TEXT[] NOT NULL,
    data_types datatype[] NOT NULL,

    total_records INTEGER DEFAULT 0,
    num_records_last_import INTEGER DEFAULT 0,

    total_securities INTEGER DEFAULT 0,
    num_securities_last_import INTEGER DEFAULT 0,

    first_obs_date TIMESTAMP, -- First observation date
    last_obs_date TIMESTAMP,  -- Last observation date

    schedule TEXT NOT NULL,
    health_check_id TEXT,
    last_run TIMESTAMP,
    active BOOLEAN DEFAULT true,
    schema_version INTEGER DEFAULT 0,

    created_on TIMESTAMP DEFAULT now(),
    created_by TEXT NOT NULL
);

CREATE TABLE dataframe (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    data_type datatype NOT NULL,
    partitioned BOOLEAN DEFAULT false,
    subscriptions TEXT[],

    UNIQUE(name)
);

CREATE OR REPLACE FUNCTION adj_close_default()
  RETURNS trigger
  LANGUAGE plpgsql AS
$func$
BEGIN
   NEW.adj_close := NEW.close;
   RETURN NEW;
END
$func$;

COMMIT;