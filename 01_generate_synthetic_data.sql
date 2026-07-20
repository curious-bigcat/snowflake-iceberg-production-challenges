/*
================================================================================
  ICEBERG PRODUCTION CHALLENGES DEMO - FINANCIAL SERVICES DATA MODEL
================================================================================
  PURPOSE:  Generate a production-scale multi-table Financial Services data model
            that organically creates Iceberg production challenges and provides
            realistic data for all 13 challenge demonstrations.

  DOMAIN: Financial Services (Investment Banking / Wealth Management)
  
  TABLES CREATED (7 tables, ~10M+ total rows):
    1. transactions         - 5M payment/trade transactions (streaming ingest)
    2. accounts             - 500K customer accounts (reference data)
    3. risk_scores          - 2M risk assessments (frequently updated)
    4. compliance_events    - 1M audit/compliance log entries
    5. market_data          - 2M price ticks (high-frequency append)
    6. portfolios           - 200K portfolio holdings (DML-heavy)
    7. transactions_staging - 100K rows CDC staging (MERGE patterns)

  DATA RELATIONSHIPS:
    accounts.account_id -> transactions.account_id
    accounts.account_id -> risk_scores.account_id
    accounts.account_id -> portfolios.account_id
    accounts.account_id -> compliance_events.account_id
    market_data.symbol -> portfolios.symbol

  SCALE: ~10M rows total (production-like, 5-10 min generation time)

  PREREQUISITE: Run 00_setup_infrastructure.sql first
  ESTIMATED TIME: 5-10 minutes on MEDIUM warehouse
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE ICEBERG_CHALLENGES_DB;
USE SCHEMA DEMO;
USE WAREHOUSE ICEBERG_DEMO_WH;


-- =============================================================================
-- TABLE 1: ACCOUNTS (500K rows - Reference/Dimension Table)
-- =============================================================================
-- Customer accounts - the central entity in our financial services model.
-- Bulk loaded, infrequently updated. Contains PII for access control demos.

CREATE OR REPLACE ICEBERG TABLE accounts (
    account_id          STRING,
    customer_name       STRING,
    email               STRING,
    account_type        STRING,
    tier                STRING,
    region              STRING,
    country             STRING,
    account_status      STRING,
    credit_limit        NUMBER(15,2),
    ssn_encrypted       STRING,
    risk_rating         STRING,
    relationship_mgr    STRING,
    opened_date         DATE,
    last_activity_date  DATE
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_demo_vol'
BASE_LOCATION = 'finserv/accounts'
TARGET_FILE_SIZE = 'AUTO';

INSERT INTO accounts
SELECT
    'ACCT_' || LPAD(SEQ4() + 1, 8, '0'),
    'Customer_' || (SEQ4() + 1),
    'client_' || (SEQ4() + 1) || '@' ||
        CASE UNIFORM(1, 5, RANDOM())
            WHEN 1 THEN 'gmail.com' WHEN 2 THEN 'corporate.com'
            WHEN 3 THEN 'finance.org' WHEN 4 THEN 'wealth.co' WHEN 5 THEN 'invest.net'
        END,
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN 'CHECKING' WHEN 2 THEN 'SAVINGS' WHEN 3 THEN 'BROKERAGE'
        WHEN 4 THEN 'RETIREMENT' WHEN 5 THEN 'MARGIN'
    END,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'PLATINUM' WHEN 2 THEN 'GOLD' WHEN 3 THEN 'SILVER' WHEN 4 THEN 'STANDARD'
    END,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'AMERICAS' WHEN 2 THEN 'EMEA' WHEN 3 THEN 'APAC' WHEN 4 THEN 'LATAM'
    END,
    CASE UNIFORM(1, 6, RANDOM())
        WHEN 1 THEN 'USA' WHEN 2 THEN 'GBR' WHEN 3 THEN 'SGP'
        WHEN 4 THEN 'DEU' WHEN 5 THEN 'JPN' WHEN 6 THEN 'BRA'
    END,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'ACTIVE' WHEN 2 THEN 'ACTIVE' WHEN 3 THEN 'ACTIVE' WHEN 4 THEN 'DORMANT'
    END,
    ROUND(UNIFORM(1000, 10000000, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2),
    SHA2(UNIFORM(100000000, 999999999, RANDOM())::VARCHAR, 256),
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN 'AAA' WHEN 2 THEN 'AA' WHEN 3 THEN 'A'
        WHEN 4 THEN 'BBB' WHEN 5 THEN 'BB'
    END,
    'RM_' || LPAD(UNIFORM(1, 200, RANDOM())::VARCHAR, 3, '0'),
    DATEADD(day, -UNIFORM(30, 7300, RANDOM()), CURRENT_DATE()),
    DATEADD(day, -UNIFORM(0, 90, RANDOM()), CURRENT_DATE())
FROM TABLE(GENERATOR(ROWCOUNT => 500000));

SELECT account_type, tier, COUNT(*) AS cnt
FROM accounts GROUP BY 1, 2 ORDER BY 1, 2;


-- =============================================================================
-- TABLE 2: TRANSACTIONS (5M rows - High-Volume Streaming Ingest)
-- =============================================================================
-- Payment and trade transactions. Simulates streaming micro-batch ingestion
-- to create the small-file problem organically.

CREATE OR REPLACE ICEBERG TABLE transactions (
    txn_id              STRING,
    account_id          STRING,
    txn_type            STRING,
    txn_status          STRING,
    amount              NUMBER(15,2),
    currency            STRING,
    counterparty        STRING,
    channel             STRING,
    txn_timestamp       TIMESTAMP_NTZ,
    settlement_date     DATE,
    region              STRING,
    risk_flag           BOOLEAN DEFAULT FALSE,
    fraud_score         FLOAT
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_demo_vol'
BASE_LOCATION = 'finserv/transactions'
TARGET_FILE_SIZE = '16MB';  -- Intentionally small to show the problem

-- Stored procedure: Simulates high-frequency transaction streaming
CREATE OR REPLACE PROCEDURE simulate_transaction_stream(
    num_batches INT DEFAULT 100,
    rows_per_batch INT DEFAULT 50000
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    i INT DEFAULT 0;
    total_rows INT;
BEGIN
    total_rows := num_batches * rows_per_batch;

    WHILE (i < :num_batches) DO
        INSERT INTO transactions (txn_id, account_id, txn_type, txn_status, amount, currency,
                                  counterparty, channel, txn_timestamp, settlement_date, region, risk_flag, fraud_score)
        SELECT
            UUID_STRING(),
            'ACCT_' || LPAD(UNIFORM(1, 500000, RANDOM())::VARCHAR, 8, '0'),
            CASE UNIFORM(1, 6, RANDOM())
                WHEN 1 THEN 'PAYMENT' WHEN 2 THEN 'TRANSFER' WHEN 3 THEN 'TRADE_BUY'
                WHEN 4 THEN 'TRADE_SELL' WHEN 5 THEN 'WITHDRAWAL' WHEN 6 THEN 'DEPOSIT'
            END,
            CASE UNIFORM(1, 5, RANDOM())
                WHEN 1 THEN 'COMPLETED' WHEN 2 THEN 'COMPLETED' WHEN 3 THEN 'PENDING'
                WHEN 4 THEN 'SETTLED' WHEN 5 THEN 'FAILED'
            END,
            ROUND(UNIFORM(10, 999999, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2),
            CASE UNIFORM(1, 4, RANDOM())
                WHEN 1 THEN 'USD' WHEN 2 THEN 'EUR' WHEN 3 THEN 'GBP' WHEN 4 THEN 'JPY'
            END,
            'COUNTERPARTY_' || LPAD(UNIFORM(1, 10000, RANDOM())::VARCHAR, 5, '0'),
            CASE UNIFORM(1, 5, RANDOM())
                WHEN 1 THEN 'ONLINE' WHEN 2 THEN 'MOBILE' WHEN 3 THEN 'BRANCH'
                WHEN 4 THEN 'API' WHEN 5 THEN 'WIRE'
            END,
            DATEADD(second, UNIFORM(0, 15552000, RANDOM()), '2024-01-01'::TIMESTAMP_NTZ),
            DATEADD(day, UNIFORM(0, 3, RANDOM()),
                    DATEADD(second, UNIFORM(0, 15552000, RANDOM()), '2024-01-01'::TIMESTAMP_NTZ)::DATE),
            CASE UNIFORM(1, 4, RANDOM())
                WHEN 1 THEN 'AMERICAS' WHEN 2 THEN 'EMEA' WHEN 3 THEN 'APAC' WHEN 4 THEN 'LATAM'
            END,
            CASE WHEN UNIFORM(1, 100, RANDOM()) <= 5 THEN TRUE ELSE FALSE END,
            ROUND(UNIFORM(0, 100, RANDOM()) / 100.0, 4)
        FROM TABLE(GENERATOR(ROWCOUNT => :rows_per_batch));

        i := i + 1;
    END WHILE;

    RETURN 'SUCCESS: Inserted ' || :total_rows || ' transactions in ' || :num_batches || ' micro-batches';
END;
$$;

-- Generate 5M transactions in 100 micro-batches of 50K each
-- This creates realistic small-file fragmentation
CALL simulate_transaction_stream(100, 50000);

SELECT txn_type, txn_status, COUNT(*) AS cnt, SUM(amount) AS total_volume
FROM transactions GROUP BY 1, 2 ORDER BY 1, 2;


-- =============================================================================
-- TABLE 3: RISK_SCORES (2M rows - Frequently Updated)
-- =============================================================================
-- Risk assessments that are recalculated daily. Heavy UPDATE workload.

CREATE OR REPLACE ICEBERG TABLE risk_scores (
    assessment_id       NUMBER(38,0),
    account_id          STRING,
    score_date          DATE,
    credit_score        NUMBER(3,0),
    market_risk         FLOAT,
    liquidity_risk      FLOAT,
    operational_risk    FLOAT,
    composite_score     FLOAT,
    risk_category       STRING,
    model_version       STRING,
    last_updated        TIMESTAMP_NTZ
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_demo_vol'
BASE_LOCATION = 'finserv/risk_scores'
TARGET_FILE_SIZE = 'AUTO'
ICEBERG_VERSION = 3;

INSERT INTO risk_scores
SELECT
    SEQ4() + 1,
    'ACCT_' || LPAD(UNIFORM(1, 500000, RANDOM())::VARCHAR, 8, '0'),
    DATEADD(day, -UNIFORM(0, 90, RANDOM()), CURRENT_DATE()),
    UNIFORM(300, 850, RANDOM()),
    ROUND(UNIFORM(0, 100, RANDOM()) / 100.0, 4),
    ROUND(UNIFORM(0, 100, RANDOM()) / 100.0, 4),
    ROUND(UNIFORM(0, 100, RANDOM()) / 100.0, 4),
    ROUND((UNIFORM(0, 100, RANDOM()) + UNIFORM(0, 100, RANDOM()) + UNIFORM(0, 100, RANDOM())) / 300.0, 4),
    CASE
        WHEN UNIFORM(0, 100, RANDOM()) < 20 THEN 'LOW'
        WHEN UNIFORM(0, 100, RANDOM()) < 60 THEN 'MEDIUM'
        WHEN UNIFORM(0, 100, RANDOM()) < 85 THEN 'HIGH'
        ELSE 'CRITICAL'
    END,
    'v' || UNIFORM(1, 3, RANDOM()) || '.' || UNIFORM(0, 9, RANDOM()),
    DATEADD(second, -UNIFORM(0, 86400, RANDOM()), CURRENT_TIMESTAMP())
FROM TABLE(GENERATOR(ROWCOUNT => 2000000));

SELECT risk_category, COUNT(*) AS cnt, AVG(composite_score) AS avg_score
FROM risk_scores GROUP BY 1 ORDER BY 1;


-- =============================================================================
-- TABLE 4: COMPLIANCE_EVENTS (1M rows - Append-Heavy Audit Log)
-- =============================================================================
-- Regulatory compliance events - append-only audit trail.

CREATE OR REPLACE ICEBERG TABLE compliance_events (
    event_id            STRING,
    account_id          STRING,
    event_type          STRING,
    severity            STRING,
    regulation          STRING,
    description         STRING,
    detected_at         TIMESTAMP_NTZ,
    resolved_at         TIMESTAMP_NTZ,
    resolution_status   STRING,
    assigned_to         STRING,
    region              STRING
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_demo_vol'
BASE_LOCATION = 'finserv/compliance_events'
TARGET_FILE_SIZE = 'AUTO';

INSERT INTO compliance_events
SELECT
    UUID_STRING(),
    'ACCT_' || LPAD(UNIFORM(1, 500000, RANDOM())::VARCHAR, 8, '0'),
    CASE UNIFORM(1, 8, RANDOM())
        WHEN 1 THEN 'KYC_VERIFICATION' WHEN 2 THEN 'AML_ALERT'
        WHEN 3 THEN 'SANCTIONS_SCREENING' WHEN 4 THEN 'UNUSUAL_ACTIVITY'
        WHEN 5 THEN 'THRESHOLD_BREACH' WHEN 6 THEN 'PEP_MATCH'
        WHEN 7 THEN 'DOCUMENT_EXPIRY' WHEN 8 THEN 'REGULATORY_FILING'
    END,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'LOW' WHEN 2 THEN 'MEDIUM' WHEN 3 THEN 'HIGH' WHEN 4 THEN 'CRITICAL'
    END,
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN 'GDPR' WHEN 2 THEN 'SOX' WHEN 3 THEN 'BASEL_III'
        WHEN 4 THEN 'MiFID_II' WHEN 5 THEN 'DODD_FRANK'
    END,
    'Compliance event detected for account monitoring - auto-generated alert #' || UNIFORM(1000, 9999, RANDOM())::VARCHAR,
    DATEADD(second, -UNIFORM(0, 15552000, RANDOM()), CURRENT_TIMESTAMP()),
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 70
         THEN DATEADD(hour, UNIFORM(1, 720, RANDOM()), DATEADD(second, -UNIFORM(0, 15552000, RANDOM()), CURRENT_TIMESTAMP()))
         ELSE NULL
    END,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'OPEN' WHEN 2 THEN 'IN_REVIEW' WHEN 3 THEN 'RESOLVED' WHEN 4 THEN 'ESCALATED'
    END,
    'ANALYST_' || LPAD(UNIFORM(1, 50, RANDOM())::VARCHAR, 3, '0'),
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'AMERICAS' WHEN 2 THEN 'EMEA' WHEN 3 THEN 'APAC' WHEN 4 THEN 'LATAM'
    END
FROM TABLE(GENERATOR(ROWCOUNT => 1000000));

SELECT event_type, severity, COUNT(*) AS cnt
FROM compliance_events GROUP BY 1, 2 ORDER BY 1, 2;


-- =============================================================================
-- TABLE 5: MARKET_DATA (2M rows - High-Frequency Append)
-- =============================================================================
-- Price ticks for financial instruments. Extremely high-frequency append.

CREATE OR REPLACE ICEBERG TABLE market_data (
    tick_id             NUMBER(38,0),
    symbol              STRING,
    exchange            STRING,
    price               NUMBER(12,4),
    bid                 NUMBER(12,4),
    ask                 NUMBER(12,4),
    volume              NUMBER(15,0),
    tick_timestamp      TIMESTAMP_NTZ,
    market_session      STRING
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_demo_vol'
BASE_LOCATION = 'finserv/market_data'
TARGET_FILE_SIZE = '128MB';

INSERT INTO market_data
SELECT
    SEQ4() + 1,
    CASE UNIFORM(1, 20, RANDOM())
        WHEN 1 THEN 'AAPL' WHEN 2 THEN 'MSFT' WHEN 3 THEN 'GOOGL' WHEN 4 THEN 'AMZN'
        WHEN 5 THEN 'TSLA' WHEN 6 THEN 'JPM' WHEN 7 THEN 'GS' WHEN 8 THEN 'MS'
        WHEN 9 THEN 'BAC' WHEN 10 THEN 'WFC' WHEN 11 THEN 'C' WHEN 12 THEN 'BLK'
        WHEN 13 THEN 'SCHW' WHEN 14 THEN 'V' WHEN 15 THEN 'MA'
        WHEN 16 THEN 'NVDA' WHEN 17 THEN 'META' WHEN 18 THEN 'BRK.B'
        WHEN 19 THEN 'UNH' WHEN 20 THEN 'XOM'
    END,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'NYSE' WHEN 2 THEN 'NASDAQ' WHEN 3 THEN 'LSE' WHEN 4 THEN 'TSE'
    END,
    ROUND(UNIFORM(10, 500, RANDOM()) + UNIFORM(0, 9999, RANDOM()) / 10000.0, 4),
    ROUND(UNIFORM(10, 500, RANDOM()) + UNIFORM(0, 9999, RANDOM()) / 10000.0 - 0.05, 4),
    ROUND(UNIFORM(10, 500, RANDOM()) + UNIFORM(0, 9999, RANDOM()) / 10000.0 + 0.05, 4),
    UNIFORM(100, 5000000, RANDOM()),
    DATEADD(second, UNIFORM(0, 15552000, RANDOM()), '2024-01-01'::TIMESTAMP_NTZ),
    CASE UNIFORM(1, 3, RANDOM())
        WHEN 1 THEN 'PRE_MARKET' WHEN 2 THEN 'REGULAR' WHEN 3 THEN 'AFTER_HOURS'
    END
FROM TABLE(GENERATOR(ROWCOUNT => 2000000));

SELECT symbol, exchange, COUNT(*) AS ticks, AVG(price) AS avg_price
FROM market_data GROUP BY 1, 2 ORDER BY ticks DESC LIMIT 20;


-- =============================================================================
-- TABLE 6: PORTFOLIOS (200K rows - DML-Heavy Holdings)
-- =============================================================================
-- Portfolio positions that are rebalanced frequently. Heavy UPDATE/MERGE workload.

CREATE OR REPLACE ICEBERG TABLE portfolios (
    holding_id          NUMBER(38,0),
    account_id          STRING,
    symbol              STRING,
    quantity            NUMBER(15,4),
    avg_cost_basis      NUMBER(12,4),
    current_value       NUMBER(15,2),
    unrealized_pnl      NUMBER(15,2),
    sector              STRING,
    asset_class         STRING,
    last_rebalance_date DATE,
    is_restricted       BOOLEAN DEFAULT FALSE
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_demo_vol'
BASE_LOCATION = 'finserv/portfolios'
TARGET_FILE_SIZE = 'AUTO'
ICEBERG_VERSION = 3;

INSERT INTO portfolios
SELECT
    SEQ4() + 1,
    'ACCT_' || LPAD(UNIFORM(1, 50000, RANDOM())::VARCHAR, 8, '0'),
    CASE UNIFORM(1, 20, RANDOM())
        WHEN 1 THEN 'AAPL' WHEN 2 THEN 'MSFT' WHEN 3 THEN 'GOOGL' WHEN 4 THEN 'AMZN'
        WHEN 5 THEN 'TSLA' WHEN 6 THEN 'JPM' WHEN 7 THEN 'GS' WHEN 8 THEN 'MS'
        WHEN 9 THEN 'BAC' WHEN 10 THEN 'WFC' WHEN 11 THEN 'C' WHEN 12 THEN 'BLK'
        WHEN 13 THEN 'SCHW' WHEN 14 THEN 'V' WHEN 15 THEN 'MA'
        WHEN 16 THEN 'NVDA' WHEN 17 THEN 'META' WHEN 18 THEN 'BRK.B'
        WHEN 19 THEN 'UNH' WHEN 20 THEN 'XOM'
    END,
    ROUND(UNIFORM(1, 10000, RANDOM()) + UNIFORM(0, 9999, RANDOM()) / 10000.0, 4),
    ROUND(UNIFORM(10, 500, RANDOM()) + UNIFORM(0, 9999, RANDOM()) / 10000.0, 4),
    ROUND(UNIFORM(1000, 5000000, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2),
    ROUND(UNIFORM(-500000, 1000000, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2),
    CASE UNIFORM(1, 8, RANDOM())
        WHEN 1 THEN 'TECHNOLOGY' WHEN 2 THEN 'FINANCIALS' WHEN 3 THEN 'HEALTHCARE'
        WHEN 4 THEN 'ENERGY' WHEN 5 THEN 'CONSUMER' WHEN 6 THEN 'INDUSTRIALS'
        WHEN 7 THEN 'UTILITIES' WHEN 8 THEN 'REAL_ESTATE'
    END,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'EQUITY' WHEN 2 THEN 'FIXED_INCOME' WHEN 3 THEN 'DERIVATIVES' WHEN 4 THEN 'ALTERNATIVES'
    END,
    DATEADD(day, -UNIFORM(0, 30, RANDOM()), CURRENT_DATE()),
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 8 THEN TRUE ELSE FALSE END
FROM TABLE(GENERATOR(ROWCOUNT => 200000));

SELECT asset_class, sector, COUNT(*) AS holdings, SUM(current_value) AS total_aum
FROM portfolios GROUP BY 1, 2 ORDER BY total_aum DESC LIMIT 15;


-- =============================================================================
-- TABLE 7: TRANSACTIONS_STAGING (100K rows - CDC Staging for MERGE)
-- =============================================================================
-- Simulates an incremental CDC feed with new transactions and status updates.

CREATE OR REPLACE ICEBERG TABLE transactions_staging (
    txn_id              STRING,
    account_id          STRING,
    txn_type            STRING,
    txn_status          STRING,
    amount              NUMBER(15,2),
    currency            STRING,
    counterparty        STRING,
    channel             STRING,
    txn_timestamp       TIMESTAMP_NTZ,
    settlement_date     DATE,
    region              STRING,
    risk_flag           BOOLEAN DEFAULT FALSE,
    fraud_score         FLOAT,
    cdc_operation       STRING  -- 'INSERT', 'UPDATE', 'DELETE'
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_demo_vol'
BASE_LOCATION = 'finserv/transactions_staging'
TARGET_FILE_SIZE = 'AUTO';

-- New transactions (INSERTs)
INSERT INTO transactions_staging
SELECT
    UUID_STRING(),
    'ACCT_' || LPAD(UNIFORM(1, 500000, RANDOM())::VARCHAR, 8, '0'),
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'PAYMENT' WHEN 2 THEN 'TRANSFER' WHEN 3 THEN 'TRADE_BUY' WHEN 4 THEN 'DEPOSIT'
    END,
    'PENDING',
    ROUND(UNIFORM(100, 50000, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2),
    CASE UNIFORM(1, 3, RANDOM()) WHEN 1 THEN 'USD' WHEN 2 THEN 'EUR' WHEN 3 THEN 'GBP' END,
    'COUNTERPARTY_' || LPAD(UNIFORM(1, 5000, RANDOM())::VARCHAR, 5, '0'),
    CASE UNIFORM(1, 3, RANDOM()) WHEN 1 THEN 'ONLINE' WHEN 2 THEN 'API' WHEN 3 THEN 'MOBILE' END,
    CURRENT_TIMESTAMP(),
    CURRENT_DATE() + 2,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'AMERICAS' WHEN 2 THEN 'EMEA' WHEN 3 THEN 'APAC' WHEN 4 THEN 'LATAM'
    END,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 3 THEN TRUE ELSE FALSE END,
    ROUND(UNIFORM(0, 100, RANDOM()) / 100.0, 4),
    'INSERT'
FROM TABLE(GENERATOR(ROWCOUNT => 70000));

-- Status updates for existing pending transactions (UPDATEs)
INSERT INTO transactions_staging
SELECT
    t.txn_id,
    t.account_id,
    t.txn_type,
    'COMPLETED',
    t.amount,
    t.currency,
    t.counterparty,
    t.channel,
    t.txn_timestamp,
    CURRENT_DATE(),
    t.region,
    t.risk_flag,
    t.fraud_score,
    'UPDATE'
FROM transactions t
WHERE t.txn_status = 'PENDING'
LIMIT 20000;

-- Failed/reversed transactions (DELETEs)
INSERT INTO transactions_staging
SELECT
    t.txn_id,
    t.account_id,
    t.txn_type,
    'REVERSED',
    t.amount,
    t.currency,
    t.counterparty,
    t.channel,
    t.txn_timestamp,
    NULL,
    t.region,
    TRUE,
    99.99,
    'DELETE'
FROM transactions t
WHERE t.txn_status = 'FAILED'
LIMIT 10000;

SELECT cdc_operation, COUNT(*) AS cnt
FROM transactions_staging GROUP BY 1 ORDER BY 1;


-- =============================================================================
-- SUMMARY: DATA MODEL STATISTICS
-- =============================================================================

SELECT 'accounts' AS table_name, COUNT(*) AS row_count FROM accounts
UNION ALL SELECT 'transactions', COUNT(*) FROM transactions
UNION ALL SELECT 'risk_scores', COUNT(*) FROM risk_scores
UNION ALL SELECT 'compliance_events', COUNT(*) FROM compliance_events
UNION ALL SELECT 'market_data', COUNT(*) FROM market_data
UNION ALL SELECT 'portfolios', COUNT(*) FROM portfolios
UNION ALL SELECT 'transactions_staging', COUNT(*) FROM transactions_staging
ORDER BY row_count DESC;

-- Show data relationships
SELECT
    'Total Accounts' AS metric, COUNT(DISTINCT account_id)::VARCHAR AS value FROM accounts
UNION ALL
SELECT 'Accounts with Transactions', COUNT(DISTINCT account_id)::VARCHAR FROM transactions
UNION ALL
SELECT 'Accounts with Risk Scores', COUNT(DISTINCT account_id)::VARCHAR FROM risk_scores
UNION ALL
SELECT 'Unique Symbols (Market Data)', COUNT(DISTINCT symbol)::VARCHAR FROM market_data
UNION ALL
SELECT 'Unique Symbols (Portfolios)', COUNT(DISTINCT symbol)::VARCHAR FROM portfolios;


/*
================================================================================
  DATA GENERATION COMPLETE - FINANCIAL SERVICES MODEL
================================================================================
  
  Total rows: ~10.8M across 7 tables
  
  Table Relationships:
  ┌─────────────────┐     ┌──────────────────┐
  │    accounts     │────>│  transactions    │  (5M rows, streaming)
  │   (500K rows)   │────>│  risk_scores     │  (2M rows, updated daily)
  │                 │────>│  compliance_evts  │  (1M rows, append-only)
  │                 │────>│  portfolios      │  (200K rows, DML-heavy)
  └─────────────────┘     └──────────────────┘
                                    │
  ┌─────────────────┐              │
  │  market_data    │──────────────┘  (joined via symbol)
  │   (2M rows)     │
  └─────────────────┘

  Challenge Mapping:
  - transactions: Small files (Challenge 1), Metadata bloat (2), Concurrency (6)
  - risk_scores: Copy-on-Write (4), Merge-on-Read (5)
  - compliance_events: Append-only, Catalog sync (7)
  - market_data: Missing indexes/Clustering (12)
  - portfolios: Full DML/MERGE (11), Access control (8)
  - accounts: Access control (8), Reference data

  Next: Run 01b_storage_comparison.sql for External Volume vs Managed Storage,
        then 02_challenge_small_files.sql to begin challenge demonstrations.
================================================================================
*/
