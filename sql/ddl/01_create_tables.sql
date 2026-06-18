-- ============================================================================
-- DDL: Healthcare Data Platform — Core Table Definitions
-- Layer: Gold (Serving Layer)
-- Database: Azure SQL / Databricks Unity Catalog
-- ============================================================================

-- ============================================================================
-- CLAIMS FACT TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS gold.claims_fact (
    -- Surrogate key
    claim_key               BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Natural keys
    claim_id                VARCHAR(50)     NOT NULL,
    claim_line_id           VARCHAR(50)     NOT NULL,
    claim_type_code         VARCHAR(10)     NOT NULL,   -- P=Professional, I=Institutional, D=Dental

    -- Member dimension FK
    member_id               VARCHAR(50)     NOT NULL,   -- SHA-256 hashed (PHI)
    subscriber_id           VARCHAR(50),
    group_id                VARCHAR(50),
    plan_id                 VARCHAR(50)     NOT NULL,

    -- Provider dimension FKs
    rendering_provider_npi  VARCHAR(20),
    billing_provider_npi    VARCHAR(20),
    facility_npi            VARCHAR(20),

    -- Date dimension FKs
    service_date            DATE            NOT NULL,
    from_date               DATE,
    to_date                 DATE,
    received_date           DATE,
    paid_date               DATE,
    admit_date              DATE,
    discharge_date          DATE,

    -- Procedure & diagnosis
    procedure_code          VARCHAR(20),
    procedure_modifier_1    VARCHAR(10),
    procedure_modifier_2    VARCHAR(10),
    primary_diagnosis_code  VARCHAR(20),
    diagnosis_code_2        VARCHAR(20),
    diagnosis_code_3        VARCHAR(20),
    revenue_code            VARCHAR(10),
    drg_code                VARCHAR(10),
    place_of_service_code   VARCHAR(5),
    service_category        VARCHAR(50),

    -- Claim status
    claim_status            VARCHAR(20)     NOT NULL,   -- PAID, DENIED, PENDING, ADJUSTED, VOID

    -- Financial measures
    billed_amount           DECIMAL(18,2)   DEFAULT 0,
    allowed_amount          DECIMAL(18,2)   DEFAULT 0,
    plan_paid_amount        DECIMAL(18,2)   DEFAULT 0,
    member_deductible_amt   DECIMAL(18,2)   DEFAULT 0,
    member_copay_amt        DECIMAL(18,2)   DEFAULT 0,
    member_coinsurance_amt  DECIMAL(18,2)   DEFAULT 0,
    member_other_liability  DECIMAL(18,2)   DEFAULT 0,
    cob_amount              DECIMAL(18,2)   DEFAULT 0,

    -- Utilization
    units_of_service        INT             DEFAULT 1,
    length_of_stay          INT,
    admit_type_code         VARCHAR(5),
    discharge_disposition   VARCHAR(10),

    -- HIPAA compliance metadata
    _phi_masked             BOOLEAN         DEFAULT TRUE,
    _phi_mask_mode          VARCHAR(20)     DEFAULT 'hash',

    -- Pipeline metadata
    _source_id              VARCHAR(100),
    _ingested_at            TIMESTAMP,
    _batch_date             DATE,
    _environment            VARCHAR(10),

    -- Audit
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
)
USING DELTA
PARTITIONED BY (service_date, claim_status)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.enableChangeDataFeed'       = 'true',   -- For downstream CDC
    'classification'                   = 'PHI-Safe',
    'owner'                            = 'healthcare-data-platform'
);

-- ============================================================================
-- MEMBER ELIGIBILITY DIMENSION (SCD Type 2)
-- ============================================================================
CREATE TABLE IF NOT EXISTS gold.member_eligibility (
    eligibility_key         BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    member_id               VARCHAR(50)     NOT NULL,   -- SHA-256 hashed
    subscriber_id           VARCHAR(50),
    group_id                VARCHAR(50),
    plan_id                 VARCHAR(50)     NOT NULL,
    plan_name               VARCHAR(200),
    metal_tier              VARCHAR(20),    -- BRONZE, SILVER, GOLD, PLATINUM
    product_type            VARCHAR(50),    -- HMO, PPO, EPO, HDHP

    -- Enrollment period
    effective_date          DATE            NOT NULL,
    termination_date        DATE,
    plan_year               INT             NOT NULL,
    is_active               BOOLEAN         DEFAULT TRUE,
    is_continuously_enrolled BOOLEAN        DEFAULT FALSE,

    -- Demographics (hashed)
    birth_date              VARCHAR(64),    -- SHA-256 hashed
    gender_code             VARCHAR(5),
    relationship_code       VARCHAR(5),     -- 01=Self, 02=Spouse, 03=Child

    -- Geographic (non-PHI at ZIP level)
    zip_code                VARCHAR(10),
    county_fips             VARCHAR(10),
    state_code              VARCHAR(5),

    -- Age derived fields (calculated, not stored PHI)
    age_as_of_dec31         INT,

    -- SCD Type 2 fields
    scd_effective_date      DATE            NOT NULL,
    scd_expiry_date         DATE            DEFAULT '9999-12-31',
    scd_is_current          BOOLEAN         DEFAULT TRUE,
    scd_version             INT             DEFAULT 1,

    -- Audit
    _ingested_at            TIMESTAMP,
    _batch_date             DATE,
    _environment            VARCHAR(10)
)
USING DELTA
PARTITIONED BY (plan_year, is_active)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.enableChangeDataFeed'       = 'true',
    'classification'                   = 'PHI-Safe'
);

-- ============================================================================
-- PROVIDER MASTER DIMENSION
-- ============================================================================
CREATE TABLE IF NOT EXISTS gold.provider_master (
    provider_key            BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    provider_npi            VARCHAR(20)     NOT NULL UNIQUE,
    provider_name           VARCHAR(200),   -- Hashed for HIPAA
    provider_type_code      VARCHAR(10),
    specialty_code          VARCHAR(20),
    specialty_description   VARCHAR(200),
    tax_id                  VARCHAR(64),    -- Hashed
    state_code              VARCHAR(5),
    zip_code                VARCHAR(10),
    network_status          VARCHAR(20),    -- IN_NETWORK, OUT_OF_NETWORK
    contract_effective_date DATE,
    contract_termination_date DATE,
    is_active               BOOLEAN         DEFAULT TRUE,
    credentialing_status    VARCHAR(20),

    -- Audit
    _ingested_at            TIMESTAMP,
    _batch_date             DATE
)
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- ============================================================================
-- PIPELINE AUDIT TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id                  BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_name           VARCHAR(100)    NOT NULL,
    source_id               VARCHAR(100),
    run_date                DATE            NOT NULL,
    environment             VARCHAR(20)     NOT NULL,
    status                  VARCHAR(20),    -- RUNNING, SUCCESS, FAILED, NO_DATA
    started_at              TIMESTAMP,
    completed_at            TIMESTAMP,
    records_read            INT             DEFAULT 0,
    records_written         INT             DEFAULT 0,
    dq_score                DECIMAL(5,1)    DEFAULT 0,
    error_message           VARCHAR(2000),  -- Never contains PHI
    sla_hour                INT             DEFAULT 6,
    sla_met                 BOOLEAN
)
USING DELTA
PARTITIONED BY (run_date)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- ============================================================================
-- PIPELINE WATERMARKS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS audit.pipeline_watermarks (
    watermark_id            BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_name           VARCHAR(100)    NOT NULL,
    environment             VARCHAR(20)     NOT NULL,
    watermark_value         VARCHAR(100),
    records_loaded          INT             DEFAULT 0,
    updated_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (pipeline_name, environment)
)
USING DELTA;
