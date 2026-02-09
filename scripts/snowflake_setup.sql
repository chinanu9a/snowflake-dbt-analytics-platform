-- ============================================================================
-- Snowflake Setup Script
-- Run this with a SYSADMIN or ACCOUNTADMIN role to create the required objects.
-- ============================================================================

-- 1. Create warehouse
CREATE WAREHOUSE IF NOT EXISTS TRANSFORMING
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for dbt transformations';

-- 2. Create database
CREATE DATABASE IF NOT EXISTS ANALYTICS
    COMMENT = 'Analytics database for stadium concessions project';

-- 3. Create schemas
USE DATABASE ANALYTICS;

CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw/bronze layer — source data loaded from POS and operational systems';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Silver layer — cleaned and standardized staging models';

CREATE SCHEMA IF NOT EXISTS INTERMEDIATE
    COMMENT = 'Intermediate transformations between staging and marts';

CREATE SCHEMA IF NOT EXISTS MARTS
    COMMENT = 'Gold layer — dimensional models and fact tables for analytics';

CREATE SCHEMA IF NOT EXISTS SNAPSHOTS
    COMMENT = 'SCD Type-2 snapshot tables';

CREATE SCHEMA IF NOT EXISTS METRICS
    COMMENT = 'Pre-aggregated metrics views for BI tools';

-- 4. Create role for dbt
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS TRANSFORMER
    COMMENT = 'Role for dbt transformations';

GRANT USAGE ON WAREHOUSE TRANSFORMING TO ROLE TRANSFORMER;
GRANT ALL ON DATABASE ANALYTICS TO ROLE TRANSFORMER;
GRANT ALL ON ALL SCHEMAS IN DATABASE ANALYTICS TO ROLE TRANSFORMER;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE ANALYTICS TO ROLE TRANSFORMER;

-- 5. Create service account (update password)
CREATE USER IF NOT EXISTS DBT_USER
    PASSWORD = 'CHANGE_ME_IMMEDIATELY'
    DEFAULT_ROLE = TRANSFORMER
    DEFAULT_WAREHOUSE = TRANSFORMING
    DEFAULT_NAMESPACE = ANALYTICS.RAW
    COMMENT = 'Service account for dbt';

GRANT ROLE TRANSFORMER TO USER DBT_USER;

-- ============================================================================
-- After running this script:
-- 1. Change the password for DBT_USER
-- 2. Set environment variables:
--    export SNOWFLAKE_ACCOUNT='your_account'
--    export SNOWFLAKE_USER='DBT_USER'
--    export SNOWFLAKE_PASSWORD='your_password'
--    export SNOWFLAKE_ROLE='TRANSFORMER'
--    export SNOWFLAKE_DATABASE='ANALYTICS'
--    export SNOWFLAKE_WAREHOUSE='TRANSFORMING'
-- 3. Update profiles.yml to use the dev_snowflake target
-- 4. Run: dbt debug --target dev_snowflake
-- ============================================================================
