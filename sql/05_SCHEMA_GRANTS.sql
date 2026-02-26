-- =====================================================
-- 05_SCHEMA_GRANTS.sql
-- Purpose: grant schema access to roles
-- Domain role: ECOMMERCE_ADMIN
-- Database: ECOMMERCE_DB
-- =====================================================

-- set context
USE ROLE ECOMMERCE_ADMIN;
USE DATABASE ECOMMERCE_DB; 

-- =========================
-- RAW (ingestion layer)
-- ingestion role will only ingest files, using file format and stage
-- =========================

-- parent object access
GRANT USAGE ON DATABASE ECOMMERCE_DB TO ROLE ecommerce_ingest;
GRANT USAGE ON SCHEMA RAW TO ROLE ecommerce_ingest;

-- Schema object access
GRANT CREATE TABLE ON SCHEMA RAW TO ROLE ecommerce_ingest;
GRANT CREATE STAGE ON SCHEMA RAW TO ROLE ecommerce_ingest;
GRANT CREATE FILE FORMAT ON SCHEMA RAW TO ROLE ecommerce_ingest;
GRANT CREATE PIPE ON SCHEMA RAW TO ROLE ecommerce_ingest;

-- =========================
-- STAGE (transformation layer)
-- transform role will be crating tables, views in stage and curated
-- =========================

-- Assumption: transform role owns all objects created in STAGED and CURATED
-- parent object access
GRANT USAGE ON DATABASE ECOMMERCE_DB TO ROLE ecommerce_transform;
GRANT USAGE ON SCHEMA RAW TO ROLE ecommerce_transform;
GRANT USAGE ON SCHEMA STAGED TO ROLE ecommerce_transform;
GRANT USAGE ON SCHEMA CURATED TO ROLE ecommerce_transform;
GRANT USAGE ON SCHEMA ORCHESTRATION TO ROLE ecommerce_transform;

-- Schema object access
GRANT SELECT ON ALL TABLES IN SCHEMA RAW TO ROLE ecommerce_transform;
GRANT SELECT ON FUTURE TABLES IN SCHEMA RAW TO ROLE ecommerce_transform;
GRANT CREATE STREAM ON SCHEMA ecommerce_db.staged TO ROLE ecommerce_transform;

GRANT CREATE TABLE ON SCHEMA CURATED TO ROLE ecommerce_transform;
GRANT CREATE TABLE ON SCHEMA STAGED TO ROLE ecommerce_transform;

GRANT CREATE VIEW ON SCHEMA STAGED TO ROLE ecommerce_transform;
GRANT CREATE VIEW ON SCHEMA CURATED TO ROLE ecommerce_transform;

GRANT CREATE TASK ON SCHEMA ORCHESTRATION TO ROLE ecommerce_transform;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE ecommerce_transform;

GRANT CREATE PROCEDURE ON SCHEMA CURATED TO ROLE ecommerce_transform;
GRANT CREATE PROCEDURE ON SCHEMA STAGED TO ROLE ecommerce_transform;
GRANT CREATE TASK ON SCHEMA CURATED TO ROLE ecommerce_transform;

GRANT CREATE TABLE ON SCHEMA ORCHESTRATION TO ROLE ecommerce_transform;


-- =========================
-- CURATED (presentation layer)
-- analyst will consume only from curated 
-- =========================
-- parent object access
GRANT USAGE ON DATABASE ECOMMERCE_DB TO ROLE ecommerce_analyst;
GRANT USAGE ON SCHEMA CURATED TO ROLE ecommerce_analyst;

-- Schema object access
GRANT SELECT ON ALL TABLES IN SCHEMA CURATED TO ROLE ecommerce_analyst;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CURATED TO ROLE ecommerce_analyst;

--Inspect grants assigned to role
SHOW GRANTS TO ROLE ecommerce_transform;
-- SHOW GRANTS TO ROLE ecommerce_ingest;
-- SHOW GRANTS TO ROLE ecommerce_analyst;

--Inspect grants assigned to schemas
-- SHOW GRANTS ON SCHEMA ECOMMERCE_DB.RAW;
-- SHOW GRANTS ON SCHEMA ECOMMERCE_DB.STAGED;
-- SHOW GRANTS ON SCHEMA ECOMMERCE_DB.CURATED;


