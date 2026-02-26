-- =====================================================
-- 04_DATABASES.sql
-- Purpose: create ecommerce domain database and schemas
-- Governance role: ACCOUNTADMIN
-- Domain role: ECOMMERCE_ADMIN
-- =====================================================

-- Platform bootstrap
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS ECOMMERCE_DB;

-- Delegate database control
GRANT ALL PRIVILEGES ON DATABASE ECOMMERCE_DB TO ROLE ECOMMERCE_ADMIN;

-- Set the context
USE ROLE ECOMMERCE_ADMIN;
USE DATABASE ECOMMERCE_DB;

-- Lock down PUBLIC schema 
REVOKE ALL ON SCHEMA PUBLIC FROM ROLE PUBLIC;

-- Create domain schemas
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGED;
CREATE SCHEMA IF NOT EXISTS CURATED;
CREATE SCHEMA IF NOT EXISTS ORCHESTRATION;

-- Verify
SHOW SCHEMAS;