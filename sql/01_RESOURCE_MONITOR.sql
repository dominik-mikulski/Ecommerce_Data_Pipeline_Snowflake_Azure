-- =====================================================
-- 01_RESOURCE_MONITOR.sql
-- Purpose: Define monitor to manage credits spend (and avoid accidental burn)
-- Resource Monitor belongs to account admin role
-- Required role: ACCOUNTADMIN
-- =====================================================

USE ROLE accountadmin;

CREATE RESOURCE MONITOR IF NOT EXISTS RM_ECOMMERCE_DAILY_3C 
WITH
    CREDIT_QUOTA = 3
    FREQUENCY = DAILY
    START_TIMESTAMP = IMMEDIATELY
    NOTIFY_USERS = (sfe_admin)
    TRIGGERS 
        ON 50 PERCENT DO NOTIFY
        ON 85 PERCENT DO NOTIFY
        ON 95 PERCENT DO SUSPEND_IMMEDIATE;

-- List monitors:
SHOW RESOURCE MONITORS;