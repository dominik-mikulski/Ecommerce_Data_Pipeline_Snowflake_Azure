-- =====================================================
-- 03_WAREHOUSES_GRANTS.sql
-- Purpose: grant warehouses rights (usage - ie. run queries with wf, operate - start, stop resize wh, monitor - observe usage, cost) to the roles
-- ADMIN role will have
-- Required role: ACCOUNTADMIN
-- =====================================================

USE ROLE accountadmin;

GRANT USAGE, OPERATE, MONITOR ON WAREHOUSE wh_admin TO ROLE ecommerce_admin;
GRANT USAGE, OPERATE ON WAREHOUSE wh_ingest TO ROLE ecommerce_ingest;
GRANT USAGE, OPERATE ON WAREHOUSE wh_transform TO ROLE ecommerce_transform;
GRANT USAGE ON WAREHOUSE wh_analyst TO ROLE ecommerce_analyst;

-- check roles grants
SHOW GRANTS TO ROLE ecommerce_admin;
-- SHOW GRANTS TO ROLE ecommerce_ingest;
-- SHOW GRANTS TO ROLE ecommerce_transform;
-- SHOW GRANTS TO ROLE ecommerce_analyst;