-- =====================================================
-- 00_ROLES.sql
-- Purpose: Define roles to separate privileges and capabilities
-- Roles will have no PRIVILEGES at creation but we will grant some as we create objects 
-- Required role: ACCOUNTADMIN
-- At row 24 replace <user> with your snowflake user name (run select current_user; to get it)
-- =====================================================

--set context
USE ROLE accountadmin;

-- Create 4 separate roles for admin (creation of objects), ingest (run ingestion actions), transform (run transform actions), analyst(query)
CREATE ROLE IF NOT EXISTS ecommerce_admin;
CREATE ROLE IF NOT EXISTS ecommerce_ingest;
CREATE ROLE IF NOT EXISTS ecommerce_transform;
CREATE ROLE IF NOT EXISTS ecommerce_analyst;

-- Allow admin role to perfrom all actions by inheriting from dependant roles
GRANT ROLE ecommerce_ingest TO ROLE ecommerce_admin;
GRANT ROLE ecommerce_transform TO ROLE ecommerce_admin;
GRANT ROLE ecommerce_analyst TO ROLE ecommerce_admin;

-- grant admin role to user (get yours using SELECT CURRENT_USER();)
-- obtain the name of current user
-- select current_user;

-- grant role to user
GRANT ROLE ecommerce_admin TO USER SFE_ADMIN;


-- Check results
-- show roles:
SHOW ROLES like 'ECOMMERCE%';
-- Check if admin roles had 3 roles granted
SHOW GRANTS TO ROLE ecommerce_admin;