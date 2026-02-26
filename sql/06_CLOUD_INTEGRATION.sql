-- =====================================================
-- 06_CLOUD INTEGRATION (read and execute sequentially)
-- Purpose: Create integrations to Azure Storage, create stage and file format
-- Role: AccountAdmin and ecommerce_ingest
-- IMPORTANT BEFORE COMMITTING TO GITHUB OR SHARING THIS CODE:
-- REPLACE TENANT_ID, STORAGE_URL and STORAGE_QUEUE_URI with placeholder values to avoid sharing sensitive information and getting billed by Azure for smn else usage.
-- =====================================================

-- =====================================================
-- SECTION 1: ACCOUNT-LEVEL INTEGRATIONS
-- ====================================================
-- set context
USE role accountadmin;

-- Azure Setup for Snowflake (Blob + Event Grid + Queue)
-- Prerequisites:
-- Azure subscription enabled.
-- 1. Get Tenant ID. Go to Microsoft Entra ID, Copy Tenant ID
-- 2. Create Resource Group, Go to Resource Group, Create, Name: snowflake_project1_resource_group, Region: same as your Snowflake account
-- 3. Create Storage Account, Under the resource group: Name: snowflakestorageproject (globally unique, lowercase)
-- Region: same as Snowflake, Performance: Standard, Redundancy: LRS, Primary service: Azure Blob Storage
-- 4. Create Blob Container, Storage Account → Blob Containers → Create, Name: landing, Copy container URL: https://snowflakestorageproject.blob.core.windows.net/landing this will be your storage account
-- 5. Create Storage Queue,  Storage Account → Queues → Create, Name: snowflakestoragequeue copy URL this is your storage queue uri https://snowflakestorageproject.queue.core.windows.net/snowflakestoragequeue
-- 6. Create Event Subscription, Storage Account → Events → + Event Subscription, Name: snowflakestoragesubscription, System topic: snowflaketopicproject1, 
-- Event type: Blob Created, Endpoint type: Storage Queue, Endpoint: snowflakestoragequeue, Create.
-- Architecture
-- Blob Container → Event Grid → Storage Queue → Snowflake Pipe (auto_ingest=true)

set TENANT_ID = 'your tenat id here';
set STORAGE_URL = 'your storage url here (note the format should be 'azure://url' e.g. azure://some_account_one.blob.core.windows.net/some_blob_name)';
set STORAGE_QUEUE_URI = 'your storage queue uri here';

-- create azure storage integration (for listing, reading files in azure blob container)
CREATE OR REPLACE STORAGE INTEGRATION azure_storage_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = AZURE
ENABLED = TRUE
AZURE_TENANT_ID = $TENANT_ID
STORAGE_ALLOWED_LOCATIONS = ($STORAGE_URL);

-- Run Desc below, click on azure_consent_url in results. You will be redirected to Azure consent for snowflake app access, copy name of snowflake app and agree 
DESC STORAGE INTEGRATION azure_storage_int;
-- Now visit your Azure -> STORAGE ACCOUNT -> ACCESS CONTROL IAM -> ADD ROLE ASSIGNMENT: Storage Blob Data Contributor -> NEXT -> SELECT MEMBERS -> Search for snowflake app name copied after step above -> CLICK ON THE APP -> SELECT -> REVIEW + ASSIGN

GRANT USAGE ON INTEGRATION AZURE_STORAGE_INT TO ROLE ecommerce_ingest;

-- create azure integration for storage queue notification 
CREATE OR REPLACE NOTIFICATION INTEGRATION azure_event_int
TYPE = QUEUE
NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
ENABLED = TRUE
AZURE_STORAGE_QUEUE_PRIMARY_URI = $STORAGE_QUEUE_URI
AZURE_TENANT_ID = $TENANT_ID;

-- visit azure consent url shown after running desc and grant snowflake access to azure 
DESC NOTIFICATION INTEGRATION azure_event_int;
-- Now visit your Azure -> STORAGE ACCOUNT -> ACCESS CONTROL IAM -> ADD ROLE ASSIGNMENT: Storage Queue Data Contributor -> NEXT -> SELECT MEMBERS -> Search for snowflake app name copied after step above -> CLICK ON THE APP -> SELECT -> REVIEW + ASSIGN

GRANT USAGE ON INTEGRATION azure_event_int TO ROLE ecommerce_ingest;

-- test configuration, show existing storage and notification integration
SHOW STORAGE INTEGRATIONS;
SHOW NOTIFICATION INTEGRATIONS;

-- =====================================================
-- SECTION 2: DATABASE-LEVEL INGESTION OBJECTS
-- =====================================================
-- create stage and file format
USE ROLE ecommerce_ingest;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;

-- create file format for reading jsons
CREATE OR REPLACE FILE FORMAT json_ff
TYPE = JSON
strip_outer_array = TRUE;

-- create stage for reading files 
CREATE OR REPLACE STAGE raw_stage
URL =  $STORAGE_URL
STORAGE_INTEGRATION = azure_storage_int
FILE_FORMAT = json_ff
DIRECTORY = (ENABLE=TRUE);

-- check stage properties
DESCRIBE STAGE raw_stage;

-- Test integration works by attempting to list files on azure blog (should return empty list)
LIST @raw_stage;




