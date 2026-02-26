-- =====================================================
-- 07_INGESTION.sql
-- Purpose: Create automated pipe that will append new files arriving into raw_table 
-- Role: ecommerce_ingest,wh: wh_ingest
-- =====================================================

-- =====================================================
-- CONTEXT SET AND TABLE CREATION
-- =====================================================
USE ROLE ecommerce_ingest;
USE SCHEMA ecommerce_db.raw;
USE WAREHOUSE wh_ingest;

-- create table with file name, row number, load time (local timestamp) and json record stored as row
CREATE TABLE IF NOT EXISTS raw_orders_json (file_name text, row_number number, load_ts timestamp with local time zone, payload variant);

-- =====================================================
-- OPTIONAL MANUAL INGEST TEST BEFORE PIPE CREATION
-- =====================================================
-- view stage metadata
-- select * from from directory(@raw_stage);

-- view the data for table
-- select METADATA$FILENAME as file_name, METADATA$FILE_ROW_NUMBER as row_number, CURRENT_TIMESTAMP() as load_ts, $1 as payload from @raw_stage
-- (file_format => json_ff);

-- copy data into table
-- COPY INTO raw_orders_json from 
-- (select 
-- METADATA$FILENAME as file_name, 
-- METADATA$FILE_ROW_NUMBER as row_number, 
-- CURRENT_TIMESTAMP() as load_ts, 
-- $1 as payload from @raw_stage) file_format = (format_name = json_ff);

-- check the table content
-- select * from raw_orders_json;

-- =====================================================
-- CREATE PIPE 
-- ===================================================

-- before creating pipe an integration needs to be created and working
CREATE OR REPLACE PIPE raw_orders_pipe
AUTO_INGEST = TRUE
INTEGRATION = azure_event_int
AS 
COPY INTO raw_orders_json 
FROM ( 
    SELECT 
        METADATA$FILENAME as file_name, 
        METADATA$FILE_ROW_NUMBER as row_number, 
        CURRENT_TIMESTAMP() as load_ts, 
        $1 as payload 
    FROM @raw_stage
) 
FILE_FORMAT = (FORMAT_NAME = json_ff);

-- check if pipe was created successfully
SELECT SYSTEM$PIPE_STATUS('raw_orders_pipe');



