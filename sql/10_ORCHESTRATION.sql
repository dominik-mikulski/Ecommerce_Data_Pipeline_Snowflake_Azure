-- =====================================================
-- 10_ORCHERSTRATION.sql
-- Purpose: Create tasks that run procedures
-- Role: ecommerce_transform,wh: wh_transform
-- =====================================================

-- set context
USE ROLE ecommerce_transform;
USE WAREHOUSE wh_transform;
USE SCHEMA ecommerce_db.orchestration;

--create task that will run when stream has data
CREATE OR REPLACE TASK ecommerce_db.orchestration.tsk_raw_to_stg_orders
   WAREHOUSE = WH_TRANSFORM
   WHEN SYSTEM$STREAM_HAS_DATA('ecommerce_db.staged.raw_orders_stream')
AS
    DECLARE
        v_result STRING;
    BEGIN
        CALL ecommerce_db.staged.sp_raw_to_stg_orders() INTO :v_result;
        CALL SYSTEM$SET_RETURN_VALUE(:v_result);
    END;
--    CALL ecommerce_db.staged.sp_raw_to_stg_orders();



CREATE OR REPLACE TASK ecommerce_db.orchestration.tsk_stg_to_curated
WAREHOUSE = WH_TRANSFORM
AFTER ecommerce_db.orchestration.tsk_raw_to_stg_orders
WHEN
    SYSTEM$STREAM_HAS_DATA('ecommerce_db.staged.stg_orders_stream')
    OR SYSTEM$STREAM_HAS_DATA('ecommerce_db.staged.stg_order_items_stream')
AS
    DECLARE
        v_result STRING;
    BEGIN
        CALL ecommerce_db.curated.sp_stg_to_curated() INTO :v_result;
        CALL SYSTEM$SET_RETURN_VALUE(:v_result);
    END;
--CALL ecommerce_db.curated.sp_stg_to_curated();

-- suspend tasks
-- ALTER task ecommerce_db.orchestration.tsk_raw_to_stg_orders SUSPEND;
-- ALTER TASK ecommerce_db.orchestration.tsk_stg_to_curated SUSPEND;


-- enable tasks (by default suspended)
ALTER TASK ecommerce_db.orchestration.tsk_stg_to_curated RESUME;
ALTER task ecommerce_db.orchestration.tsk_raw_to_stg_orders RESUME;

-- check tasks status
SHOW TASKS IN SCHEMA ecommerce_db.orchestration;

-- Create table which will get updated whenever tasks run procedures with:
-- name of file triggering the pipeline
-- name of procedure runnning (raw to staged, or staged to curated)
-- name of step in the procedure (reading stream, merging into order table, delete & insert into order items table)
-- count of rows processed
-- status of step
-- error msg if applicable
-- Note transofrmations are run in single procedure block for atomicity (i.e. if one steps fails everything is rolled back and you never look at half updated tables)

USE ROLE ecommerce_transform;
USE WAREHOUSE wh_transform;
USE SCHEMA ecommerce_db.orchestration;

CREATE OR REPLACE TABLE ecommerce_db.orchestration.pipeline_audit (
    audit_id        NUMBER AUTOINCREMENT,
    file_name       STRING,               -- source file processed
    procedure_name  STRING,               -- SP_RAW_TO_STG_ORDERS / SP_STG_TO_CURATED
    stage_step      STRING,               -- STREAM_READ / MERGE / DELETE / INSERT / etc.
    event_ts        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    affected_rows   NUMBER,               -- SQLROWCOUNT or COUNT(*)
    status          STRING,               -- SUCCESS | FAILED
    error_message   STRING,               -- populated only on failure
    query_id        STRING                -- LAST_QUERY_ID() for task correlation
)
COMMENT = 'Event-level audit log for pipeline execution (file + procedure + step)';

