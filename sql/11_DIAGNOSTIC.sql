-- ==========================================
-- 11_DIAGNOSTIC - Block of code to run manual diagnostics 
-- ==========================================

-- Select items across all pipeline layers to see if streams get emptied, target tables increment
select 'RAW' as layer, 'raw_orders_json' as object_name, count(*) as row_count
from ecommerce_db.raw.raw_orders_json
union all
select 'STREAM', 'RAW_ORDERS_STREAM', count(*)
from ecommerce_db.staged.RAW_ORDERS_STREAM
union all
select 'STAGED', 'stg_orders', count(*)
from ecommerce_db.staged.stg_orders
union all
select 'STREAM', 'STG_ORDERS_STREAM', count(*)
from ecommerce_db.staged.STG_ORDERS_STREAM
union all
select 'STAGED', 'stg_order_items', count(*)
from ecommerce_db.staged.stg_order_items
union all
select 'STREAM', 'STG_ORDER_ITEMS_STREAM', count(*)
from ecommerce_db.staged.STG_ORDER_ITEMS_STREAM
union all
select 'CURATED', 'dim_customer', count(*)
from ecommerce_db.curated.dim_customer
union all
select 'CURATED', 'dim_product', count(*)
from ecommerce_db.curated.dim_product
union all
select 'CURATED', 'fact_orders', count(*)
from ecommerce_db.curated.fact_orders
union all
select 'CURATED', 'fact_order_items', count(*)
from ecommerce_db.curated.fact_order_items;
from ecommerce_db.curated.fact_order_items;

-- show stream status
SHOW STREAMS IN ACCOUNT;

-- show tasks status
SHOW TASKS IN ACCOUNT;

-- show files in stage
list @ECOMMERCE_DB.RAW.RAW_STAGE;

-- show pipes 
SHOW PIPES IN ACCOUNT;

-- show pipe status (one can inspect if notification came in and when last processed)
SELECT SYSTEM$PIPE_STATUS('ECOMMERCE_DB.RAW.RAW_ORDERS_PIPE');

-- show pipe usage history also useful
SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
        PIPE_NAME => 'ECOMMERCE_DB.RAW.RAW_ORDERS_PIPE',
        DATE_RANGE_START => DATEADD('HOUR', -48, CURRENT_TIMESTAMP())
    )
);

-- show copy history (in our case all dobe by raw orders pipe)
select *
from table(information_schema.copy_history(TABLE_NAME=>'ECOMMERCE_DB.RAW.RAW_ORDERS_JSON', START_TIME=> DATEADD(hours, -48, CURRENT_TIMESTAMP())));

-- show history of tasks 
SELECT *
  FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.TASK_HISTORY())
  WHERE NAME ILIKE 'tsk%'
  ORDER BY SCHEDULED_TIME DESC;

  select * from ECOMMERCE_DB.ORCHESTRATION.PIPELINE_AUDIT;

