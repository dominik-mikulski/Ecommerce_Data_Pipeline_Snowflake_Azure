-- ==========================================
-- 12_RESET - purge tables, restart tasks. Deletes data  
-- ==========================================

use role ecommerce_admin;
use warehouse wh_admin;

-- RESET all tables
truncate table ecommerce_db.raw.raw_orders_json;
truncate table ecommerce_db.staged.stg_orders;
truncate table ecommerce_db.staged.stg_order_items;
truncate table ecommerce_db.staged.transform_audit;
truncate table ecommerce_db.curated.dim_customer;
truncate table ecommerce_db.curated.dim_product;
truncate table ecommerce_db.curated.fact_orders;
truncate table ecommerce_db.curated.fact_order_items;
truncate table ecommerce_db.orchestration.pipeline_audit;

-- suspend tasks
ALTER task ecommerce_db.orchestration.tsk_raw_to_stg_orders SUSPEND;
ALTER TASK ecommerce_db.orchestration.tsk_stg_to_curated SUSPEND;

-- enable tasks 
ALTER TASK ecommerce_db.orchestration.tsk_stg_to_curated RESUME;
ALTER task ecommerce_db.orchestration.tsk_raw_to_stg_orders RESUME;