-- =====================================================
-- 09_Transform setup.sql
-- Purpose: Create stream on raw table, define procedure transforming raw table into order and order items staged tables with observability
-- Role: ecommerce_transform,wh: wh_transform
-- =====================================================

USE ROLE ecommerce_transform;
USE WAREHOUSE wh_transform;
USE SCHEMA ecommerce_db.staged;

CREATE OR REPLACE STREAM raw_orders_stream
ON TABLE ecommerce_db.raw.raw_orders_json;

-- test is stream has any records 
-- select * from raw_orders_stream;

create table IF NOT EXISTS stg_orders (customer_id number,
loyalty_tier text,
segment text,
order_date date,
order_id number,
payment_amount number,
payment_currency text,
payment_method text,
shipping_city text,
shipping_country text,
shipping_delivery_type text,
status text,
file_name text,
ingested_at_ltz timestamp with local time zone,
updated_at_ltz timestamp with local time zone
);

create table IF NOT EXISTS stg_order_items 
(
order_id number,
order_date date,
product_id number,
category text,
quantity number,
unit_price number,
file_name text,
ingested_at_ltz  timestamp with local time zone,
updated_at_ltz  timestamp with local time zone
);

-- create table to store audit information from transformation procedure
CREATE TABLE IF NOT EXISTS staged.transform_audit (
    batch_id        NUMBER AUTOINCREMENT,
    batch_ts        TIMESTAMP_LTZ,
    stream_rows     NUMBER,
    merged_rows     NUMBER,
    deleted_rows    NUMBER,
    inserted_rows   NUMBER,
    status          STRING,
    error_message   STRING
);


CREATE OR REPLACE PROCEDURE STAGED.SP_RAW_TO_STG_ORDERS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
   v_latest_count NUMBER;
   v_merge_count  NUMBER;
   v_delete_count NUMBER;
   v_insert_count NUMBER;

   v_file_name    STRING;
   v_current_step STRING;

BEGIN

-- =====================================================
-- STREAM_READ + TMP_CREATE
-- =====================================================

v_current_step := 'STREAM_READ_CREATE_TMP';

CREATE OR REPLACE TEMPORARY TABLE latest_orders AS 
SELECT
    payload:order_id::number as order_id,
    payload as payload,
    file_name::text as file_name,
    load_ts::timestamp_ltz as ingested_at_ltz
FROM ecommerce_db.staged.raw_orders_stream
QUALIFY row_number() over (partition by order_id order by load_ts DESC) = 1;

SELECT COUNT(*) INTO v_latest_count FROM latest_orders;
SELECT MAX(file_name) INTO v_file_name FROM latest_orders;

INSERT INTO ecommerce_db.orchestration.pipeline_audit (
    file_name,
    procedure_name,
    stage_step,
    affected_rows,
    status,
    query_id
)
VALUES (
    :v_file_name,
    'SP_RAW_TO_STG_ORDERS',
    :v_current_step,
    :v_latest_count,
    'SUCCESS',
    LAST_QUERY_ID()
);

-- =====================================================
-- MERGE STG_ORDERS
-- =====================================================

v_current_step := 'MERGE_STG_ORDERS';

MERGE INTO stg_orders o USING
(
    SELECT
        payload:customer:customer_id::number as customer_id,
        payload:customer:loyalty_tier::text as loyalty_tier,
        payload:customer:segment::text as segment,
        payload:order_date::date as order_date,
        payload:order_id::number as order_id,
        payload:payment:amount::number as payment_amount,
        payload:payment:currency::text as payment_currency,
        payload:payment:method::text as payment_method,
        payload:shipping:city::text as shipping_city,
        payload:shipping:country::text as shipping_country,
        payload:shipping:delivery_type::text as shipping_delivery_type,
        payload:status::text as status,
        file_name,
        ingested_at_ltz,
        current_timestamp(0) as updated_at_ltz
    FROM latest_orders
) s
ON s.order_id = o.order_id
WHEN MATCHED THEN UPDATE ALL BY NAME
WHEN NOT MATCHED THEN INSERT ALL BY NAME;

v_merge_count := SQLROWCOUNT;

INSERT INTO ecommerce_db.orchestration.pipeline_audit (
    file_name,
    procedure_name,
    stage_step,
    affected_rows,
    status,
    query_id
)
VALUES (
    :v_file_name,
    'SP_RAW_TO_STG_ORDERS',
    :v_current_step,
    :v_merge_count,
    'SUCCESS',
    LAST_QUERY_ID()
);

-- =====================================================
-- DELETE STG_ORDER_ITEMS
-- =====================================================

v_current_step := 'DELETE_STG_ORDER_ITEMS';

DELETE FROM stg_order_items 
WHERE order_id IN (SELECT order_id FROM latest_orders);

v_delete_count := SQLROWCOUNT;

INSERT INTO ecommerce_db.orchestration.pipeline_audit (
    file_name,
    procedure_name,
    stage_step,
    affected_rows,
    status,
    query_id
)
VALUES (
    :v_file_name,
    'SP_RAW_TO_STG_ORDERS',
    :v_current_step,
    :v_delete_count,
    'SUCCESS',
    LAST_QUERY_ID()
);

-- =====================================================
-- INSERT STG_ORDER_ITEMS
-- =====================================================

v_current_step := 'INSERT_STG_ORDER_ITEMS';

INSERT INTO stg_order_items 
SELECT
    ls.order_id,
    ls.payload:order_date::date as order_date,
    payload_items.value:product_id::number,
    payload_items.value:category::text,
    payload_items.value:quantity::number,
    payload_items.value:unit_price::number,
    ls.file_name,
    ls.ingested_at_ltz,
    current_timestamp(0)
FROM latest_orders ls,
LATERAL FLATTEN(input => ls.payload:items) payload_items;

v_insert_count := SQLROWCOUNT;

INSERT INTO ecommerce_db.orchestration.pipeline_audit (
    file_name,
    procedure_name,
    stage_step,
    affected_rows,
    status,
    query_id
)
VALUES (
    :v_file_name,
    'SP_RAW_TO_STG_ORDERS',
    :v_current_step,
    :v_insert_count,
    'SUCCESS',
    LAST_QUERY_ID()
);

RETURN
      'latest=' || COALESCE(:v_latest_count,0)
   || ', merged=' || COALESCE(:v_merge_count,0)
   || ', deleted=' || COALESCE(:v_delete_count,0)
   || ', inserted=' || COALESCE(:v_insert_count,0);

-- =====================================================
-- FAIL FAST
-- =====================================================

EXCEPTION
WHEN OTHER THEN

    INSERT INTO ecommerce_db.orchestration.pipeline_audit (
        file_name,
        procedure_name,
        stage_step,
        affected_rows,
        status,
        error_message,
        query_id
    )
    VALUES (
        :v_file_name,
        'SP_RAW_TO_STG_ORDERS',
        :v_current_step,
        NULL,
        'FAILED',
        :SQLERRM,
        LAST_QUERY_ID()
    );

    RAISE;

END;
$$;









