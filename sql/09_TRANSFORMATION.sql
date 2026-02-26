-- =====================================================
-- 10_curated setup.sql
-- Purpose: Create streams on staged tables and define procedure updating curated tables (dim client, product and fact order, order items)
-- Role: ecommerce_transform,wh: wh_transform
-- =====================================================

USE ROLE ecommerce_transform;
USE WAREHOUSE wh_transform;
USE SCHEMA ecommerce_db.curated;

CREATE TABLE IF NOT EXISTS dim_customer (
customer_id number primary key,
segment text,
loyalty_tier text,
updated_at timestamp_ltz
);

CREATE TABLE IF NOT EXISTS dim_product (
product_id number primary key,
category text,
updated_at timestamp_ltz
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id NUMBER PRIMARY KEY,
    customer_id NUMBER,
    order_date DATE,
    payment_amount NUMBER,
    payment_currency STRING,
    payment_method STRING,
    shipping_city STRING,
    shipping_country STRING,
    shipping_delivery_type STRING,
    status STRING,
    updated_at TIMESTAMP_LTZ
);

CREATE TABLE IF NOT EXISTS fact_order_items (
    order_id NUMBER,
    product_id NUMBER,
    order_date DATE,
    quantity NUMBER,
    unit_price NUMBER,
    extended_amount NUMBER,
    updated_at TIMESTAMP_LTZ
);

USE ROLE ecommerce_transform;
USE SCHEMA ecommerce_db.staged;

CREATE OR REPLACE STREAM stg_orders_stream
ON TABLE ecommerce_db.staged.stg_orders;

CREATE OR REPLACE STREAM stg_order_items_stream
ON TABLE ecommerce_db.staged.stg_order_items;

USE ROLE ecommerce_transform;
USE SCHEMA ecommerce_db.curated;

CREATE OR REPLACE PROCEDURE curated.sp_stg_to_curated()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_dim_customer_rows   NUMBER;
    v_dim_product_rows    NUMBER;
    v_fact_orders_rows    NUMBER;
    v_deleted_items_rows  NUMBER;
    v_inserted_items_rows NUMBER;

    v_file_name    STRING;
    v_current_step STRING;
    v_stream_rows  NUMBER;

BEGIN

-- =====================================================
-- STREAM_READ_ORDERS
-- =====================================================

v_current_step := 'STREAM_READ_ORDERS';

CREATE OR REPLACE TEMP TABLE tmp_stg_orders AS
SELECT *
FROM ecommerce_db.staged.stg_orders_stream;

SELECT COUNT(*) INTO v_stream_rows FROM tmp_stg_orders;
SELECT MAX(file_name) INTO v_file_name FROM tmp_stg_orders;

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
    'SP_STG_TO_CURATED',
    :v_current_step,
    :v_stream_rows,
    'SUCCESS',
    LAST_QUERY_ID()
);

-- =====================================================
-- STREAM_READ_ORDER_ITEMS
-- =====================================================

v_current_step := 'STREAM_READ_ORDER_ITEMS';

CREATE OR REPLACE TEMP TABLE tmp_stg_order_items AS
SELECT *
FROM ecommerce_db.staged.stg_order_items_stream;

-- Capture metadata from temp table
SELECT COUNT(*) INTO v_stream_rows 
FROM tmp_stg_order_items;

SELECT MAX(file_name) INTO v_file_name 
FROM tmp_stg_order_items;

-- Write audit
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
    'SP_STG_TO_CURATED',
    :v_current_step,
    :v_stream_rows,
    'SUCCESS',
    LAST_QUERY_ID()
);

-- =====================================================
-- MERGE_DIM_CUSTOMER
-- =====================================================

v_current_step := 'MERGE_DIM_CUSTOMER';

MERGE INTO ecommerce_db.curated.dim_customer d
USING (
    SELECT DISTINCT
        customer_id,
        loyalty_tier,
        segment
    FROM tmp_stg_orders
) s
ON d.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET
    loyalty_tier = s.loyalty_tier,
    segment = s.segment,
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    customer_id, loyalty_tier, segment, updated_at
)
VALUES (
    s.customer_id, s.loyalty_tier, s.segment, CURRENT_TIMESTAMP()
);

v_dim_customer_rows := SQLROWCOUNT;

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
    'SP_STG_TO_CURATED',
    :v_current_step,
    :v_dim_customer_rows,
    'SUCCESS',
    LAST_QUERY_ID()
);

-- =====================================================
-- MERGE_DIM_PRODUCT
-- =====================================================

v_current_step := 'MERGE_DIM_PRODUCT';

MERGE INTO ecommerce_db.curated.dim_product d
USING (
    SELECT DISTINCT
        product_id,
        category
    FROM tmp_stg_order_items
) s
ON d.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET
    category = s.category,
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    product_id, category, updated_at
)
VALUES (
    s.product_id, s.category, CURRENT_TIMESTAMP()
);

v_dim_product_rows := SQLROWCOUNT;

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
    'SP_STG_TO_CURATED',
    :v_current_step,
    :v_dim_product_rows,
    'SUCCESS',
    LAST_QUERY_ID()
);

-- =====================================================
-- MERGE_FACT_ORDERS
-- =====================================================

v_current_step := 'MERGE_FACT_ORDERS';

MERGE INTO ecommerce_db.curated.fact_orders f
USING tmp_stg_orders s
ON f.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
    customer_id = s.customer_id,
    order_date = s.order_date,
    payment_amount = s.payment_amount,
    payment_currency = s.payment_currency,
    payment_method = s.payment_method,
    shipping_city = s.shipping_city,
    shipping_country = s.shipping_country,
    shipping_delivery_type = s.shipping_delivery_type,
    status = s.status,
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    order_id, customer_id, order_date,
    payment_amount, payment_currency, payment_method,
    shipping_city, shipping_country, shipping_delivery_type,
    status, updated_at
)
VALUES (
    s.order_id, s.customer_id, s.order_date,
    s.payment_amount, s.payment_currency, s.payment_method,
    s.shipping_city, s.shipping_country, s.shipping_delivery_type,
    s.status, CURRENT_TIMESTAMP()
);

v_fact_orders_rows := SQLROWCOUNT;

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
    'SP_STG_TO_CURATED',
    :v_current_step,
    :v_fact_orders_rows,
    'SUCCESS',
    LAST_QUERY_ID()
);

-- =====================================================
-- DELETE_FACT_ORDER_ITEMS  (Snapshot semantics)
-- =====================================================

v_current_step := 'DELETE_FACT_ORDER_ITEMS';

DELETE FROM ecommerce_db.curated.fact_order_items
WHERE order_id IN (
    SELECT DISTINCT order_id FROM tmp_stg_order_items
);

v_deleted_items_rows := SQLROWCOUNT;

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
    'SP_STG_TO_CURATED',
    :v_current_step,
    :v_deleted_items_rows,
    'SUCCESS',
    LAST_QUERY_ID()
);

-- =====================================================
-- INSERT_FACT_ORDER_ITEMS  (Snapshot semantics)
-- =====================================================

v_current_step := 'INSERT_FACT_ORDER_ITEMS';

INSERT INTO ecommerce_db.curated.fact_order_items (
    order_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    extended_amount,
    updated_at
)
SELECT
    order_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    quantity * unit_price,
    CURRENT_TIMESTAMP()
FROM tmp_stg_order_items;

v_inserted_items_rows := SQLROWCOUNT;

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
    'SP_STG_TO_CURATED',
    :v_current_step,
    :v_inserted_items_rows,
    'SUCCESS',
    LAST_QUERY_ID()
);

RETURN
      'dim_customer=' || COALESCE(:v_dim_customer_rows,0)
   || ', dim_product=' || COALESCE(:v_dim_product_rows,0)
   || ', fact_orders=' || COALESCE(:v_fact_orders_rows,0)
   || ', items_deleted=' || COALESCE(:v_deleted_items_rows,0)
   || ', items_inserted=' || COALESCE(:v_inserted_items_rows,0);

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
        'SP_STG_TO_CURATED',
        :v_current_step,
        NULL,
        'FAILED',
        :SQLERRM,
        LAST_QUERY_ID()
    );

    RAISE;

END;
$$;




