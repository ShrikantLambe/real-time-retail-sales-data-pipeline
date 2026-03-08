{{
    config(
        materialized='incremental',
        unique_key='TRANSACTION_ID',
        incremental_strategy='merge',
        tags=['fact', 'sales'],
        snowflake_warehouse='RETAIL_WH'
    )
}}

-- fact_sales incremental model
-- Grain: transaction_id
-- Source: STREAMING.RAW_RETAIL_SALES
--
-- Incremental strategy: MERGE on TRANSACTION_ID (unique_key).
-- The WHERE clause pushes only new rows from the source to dbt's staging
-- step, keeping the scan small. dbt's MERGE then handles any late-arriving
-- duplicates via the unique_key — this replaces the NOT IN (SELECT ...)
-- anti-pattern, which degrades to O(n²) at scale in Snowflake.
--
-- Note: partition_by is a BigQuery-specific dbt config; it is not valid for
-- Snowflake. Physical clustering is defined in snowflake/setup.sql.

with source as (
    select
        TRANSACTION_ID,
        STORE_ID,
        PRODUCT_ID,
        QUANTITY,
        PRICE,
        PAYMENT_TYPE,
        TRANSACTION_TS,
        QUANTITY * PRICE as REVENUE
    from {{ source('streaming', 'RAW_RETAIL_SALES') }}
    {% if is_incremental() %}
    where TRANSACTION_TS > (select max(TRANSACTION_TS) from {{ this }})
    {% endif %}
)

select * from source