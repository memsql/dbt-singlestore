{{ config(materialized='table', storage_type='rowstore') }}

select * from {{ ref('raw_orders') }}
