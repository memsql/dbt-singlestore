{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

select * from {{ ref('raw_orders') }}
