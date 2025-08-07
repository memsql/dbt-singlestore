{{
    config(
        materialized='table',
        unique_table_key=['id'],
        sort_key=['status']
    )
}}

select * from {{ ref('raw_orders') }}
