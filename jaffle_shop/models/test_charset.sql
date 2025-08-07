{{
    config(
        charset='utf8mb4',
        collation='utf8mb4_general_ci'
    )
}}

select * from {{ ref('raw_orders') }}
