{{
    config(
        primary_key=['id', 'user_id'],
        shard_key=['id'],
        storage_type='rowstore'
    )
}}

select * from {{ ref('raw_orders') }}
