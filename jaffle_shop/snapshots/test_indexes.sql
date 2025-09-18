{% snapshot indexes_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='order_id',
      strategy='check',
      check_cols=['status'],
      indexes=[{'columns': ['status'], 'type': 'hash'}],
      invalidate_hard_deletes=True,
    )
}}

select * from {{ ref('stg_orders') }}

{% endsnapshot %}
