{{ config(materialized='incremental', incremental_strategy='append') }}

select * from {{ ref('input') }}
where id <= {{ var('max_id', 2) }}
{% if is_incremental() %}
  and id > (select coalesce(max(id), 0) from {{ this }})
{% endif %}
