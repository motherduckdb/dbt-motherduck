{{ config(materialized='view') }}

select * from {{ ref('input') }}
