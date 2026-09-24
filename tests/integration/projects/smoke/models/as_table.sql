{{ config(materialized='table') }}

select * from {{ ref('as_view') }}
