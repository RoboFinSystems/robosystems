{{
  config(
    materialized='table'
  )
}}

-- Phase 2: Agents mart — UNION of customers / vendors / employees from QB.
-- Loader UPSERTs into the extensions Agent table keyed on
-- (connection_id, source='quickbooks', external_id).

with customers as (
  select * from {{ ref('stg_qb_customers') }}
),
vendors as (
  select * from {{ ref('stg_qb_vendors') }}
),
employees as (
  select * from {{ ref('stg_qb_employees') }}
)

select * from customers
union all
select * from vendors
union all
select * from employees
