{{
  config(
    materialized='table'
  )
}}

with lines as (
  select * from {{ ref('stg_qb_journal_lines') }}
),

departments as (
  select distinct
    department_ref_id                           as external_id,
    'department'                                as dimension_type,
    department_ref_name                         as name,
    department_ref_id                           as value
  from lines
  where department_ref_id is not null
),

classes as (
  select distinct
    class_ref_id                                as external_id,
    'class'                                     as dimension_type,
    class_ref_name                              as name,
    class_ref_id                                as value
  from lines
  where class_ref_id is not null
),

locations as (
  select distinct
    location_ref_id                             as external_id,
    'location'                                  as dimension_type,
    location_ref_name                           as name,
    location_ref_id                             as value
  from lines
  where location_ref_id is not null
)

select * from departments
union all
select * from classes
union all
select * from locations
