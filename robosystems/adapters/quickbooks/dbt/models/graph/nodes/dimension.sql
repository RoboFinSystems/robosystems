{#
  Dimension node - from QuickBooks Department/Class/Location refs on journal lines.
  Matches base schema Dimension node in schemas/base.py.
  Only creates rows where dimensional qualifiers are present.
#}

with lines as (
  select * from {{ ref('stg_qb_journal_lines') }}
),

departments as (
  select distinct
    department_ref_id as ref_id,
    department_ref_name as ref_name,
    'Department' as axis,
    'department' as dimension_type
  from lines
  where department_ref_id is not null
),

classes as (
  select distinct
    class_ref_id as ref_id,
    class_ref_name as ref_name,
    'Class' as axis,
    'class' as dimension_type
  from lines
  where class_ref_id is not null
),

locations as (
  select distinct
    location_ref_id as ref_id,
    location_ref_name as ref_name,
    'Location' as axis,
    'location' as dimension_type
  from lines
  where location_ref_id is not null
),

all_dimensions as (
  select * from departments
  union all
  select * from classes
  union all
  select * from locations
)

select
  {{ generate_identifier("concat(dimension_type, ':', ref_id)") }} as identifier,
  axis,
  ref_name as member,
  dimension_type,
  null as axis_uri,
  null as member_uri,
  null as type,
  null as is_explicit,
  null as is_typed
from all_dimensions
