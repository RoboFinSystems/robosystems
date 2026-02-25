{#
  LINE_ITEM_HAS_DIMENSION relationship.
  Links LineItems to their dimensional qualifiers (department, class, location).
  Matches LINE_ITEM_HAS_DIMENSION in schemas/extensions/roboledger.py.
  Only creates rows where the line item actually has a dimension.
#}

with lines as (
  select * from {{ ref('stg_qb_journal_lines') }}
),

department_dims as (
  select
    tx_type,
    tx_number,
    line_num,
    department_ref_id as ref_id,
    'department' as dimension_type
  from lines
  where department_ref_id is not null
),

class_dims as (
  select
    tx_type,
    tx_number,
    line_num,
    class_ref_id as ref_id,
    'class' as dimension_type
  from lines
  where class_ref_id is not null
),

location_dims as (
  select
    tx_type,
    tx_number,
    line_num,
    location_ref_id as ref_id,
    'location' as dimension_type
  from lines
  where location_ref_id is not null
),

all_line_dims as (
  select * from department_dims
  union all
  select * from class_dims
  union all
  select * from location_dims
)

select
  {{ generate_identifier(
    qb_line_item_uri('tx_type', 'tx_number', 'line_num')
  ) }} as line_item_identifier,
  {{ generate_identifier("concat(dimension_type, ':', ref_id)") }} as dimension_identifier
from all_line_dims
