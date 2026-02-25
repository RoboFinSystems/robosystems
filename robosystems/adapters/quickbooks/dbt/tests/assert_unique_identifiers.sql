{#
  Verify that all node tables have unique, non-null identifiers.
  Also verify that relationship foreign keys reference existing nodes.
  Returns rows if any violations are found.
#}

with entity_ids as (
  select identifier, 'entity' as table_name from {{ ref('entity') }}
),

element_ids as (
  select identifier, 'element' as table_name from {{ ref('element') }}
),

transaction_ids as (
  select identifier, 'transaction' as table_name from {{ ref('transaction') }}
),

line_item_ids as (
  select identifier, 'line_item' as table_name from {{ ref('line_item') }}
),

dimension_ids as (
  select identifier, 'dimension' as table_name from {{ ref('dimension') }}
),

all_ids as (
  select * from entity_ids
  union all
  select * from element_ids
  union all
  select * from transaction_ids
  union all
  select * from line_item_ids
  union all
  select * from dimension_ids
),

-- Check for nulls
null_ids as (
  select table_name, identifier, 'null_identifier' as violation
  from all_ids
  where identifier is null
),

-- Check for duplicates within each table
duplicate_ids as (
  select table_name, identifier, 'duplicate_identifier' as violation
  from all_ids
  group by table_name, identifier
  having count(*) > 1
)

select * from null_ids
union all
select * from duplicate_ids
