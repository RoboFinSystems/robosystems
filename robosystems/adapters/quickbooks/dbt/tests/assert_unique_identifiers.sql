{#
  Verify that all ledger output tables have unique external_id values
  (within each table). Returns rows if any violations are found.
#}

with account_ids as (
  select external_id, 'accounts' as table_name from {{ ref('elements') }}
),

transaction_ids as (
  select external_id, 'transactions' as table_name from {{ ref('transactions') }}
),

entry_ids as (
  select external_id, 'entries' as table_name from {{ ref('entries') }}
),

all_ids as (
  select * from account_ids
  union all
  select * from transaction_ids
  union all
  select * from entry_ids
),

-- Check for nulls
null_ids as (
  select table_name, external_id, 'null_external_id' as violation
  from all_ids
  where external_id is null
),

-- Check for duplicates within each table
duplicate_ids as (
  select table_name, external_id, 'duplicate_external_id' as violation
  from all_ids
  group by table_name, external_id
  having count(*) > 1
)

select * from null_ids
union all
select * from duplicate_ids
