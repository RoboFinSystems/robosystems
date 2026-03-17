with source as (
  {% if var('use_seeds', false) %}
    select * from {{ ref('raw_accounts') }}
  {% else %}
    select * from read_parquet('{{ var("qb_extract_path") }}/raw_accounts.parquet')
  {% endif %}
)

select
  cast("Id" as varchar) as id,
  cast("Name" as varchar) as name,
  cast("FullyQualifiedName" as varchar) as fully_qualified_name,
  cast("AccountType" as varchar) as account_type,
  case
    when cast("AccountType" as varchar) in ('Other Income') then 'Other Income'
    when cast("AccountType" as varchar) in ('Other Expense') then 'Other Expense'
    else cast("Classification" as varchar)
  end as classification,
  {% if var('use_seeds', false) %}
    nullif(cast("ParentRef" as varchar), '') as parent_ref,
  {% else %}
    nullif(cast("ParentRef"->>'value' as varchar), '') as parent_ref,
  {% endif %}
  cast("Active" as boolean) as is_active,
  cast("CurrentBalance" as double) as current_balance,
  cast("AccountSubType" as varchar) as account_sub_type,
  lower(cast("Classification" as varchar)) as domain,
  nullif(cast("AcctNum" as varchar), '') as acct_num,
  coalesce(cast("SubAccount" as boolean), false) as sub_account
from source
