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
  nullif(cast("ParentRef" as varchar), '') as parent_ref,
  cast("Active" as boolean) as is_active,
  cast("CurrentBalance" as double) as current_balance,
  cast("AccountSubType" as varchar) as account_sub_type,
  lower(cast("Classification" as varchar)) as domain
from source
