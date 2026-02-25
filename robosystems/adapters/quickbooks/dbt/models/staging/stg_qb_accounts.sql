with source as (
  {% if var('use_seeds', false) %}
    select * from {{ ref('raw_accounts') }}
  {% else %}
    select * from read_parquet('{{ var("qb_extract_path") }}/raw_accounts.parquet')
  {% endif %}
)

select
  cast("Id" as varchar) as id,
  "Name" as name,
  "FullyQualifiedName" as fully_qualified_name,
  "AccountType" as account_type,
  case
    when "AccountType" in ('Other Income') then 'Other Income'
    when "AccountType" in ('Other Expense') then 'Other Expense'
    else "Classification"
  end as classification,
  cast(nullif("ParentRef", '') as varchar) as parent_ref,
  cast("Active" as boolean) as is_active,
  cast("CurrentBalance" as double) as current_balance,
  "AccountSubType" as account_sub_type,
  lower("Classification") as domain
from source
