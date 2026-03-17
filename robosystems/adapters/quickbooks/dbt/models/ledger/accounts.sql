{{
  config(
    materialized='table'
  )
}}

with accounts as (
  select * from {{ ref('stg_qb_accounts') }}
),

-- Resolve parent hierarchy: map QB parent_ref (external ID) to our generated account IDs
parents as (
  select
    id as parent_external_id,
    name as parent_name
  from accounts
)

select
    a.id                                         as external_id,
    'quickbooks'                               as external_source,
    coalesce(a.acct_num, a.fully_qualified_name) as code,
    a.name,
    a.fully_qualified_name                       as description,
    case lower(a.classification)
      when 'other income' then 'revenue'
      when 'other expense' then 'expense'
      else lower(a.classification)
    end                                         as classification,
    a.account_sub_type                            as sub_classification,
    {{ normal_balance('a.classification') }}      as balance_type,
    a.parent_ref                                  as external_parent_id,
    case when a.sub_account then 1 else 0 end     as depth,
    coalesce(a.acct_num, a.fully_qualified_name)  as path,
    'USD'                                       as currency,
    a.is_active,
    false                                       as is_placeholder,
    '{}'::json                                  as metadata
from accounts a
