{{
  config(
    materialized='table'
  )
}}

with accounts as (
  select * from {{ ref('stg_qb_accounts') }}
)

select
    id                                         as external_id,
    'quickbooks'                               as external_source,
    fully_qualified_name                        as code,
    name,
    fully_qualified_name                        as description,
    case lower(classification)
      when 'other income' then 'revenue'
      when 'other expense' then 'expense'
      else lower(classification)
    end                                         as classification,
    account_sub_type                            as sub_classification,
    {{ normal_balance('classification') }}      as balance_type,
    parent_ref                                  as external_parent_id,
    0                                           as depth,
    ''                                          as path,
    'USD'                                       as currency,
    is_active,
    false                                       as is_placeholder,
    '{}'::json                                  as metadata
from accounts
