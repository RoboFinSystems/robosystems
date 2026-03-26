{{
  config(
    materialized='table'
  )
}}

with entries as (
  select * from {{ ref('stg_qb_journal_entries') }}
)

select
    id                                          as external_id,
    doc_number                                  as number,
    tx_type                                     as type,
    null                                        as category,
    cast(round(total_amount * 100, 0) as bigint)  as amount,
    'USD'                                       as currency,
    txn_date                                    as date,
    null                                        as due_date,
    null                                        as merchant_name,
    doc_number                                  as reference_number,
    private_note                                as description,
    'quickbooks'                                as source,
    id                                          as source_id,
    'posted'                                    as status,
    '{}'::json                                  as metadata
from entries
