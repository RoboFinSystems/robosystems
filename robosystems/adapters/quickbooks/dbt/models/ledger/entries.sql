{{
  config(
    materialized='table'
  )
}}

with journal_entries as (
  select * from {{ ref('stg_qb_journal_entries') }}
)

select
    id                                          as external_id,
    id                                          as external_transaction_id,
    doc_number                                  as number,
    'standard'                                  as type,
    txn_date                                    as posting_date,
    private_note                                as memo,
    'posted'                                    as status,
    '{}'::json                                  as metadata
from journal_entries
