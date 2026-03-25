{{
  config(
    materialized='table'
  )
}}

with lines as (
  select * from {{ ref('stg_qb_journal_lines') }}
)

select
    journal_entry_id                            as entry_external_id,
    account_ref_id                              as account_external_id,
    cast(round(debit_amount * 100, 0) as bigint)   as debit_amount,
    cast(round(credit_amount * 100, 0) as bigint)  as credit_amount,
    description,
    line_num                                    as line_order,
    '{}'::json                                  as metadata
from lines
where debit_amount > 0 or credit_amount > 0
