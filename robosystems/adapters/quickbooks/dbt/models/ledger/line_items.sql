{{
  config(
    materialized='table'
  )
}}

with lines as (
  select * from {{ ref('stg_qb_journal_lines') }}
)

-- A negative amount (a processor fee netted on a deposit) belongs on the
-- other side; dropped, it would unbalance its entry.
, sided as (
  select
      *,
      greatest(debit_amount, 0) + greatest(-credit_amount, 0)  as debit_side,
      greatest(credit_amount, 0) + greatest(-debit_amount, 0)  as credit_side
  from lines
)

select
    journal_entry_id                            as entry_external_id,
    account_ref_id                              as element_external_id,
    cast(round(debit_side * 100, 0) as bigint)   as debit_amount,
    cast(round(credit_side * 100, 0) as bigint)  as credit_amount,
    description,
    line_num                                    as line_order,
    '{}'::json                                  as metadata
from sided
where debit_side > 0 or credit_side > 0
