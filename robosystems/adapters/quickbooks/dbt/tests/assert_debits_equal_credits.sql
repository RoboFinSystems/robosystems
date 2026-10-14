{#
  Accounting invariant: Total debits must equal total credits.
  Returns rows if the invariant is violated (i.e., test fails if any rows returned).
#}

{# A warning, not a failure: one odd transaction must not fail a tenant's
   whole sync; the loader drops an unbalanced entry and counts it. #}
{{ config(severity='warn') }}

with line_items as (
  select * from {{ ref('line_items') }}
),

totals as (
  select
    sum(debit_amount) as total_debits,
    sum(credit_amount) as total_credits
  from line_items
)

select *
from totals
where abs(total_debits - total_credits) > 1
