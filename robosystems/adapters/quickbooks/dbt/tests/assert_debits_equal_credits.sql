{#
  Accounting invariant: Total debits must equal total credits.
  Returns rows if the invariant is violated (i.e., test fails if any rows returned).
#}

with line_items as (
  select * from {{ ref('line_item') }}
),

totals as (
  select
    sum(debit_amount) as total_debits,
    sum(credit_amount) as total_credits
  from line_items
)

select *
from totals
where abs(total_debits - total_credits) > 0.01
