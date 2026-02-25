{#
  Accounting equation: Assets = Liabilities + Equity
  (Net of debits and credits by classification)

  For balance sheet accounts:
  - Assets have debit normal balance (debits increase, credits decrease)
  - Liabilities have credit normal balance (credits increase, debits decrease)
  - Equity has credit normal balance (credits increase, debits decrease)

  Net balance = debits - credits for debit-normal accounts
  Net balance = credits - debits for credit-normal accounts

  Test: total_assets - total_liabilities - total_equity should equal 0
  (within rounding tolerance)

  Returns rows if the equation is violated.
#}

with line_items as (
  select * from {{ ref('line_item') }}
),

line_elements as (
  select * from {{ ref('line_item_relates_to_element') }}
),

elements as (
  select * from {{ ref('element') }}
),

classified_lines as (
  select
    li.debit_amount,
    li.credit_amount,
    e.classification
  from line_items li
  inner join line_elements le on li.identifier = le.line_item_identifier
  inner join elements e on le.element_identifier = e.identifier
),

balances as (
  select
    sum(case when classification = 'asset' then debit_amount - credit_amount else 0 end) as total_assets,
    sum(case when classification = 'liability' then credit_amount - debit_amount else 0 end) as total_liabilities,
    sum(case when classification = 'equity' then credit_amount - debit_amount else 0 end) as total_equity,
    sum(case when classification = 'revenue' then credit_amount - debit_amount else 0 end) as total_revenue,
    sum(case when classification = 'expense' then debit_amount - credit_amount else 0 end) as total_expenses,
    sum(case when classification = 'other income' then credit_amount - debit_amount else 0 end) as total_other_income,
    sum(case when classification = 'other expense' then debit_amount - credit_amount else 0 end) as total_other_expenses
  from classified_lines
),

equation_check as (
  select
    total_assets,
    total_liabilities,
    total_equity,
    (total_revenue - total_expenses + total_other_income - total_other_expenses) as net_income,
    total_assets - total_liabilities - total_equity
      - (total_revenue - total_expenses + total_other_income - total_other_expenses) as imbalance
  from balances
)

select *
from equation_check
where abs(imbalance) > 0.01
