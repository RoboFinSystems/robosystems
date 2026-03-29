{#
  Accounting equation: Assets = Liabilities + Equity + Net Income

  For balance sheet accounts:
  - Assets have debit normal balance (debits increase, credits decrease)
  - Liabilities have credit normal balance (credits increase, debits decrease)
  - Equity has credit normal balance (credits increase, debits decrease)

  Net balance = debits - credits for debit-normal accounts
  Net balance = credits - debits for credit-normal accounts

  Test: total_assets - total_liabilities - total_equity - net_income should equal 0
  (within rounding tolerance of 1 cent since amounts are in cents)

  Returns rows if the equation is violated.
#}

with line_items as (
  select * from {{ ref('line_items') }}
),

accounts as (
  select * from {{ ref('elements') }}
),

classified_lines as (
  select
    li.debit_amount,
    li.credit_amount,
    a.classification
  from line_items li
  inner join accounts a on li.element_external_id = a.external_id
),

balances as (
  select
    sum(case when classification = 'asset' then debit_amount - credit_amount else 0 end) as total_assets,
    sum(case when classification = 'liability' then credit_amount - debit_amount else 0 end) as total_liabilities,
    sum(case when classification = 'equity' then credit_amount - debit_amount else 0 end) as total_equity,
    sum(case when classification = 'revenue' then credit_amount - debit_amount else 0 end) as total_revenue,
    sum(case when classification = 'expense' then debit_amount - credit_amount else 0 end) as total_expenses
  from classified_lines
),

equation_check as (
  select
    total_assets,
    total_liabilities,
    total_equity,
    (total_revenue - total_expenses) as net_income,
    total_assets - total_liabilities - total_equity
      - (total_revenue - total_expenses) as imbalance
  from balances
)

select *
from equation_check
where abs(imbalance) > 1
