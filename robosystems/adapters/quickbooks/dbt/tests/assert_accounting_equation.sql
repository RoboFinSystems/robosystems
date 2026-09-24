{#
  Accounting equation: Assets = Liabilities + Equity + Net Income, within a
  1-cent tolerance (amounts are cents). Returns rows if violated.
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
