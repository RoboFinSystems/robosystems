{% macro normal_balance(classification_expr) %}
{#
  Determine the normal balance (debit or credit) from QB Classification.
  Mirrors the logic from local/qb_transactions.py:564-574.

  Debit normal: Asset, Expense, Other Expense
  Credit normal: Liability, Equity, Revenue, Other Income
#}
case lower({{ classification_expr }})
  when 'asset' then 'debit'
  when 'expense' then 'debit'
  when 'other expense' then 'debit'
  when 'liability' then 'credit'
  when 'equity' then 'credit'
  when 'revenue' then 'credit'
  when 'other income' then 'credit'
  else 'debit'
end
{% endmacro %}
