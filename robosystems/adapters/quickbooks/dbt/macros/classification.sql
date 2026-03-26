{% macro period_type(classification_expr) %}
{#
  Map QB Classification to XBRL period_type.
  Mirrors local/qb_transactions.py:551-562.

  Balance sheet items (instant): Asset, Liability, Equity
  Income statement items (duration): Revenue, Expense, Other Income, Other Expense
#}
case lower({{ classification_expr }})
  when 'asset' then 'instant'
  when 'liability' then 'instant'
  when 'equity' then 'instant'
  when 'revenue' then 'duration'
  when 'expense' then 'duration'
  when 'other income' then 'duration'
  when 'other expense' then 'duration'
  else 'instant'
end
{% endmacro %}
