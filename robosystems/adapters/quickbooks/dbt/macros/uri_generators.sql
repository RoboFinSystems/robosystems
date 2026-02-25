{% macro qb_entity_uri() %}
{# Generate QuickBooks entity URI. Mirrors uri_utils.qb_entity_uri() #}
concat('{{ var("qb_base_uri") }}', '/entity/', '{{ var("realm_id") }}')
{% endmacro %}


{% macro rs_entity_uri() %}
{# Generate RoboSystems entity URI. Mirrors uri_utils.rl_entity_uri() #}
concat('{{ var("rs_base_uri") }}', '/api/entity/', '{{ var("realm_id") }}')
{% endmacro %}


{% macro qb_element_uri(account_name_expr) %}
{# Generate QuickBooks element URI. Mirrors uri_utils.qb_element_uri() #}
concat(
  '{{ var("qb_base_uri") }}',
  '/entity/',
  '{{ var("realm_id") }}',
  '/element#',
  {{ qb_stripped_account_name(account_name_expr) }}
)
{% endmacro %}


{% macro qb_transaction_uri(tx_type_expr, tx_id_expr) %}
{# Generate QuickBooks transaction URI. Mirrors uri_utils.qb_transaction_uri() #}
concat(
  '{{ var("qb_base_uri") }}',
  '/entity/',
  '{{ var("realm_id") }}',
  '/transaction/',
  replace(replace({{ tx_type_expr }}, ' ', ''), '(Check)', 'Check'),
  '/',
  {{ tx_id_expr }}
)
{% endmacro %}


{% macro qb_line_item_uri(tx_type_expr, tx_id_expr, line_num_expr) %}
{# Generate QuickBooks line item URI. Mirrors uri_utils.qb_line_item_uri() #}
concat(
  {{ qb_transaction_uri(tx_type_expr, tx_id_expr) }},
  '/line-item/',
  {{ line_num_expr }}
)
{% endmacro %}


{% macro qb_stripped_account_name(name_expr) %}
{#
  Strip and normalize account name for URI generation.
  Mirrors uri_utils.qb_stripped_account_name():
  - Remove everything after first '('
  - Remove spaces
  - Remove special symbols

  Note: Python version uses title() for case normalization.
  QB account names are already properly cased from the API,
  so we skip title-casing in SQL for DuckDB compatibility.
#}
regexp_replace(
  replace(
    trim(split_part({{ name_expr }}, '(', 1)),
    ' ', ''
  ),
  '[&().,;:!?/\\|+=*@#$%^<>~`]',
  '',
  'g'
)
{% endmacro %}
