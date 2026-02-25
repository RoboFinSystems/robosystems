{% macro generate_identifier(uri_expr) %}
{#
  Generate a deterministic identifier from a URI string.
  Produces a UUID-formatted string from an MD5 hash.
  Mirrors Python's generate_deterministic_uuid7() behavior.
#}
regexp_replace(
  md5(cast({{ uri_expr }} as varchar)),
  '(.{8})(.{4})(.{4})(.{4})(.{12})',
  '\1-\2-\3-\4-\5'
)
{% endmacro %}
