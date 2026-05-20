with source as (
  {% if var('use_seeds', false) %}
    select * from {{ ref('raw_vendors') }}
  {% else %}
    select * from read_parquet('{{ var("qb_extract_path") }}/raw_vendors.parquet')
  {% endif %}
)

select
  cast("Id" as varchar) as id,
  cast("agent_type" as varchar) as agent_type,
  cast("name" as varchar) as name,
  cast("legal_name" as varchar) as legal_name,
  cast("email" as varchar) as email,
  cast("phone" as varchar) as phone,
  "address" as address,
  cast("tax_id" as varchar) as tax_id,
  cast("is_1099_recipient" as boolean) as is_1099_recipient,
  cast("is_active" as boolean) as is_active,
  cast("sync_token" as varchar) as sync_token
from source
