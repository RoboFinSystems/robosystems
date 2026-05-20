with source as (
  {% if var('use_seeds', false) %}
    select * from {{ ref('raw_sales_receipt_headers') }}
  {% else %}
    select * from read_parquet('{{ var("qb_extract_path") }}/raw_sales_receipt_headers.parquet')
  {% endif %}
)

select
  cast("tx_type" as varchar) as tx_type,
  cast("tx_id" as varchar) as tx_id,
  cast("external_id" as varchar) as external_id,
  cast("txn_date" as varchar) as txn_date,
  cast("doc_number" as varchar) as doc_number,
  cast("total_amount" as double) as total_amount,
  cast("currency" as varchar) as currency,
  cast("memo" as varchar) as memo,
  cast("agent_external_id" as varchar) as agent_external_id,
  cast("agent_type" as varchar) as agent_type,
  cast("linked_txns" as varchar) as linked_txns,
  cast("sync_token" as varchar) as sync_token
from source
