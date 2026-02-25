with source as (
  {% if var('use_seeds', false) %}
    select * from {{ ref('raw_journal_entries') }}
  {% else %}
    select * from read_parquet('{{ var("qb_extract_path") }}/raw_journal_entries.parquet')
  {% endif %}
)

select
  cast("Id" as varchar) as id,
  cast("TxnDate" as date) as txn_date,
  "DocNumber" as doc_number,
  cast("TotalAmt" as double) as total_amount,
  "PrivateNote" as private_note,
  cast("Adjustment" as boolean) as is_adjustment
from source
