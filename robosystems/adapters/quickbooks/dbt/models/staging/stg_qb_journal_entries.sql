with source as (
  {% if var('use_seeds', false) %}
    select * from {{ ref('raw_journal_entries') }}
  {% else %}
    select * from read_parquet('{{ var("qb_extract_path") }}/raw_journal_entries.parquet')
  {% endif %}
)

select
  cast("Id" as varchar) as id,
  -- Extract tx_type and tx_number from composite id (e.g. 'Invoice_123')
  -- For plain numeric ids, default to 'JournalEntry'
  case
    when position('_' in cast("Id" as varchar)) > 0
    then split_part(cast("Id" as varchar), '_', 1)
    else 'JournalEntry'
  end as tx_type,
  case
    when position('_' in cast("Id" as varchar)) > 0
    then split_part(cast("Id" as varchar), '_', 2)
    else cast("Id" as varchar)
  end as tx_number,
  cast("TxnDate" as date) as txn_date,
  cast("DocNumber" as varchar) as doc_number,
  cast("TotalAmt" as double) as total_amount,
  cast("PrivateNote" as varchar) as private_note,
  cast("Adjustment" as boolean) as is_adjustment
from source
