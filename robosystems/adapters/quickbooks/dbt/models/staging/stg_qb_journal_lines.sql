with source as (
  {% if var('use_seeds', false) %}
    select * from {{ ref('raw_journal_lines') }}
  {% else %}
    select * from read_parquet('{{ var("qb_extract_path") }}/raw_journal_lines.parquet')
  {% endif %}
)

select
  journal_entry_id,
  -- Extract tx_type and tx_number from composite id (e.g. 'Invoice_123')
  -- For seed data (plain numeric ids), default to 'JournalEntry'
  case
    when position('_' in journal_entry_id) > 0
    then split_part(journal_entry_id, '_', 1)
    else 'JournalEntry'
  end as tx_type,
  case
    when position('_' in journal_entry_id) > 0
    then split_part(journal_entry_id, '_', 2)
    else journal_entry_id
  end as tx_number,
  cast(line_num as integer) as line_num,
  cast("Amount" as double) as amount,
  "PostingType" as posting_type,
  "AccountRef_value" as account_ref_id,
  "AccountRef_name" as account_ref_name,
  "Description" as description,
  "DetailType" as detail_type,
  nullif(trim("DepartmentRef_value"), '') as department_ref_id,
  nullif(trim("DepartmentRef_name"), '') as department_ref_name,
  nullif(trim("ClassRef_value"), '') as class_ref_id,
  nullif(trim("ClassRef_name"), '') as class_ref_name,
  nullif(trim("LocationRef_value"), '') as location_ref_id,
  nullif(trim("LocationRef_name"), '') as location_ref_name,
  case when "PostingType" = 'Debit' then cast("Amount" as double) else 0.0 end as debit_amount,
  case when "PostingType" = 'Credit' then cast("Amount" as double) else 0.0 end as credit_amount
from source
