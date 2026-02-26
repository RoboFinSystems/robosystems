with source as (
  {% if var('use_seeds', false) %}
    select * from {{ ref('raw_journal_lines') }}
  {% else %}
    select * from read_parquet('{{ var("qb_extract_path") }}/raw_journal_lines.parquet')
  {% endif %}
)

select
  cast(journal_entry_id as varchar) as journal_entry_id,
  -- Extract tx_type and tx_number from composite id (e.g. 'Invoice_123')
  -- For plain numeric ids, default to 'JournalEntry'
  case
    when position('_' in cast(journal_entry_id as varchar)) > 0
    then split_part(cast(journal_entry_id as varchar), '_', 1)
    else 'JournalEntry'
  end as tx_type,
  case
    when position('_' in cast(journal_entry_id as varchar)) > 0
    then split_part(cast(journal_entry_id as varchar), '_', 2)
    else cast(journal_entry_id as varchar)
  end as tx_number,
  cast(line_num as integer) as line_num,
  cast("Amount" as double) as amount,
  cast("PostingType" as varchar) as posting_type,
  cast("AccountRef_value" as varchar) as account_ref_id,
  cast("AccountRef_name" as varchar) as account_ref_name,
  cast("Description" as varchar) as description,
  cast("DetailType" as varchar) as detail_type,
  nullif(trim(cast("DepartmentRef_value" as varchar)), '') as department_ref_id,
  nullif(trim(cast("DepartmentRef_name" as varchar)), '') as department_ref_name,
  nullif(trim(cast("ClassRef_value" as varchar)), '') as class_ref_id,
  nullif(trim(cast("ClassRef_name" as varchar)), '') as class_ref_name,
  nullif(trim(cast("LocationRef_value" as varchar)), '') as location_ref_id,
  nullif(trim(cast("LocationRef_name" as varchar)), '') as location_ref_name,
  case when "PostingType" = 'Debit' then cast("Amount" as double) else 0.0 end as debit_amount,
  case when "PostingType" = 'Credit' then cast("Amount" as double) else 0.0 end as credit_amount
from source
