{#
  Transaction node - from QuickBooks Journal Entries.
  Matches extension schema Transaction node in schemas/extensions/roboledger.py.
#}

with entries as (
  select * from {{ ref('stg_qb_journal_entries') }}
)

select
  {{ generate_identifier(qb_transaction_uri("'JournalEntry'", 'id')) }} as identifier,
  {{ qb_transaction_uri("'JournalEntry'", 'id') }} as uri,
  doc_number as transaction_number,
  total_amount as amount,
  private_note as description,
  txn_date as date,
  txn_date as transaction_date,
  doc_number as reference_number,
  'journal_entry' as transaction_type,
  'JournalEntry' as type,
  id as number,
  null as sync_hash,
  'USD' as currency,
  null as plaid_merchant_name,
  null as plaid_category,
  null as plaid_pending,
  current_timestamp::varchar as updated_at
from entries
