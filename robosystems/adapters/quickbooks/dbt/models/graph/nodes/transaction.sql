{#
  Transaction node - from QuickBooks Journal Entries.
  Matches extension schema Transaction node in schemas/extensions/roboledger.py.
#}

with entries as (
  select * from {{ ref('stg_qb_journal_entries') }}
)

select
  {{ generate_identifier(qb_transaction_uri('tx_type', 'tx_number')) }} as identifier,
  {{ qb_transaction_uri('tx_type', 'tx_number') }} as uri,
  doc_number as transaction_number,
  total_amount as amount,
  private_note as description,
  txn_date as date,
  txn_date as transaction_date,
  doc_number as reference_number,
  lower(replace(tx_type, ' ', '_')) as transaction_type,
  tx_type as type,
  tx_number as number,
  null as sync_hash,
  'USD' as currency,
  null as merchant_name,
  null as category,
  null as pending,
  current_timestamp::varchar as updated_at
from entries
