{#
  Transaction node - business events from QuickBooks.
  Matches extension schema Transaction node in schemas/extensions/roboledger.py.
#}

with entries as (
  select * from {{ ref('stg_qb_journal_entries') }}
)

select
  {{ generate_identifier(qb_transaction_uri('tx_type', 'tx_number')) }} as identifier,
  {{ qb_transaction_uri('tx_type', 'tx_number') }} as uri,
  doc_number as number,
  total_amount as amount,
  private_note as description,
  txn_date as date,
  doc_number as reference_number,
  tx_type as type,
  'USD' as currency,
  null as merchant_name,
  null as category,
  null as pending,
  current_timestamp::varchar as updated_at
from entries
