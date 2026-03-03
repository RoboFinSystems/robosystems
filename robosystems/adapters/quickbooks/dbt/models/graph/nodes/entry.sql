{#
  Entry node - ledger entries from QuickBooks.
  For QB data this is 1:1 with Transaction (QB transactions are pre-journalized).
  Matches extension schema Entry node in schemas/extensions/roboledger.py.
#}

with entries as (
  select * from {{ ref('stg_qb_journal_entries') }}
)

select
  {{ generate_identifier(qb_entry_uri('tx_type', 'tx_number')) }} as identifier,
  {{ qb_entry_uri('tx_type', 'tx_number') }} as uri,
  doc_number as number,
  private_note as memo,
  txn_date as posting_date,
  'standard' as type,
  'posted' as status,
  null as reversal_of,
  current_timestamp::varchar as updated_at
from entries
