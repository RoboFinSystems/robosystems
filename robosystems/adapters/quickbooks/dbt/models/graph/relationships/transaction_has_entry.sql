{#
  TRANSACTION_HAS_ENTRY relationship.
  Links Transactions to their ledger Entries.
  For QB data this is 1:1 (each transaction produces one entry).
  Matches TRANSACTION_HAS_ENTRY in schemas/extensions/roboledger.py.
#}

with entries as (
  select * from {{ ref('stg_qb_journal_entries') }}
)

select
  {{ generate_identifier(
    qb_transaction_uri('tx_type', 'tx_number')
  ) }} as transaction_identifier,
  {{ generate_identifier(
    qb_entry_uri('tx_type', 'tx_number')
  ) }} as entry_identifier,
  null as entry_context
from entries
