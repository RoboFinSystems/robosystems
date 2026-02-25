{#
  TRANSACTION_HAS_LINE_ITEM relationship.
  Links Transactions to their LineItems.
  Matches TRANSACTION_HAS_LINE_ITEM in schemas/extensions/roboledger.py.
#}

with lines as (
  select * from {{ ref('stg_qb_journal_lines') }}
)

select
  {{ generate_identifier(
    qb_transaction_uri("'JournalEntry'", 'journal_entry_id')
  ) }} as transaction_identifier,
  {{ generate_identifier(
    qb_line_item_uri("'JournalEntry'", 'journal_entry_id', 'line_num')
  ) }} as line_item_identifier,
  null as line_item_context
from lines
