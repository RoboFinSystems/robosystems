{#
  ENTRY_HAS_LINE_ITEM relationship.
  Links Entries to their LineItems.
  Matches ENTRY_HAS_LINE_ITEM in schemas/extensions/roboledger.py.
#}

with lines as (
  select * from {{ ref('stg_qb_journal_lines') }}
)

select
  {{ generate_identifier(
    qb_entry_uri('tx_type', 'tx_number')
  ) }} as entry_identifier,
  {{ generate_identifier(
    qb_line_item_uri('tx_type', 'tx_number', 'line_num')
  ) }} as line_item_identifier,
  null as line_item_context
from lines
