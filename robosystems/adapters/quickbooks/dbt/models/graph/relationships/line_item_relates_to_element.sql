{#
  LINE_ITEM_RELATES_TO_ELEMENT relationship.
  Maps LineItems to their corresponding Element (account).
  Matches LINE_ITEM_RELATES_TO_ELEMENT in schemas/extensions/roboledger.py.
#}

with lines as (
  select * from {{ ref('stg_qb_journal_lines') }}
),

accounts as (
  select * from {{ ref('stg_qb_accounts') }}
)

select
  {{ generate_identifier(
    qb_line_item_uri("'JournalEntry'", 'l.journal_entry_id', 'l.line_num')
  ) }} as line_item_identifier,
  {{ generate_identifier(
    qb_element_uri('a.fully_qualified_name')
  ) }} as element_identifier,
  null as mapping_context
from lines l
inner join accounts a on l.account_ref_id = a.id
