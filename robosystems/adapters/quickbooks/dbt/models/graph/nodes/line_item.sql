{#
  LineItem node - from QuickBooks Journal Entry Lines.
  Matches extension schema LineItem node in schemas/extensions/roboledger.py.
#}

with lines as (
  select * from {{ ref('stg_qb_journal_lines') }}
)

select
  {{ generate_identifier(
    qb_line_item_uri('tx_type', 'tx_number', 'line_num')
  ) }} as identifier,
  {{ qb_line_item_uri('tx_type', 'tx_number', 'line_num') }} as uri,
  description,
  debit_amount,
  credit_amount,
  (department_ref_id is not null or class_ref_id is not null or location_ref_id is not null) as has_dimensions,
  (case when department_ref_id is not null then 1 else 0 end
   + case when class_ref_id is not null then 1 else 0 end
   + case when location_ref_id is not null then 1 else 0 end
  )::bigint as dimension_count,
  current_timestamp::varchar as updated_at
from lines
