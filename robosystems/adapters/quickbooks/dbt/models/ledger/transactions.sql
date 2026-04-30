{{
  config(
    materialized='table'
  )
}}

-- Phase 2: enrich JournalReport-derived transactions with class-specific
-- event_type, event_category, and agent_external_id by LEFT JOINing the
-- per-class header staging models on the composite tx id (e.g. Invoice_123).
--
-- JournalReport stays as the GL line-item source (see entries.sql /
-- line_items.sql). The headers exist solely to surface the class +
-- counterparty that JournalReport flattens away.

with entries as (
  select * from {{ ref('stg_qb_journal_entries') }}
),
invoice_headers as (
  select tx_type, tx_id, agent_external_id, agent_type
  from {{ ref('stg_qb_invoice_headers') }}
),
bill_headers as (
  select tx_type, tx_id, agent_external_id, agent_type
  from {{ ref('stg_qb_bill_headers') }}
),
payment_headers as (
  select tx_type, tx_id, agent_external_id, agent_type
  from {{ ref('stg_qb_payment_headers') }}
),
all_headers as (
  select * from invoice_headers
  union all
  select * from bill_headers
  union all
  select * from payment_headers
)

select
    e.id                                               as external_id,
    e.doc_number                                       as number,
    e.tx_type                                          as type,
    null                                               as category,
    cast(round(e.total_amount * 100, 0) as bigint)     as amount,
    'USD'                                              as currency,
    e.txn_date                                         as date,
    null                                               as due_date,
    null                                               as merchant_name,
    e.doc_number                                       as reference_number,
    e.private_note                                     as description,
    'quickbooks'                                       as source,
    e.id                                               as source_id,
    'posted'                                           as status,
    '{}'::json                                         as metadata,
    case
      when e.tx_type = 'Invoice'      then 'invoice_issued'
      when e.tx_type = 'Bill'         then 'bill_received'
      when e.tx_type = 'Payment'      then 'payment_received'
      when e.tx_type = 'JournalEntry' then 'journal_entry_recorded'
      else 'journal_entry_recorded'
    end                                                as event_type,
    case
      when e.tx_type = 'Invoice'      then 'sales'
      when e.tx_type = 'Bill'         then 'purchase'
      when e.tx_type = 'Payment'      then 'sales'
      when e.tx_type = 'JournalEntry' then 'adjustment'
      else 'adjustment'
    end                                                as event_category,
    h.agent_external_id                                as agent_external_id,
    h.agent_type                                       as agent_type
from entries e
left join all_headers h
  on h.tx_type = e.tx_type and h.tx_id = e.tx_number
