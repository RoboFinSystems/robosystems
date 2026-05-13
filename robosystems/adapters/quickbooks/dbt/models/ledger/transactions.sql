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
bill_payment_headers as (
  select tx_type, tx_id, agent_external_id, agent_type
  from {{ ref('stg_qb_bill_payment_headers') }}
),
sales_receipt_headers as (
  select tx_type, tx_id, agent_external_id, agent_type
  from {{ ref('stg_qb_sales_receipt_headers') }}
),
purchase_headers as (
  -- Purchase covers QB's Expense / Cash Expense / Check / Credit Card Expense.
  -- The Python flattener emits multiple header rows per Purchase (one per
  -- candidate tx_type JournalReport might use for that PaymentType), so the
  -- LEFT JOIN below resolves to whichever flavor matched.
  select tx_type, tx_id, agent_external_id, agent_type
  from {{ ref('stg_qb_purchase_headers') }}
),
all_headers as (
  select * from invoice_headers
  union all
  select * from bill_headers
  union all
  select * from payment_headers
  union all
  select * from bill_payment_headers
  union all
  select * from sales_receipt_headers
  union all
  select * from purchase_headers
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
    -- QB JournalReport surfaces tx_type as a display label (with spaces +
    -- parentheses), not the canonical QBO API entity name. Match the
    -- display forms verbatim because that's what flows through the
    -- composite Id parse in stg_qb_journal_entries. Each branch routes
    -- to the closest matching entry in the open event_type vocabulary
    -- (see event-driven-ledger.md §3) so downstream consumers can branch
    -- on event_type without sniffing metadata.qb_txn_type.
    case
      when e.tx_type = 'Invoice'                    then 'invoice_issued'
      when e.tx_type = 'Bill'                       then 'bill_received'
      when e.tx_type = 'Payment'                    then 'payment_received'
      when e.tx_type = 'BillPayment'                then 'bill_paid'
      when e.tx_type = 'Bill Payment'               then 'bill_paid'
      when e.tx_type = 'Bill Payment (Credit Card)' then 'bill_paid'
      when e.tx_type = 'Bill Payment (Check)'       then 'bill_paid'
      when e.tx_type = 'SalesReceipt'               then 'sales_receipt_recorded'
      when e.tx_type = 'Sales Receipt'              then 'sales_receipt_recorded'
      when e.tx_type = 'JournalEntry'               then 'journal_entry_recorded'
      when e.tx_type = 'Journal Entry'              then 'journal_entry_recorded'
      when e.tx_type = 'Expense'                    then 'cash_expense_recorded'
      when e.tx_type = 'Cash Expense'               then 'cash_expense_recorded'
      when e.tx_type = 'Check'                      then 'check_written'
      when e.tx_type = 'Credit Card Expense'        then 'credit_card_charge'
      when e.tx_type = 'Credit Card Credit'         then 'credit_card_refund'
      when e.tx_type = 'Deposit'                    then 'deposit_received'
      when e.tx_type = 'Inventory Qty Adjust'       then 'inventory_adjusted'
      when e.tx_type = 'Inventory Adjustment'       then 'inventory_adjusted'
      else 'journal_entry_recorded'
    end                                                as event_type,
    case
      when e.tx_type = 'Invoice'                    then 'sales'
      when e.tx_type = 'Bill'                       then 'purchase'
      when e.tx_type = 'Payment'                    then 'sales'
      when e.tx_type = 'BillPayment'                then 'purchase'
      when e.tx_type = 'Bill Payment'               then 'purchase'
      when e.tx_type = 'Bill Payment (Credit Card)' then 'purchase'
      when e.tx_type = 'Bill Payment (Check)'       then 'purchase'
      when e.tx_type = 'SalesReceipt'               then 'sales'
      when e.tx_type = 'Sales Receipt'              then 'sales'
      when e.tx_type = 'JournalEntry'               then 'adjustment'
      when e.tx_type = 'Journal Entry'              then 'adjustment'
      when e.tx_type = 'Expense'                    then 'purchase'
      when e.tx_type = 'Cash Expense'               then 'purchase'
      when e.tx_type = 'Check'                      then 'purchase'
      when e.tx_type = 'Credit Card Expense'        then 'purchase'
      when e.tx_type = 'Credit Card Credit'         then 'purchase'
      when e.tx_type = 'Deposit'                    then 'treasury'
      when e.tx_type = 'Inventory Qty Adjust'       then 'adjustment'
      when e.tx_type = 'Inventory Adjustment'       then 'adjustment'
      else 'adjustment'
    end                                                as event_category,
    h.agent_external_id                                as agent_external_id,
    h.agent_type                                       as agent_type
from entries e
left join all_headers h
  on h.tx_type = e.tx_type and h.tx_id = e.tx_number
