{#
  ENTITY_HAS_TRANSACTION relationship.
  Links the Entity to all Transactions.
  Matches ENTITY_HAS_TRANSACTION in schemas/extensions/roboledger.py.
#}

with entity as (
  select identifier as entity_identifier from {{ ref('entity') }}
),

transactions as (
  select identifier as transaction_identifier from {{ ref('transaction') }}
)

select
  e.entity_identifier,
  t.transaction_identifier,
  null as transaction_context
from entity e
cross join transactions t
