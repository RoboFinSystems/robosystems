{#
  Element node - from QuickBooks Chart of Accounts.
  Matches base schema Element node in schemas/base.py.
  Each QB account becomes an Element with qb: prefix qname.
#}

with accounts as (
  select * from {{ ref('stg_qb_accounts') }}
)

select
  {{ generate_identifier(qb_element_uri('fully_qualified_name')) }} as identifier,
  {{ qb_element_uri('fully_qualified_name') }} as uri,
  concat(domain, ':', {{ qb_stripped_account_name('fully_qualified_name') }}) as qname,
  name,
  {{ period_type('classification') }} as period_type,
  'Monetary' as type,
  {{ normal_balance('classification') }} as balance,
  false as is_abstract,
  false as is_dimension_item,
  false as is_domain_member,
  false as is_hypercube_item,
  false as is_integer,
  true as is_numeric,
  false as is_shares,
  false as is_fraction,
  false as is_textblock,
  null as substitution_group,
  null as item_type,
  lower(classification) as classification,
  null as canonical_concept,
  null as canonical_confidence,
  null as embedding
from accounts
where is_active = true
