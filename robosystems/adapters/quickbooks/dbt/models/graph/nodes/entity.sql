{#
  Entity node - from QuickBooks CompanyInfo.
  Matches base schema Entity node in schemas/base.py.
#}

with company as (
  select * from {{ ref('stg_qb_company_info') }}
)

select
  {{ generate_identifier(rs_entity_uri()) }} as identifier,
  {{ rs_entity_uri() }} as uri,
  company_name as name,
  legal_name,
  'corporation' as entity_type,
  null as scheme,
  null as cik,
  null as ticker,
  null as exchange,
  null as industry,
  null as sic,
  null as sic_description,
  null as category,
  null as state_of_incorporation,
  null as fiscal_year_end,
  null as tax_id,
  null as lei,
  null as phone,
  null as website,
  'active' as status,
  true as is_parent,
  null as parent_entity_id,
  current_timestamp::varchar as created_at,
  current_timestamp::varchar as updated_at
from company
