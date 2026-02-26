with source as (
  {% if var('use_seeds', false) %}
    select * from {{ ref('raw_company_info') }}
  {% else %}
    select * from read_parquet('{{ var("qb_extract_path") }}/raw_company_info.parquet')
  {% endif %}
)

select
  cast("Id" as varchar) as id,
  cast("CompanyName" as varchar) as company_name,
  cast("LegalName" as varchar) as legal_name,
  cast("CompanyAddr_Line1" as varchar) as address_line1,
  cast("CompanyAddr_City" as varchar) as city,
  cast("CompanyAddr_CountrySubDivisionCode" as varchar) as state,
  cast("CompanyAddr_PostalCode" as varchar) as postal_code,
  cast("CompanyAddr_Country" as varchar) as country
from source
