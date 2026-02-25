with source as (
  {% if var('use_seeds', false) %}
    select * from {{ ref('raw_company_info') }}
  {% else %}
    select * from read_parquet('{{ var("qb_extract_path") }}/raw_company_info.parquet')
  {% endif %}
)

select
  cast("Id" as varchar) as id,
  "CompanyName" as company_name,
  "LegalName" as legal_name,
  "CompanyAddr_Line1" as address_line1,
  "CompanyAddr_City" as city,
  "CompanyAddr_CountrySubDivisionCode" as state,
  "CompanyAddr_PostalCode" as postal_code,
  "CompanyAddr_Country" as country
from source
