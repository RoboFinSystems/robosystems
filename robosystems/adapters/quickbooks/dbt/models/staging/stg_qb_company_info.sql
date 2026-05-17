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
  cast("CompanyAddr_Country" as varchar) as country,
  cast("PrimaryPhone_FreeFormNumber" as varchar) as phone,
  cast("WebAddr_URI" as varchar) as website,
  -- QB stores FiscalYearStartMonth as a month name (January..December).
  -- Map start-month → fiscal-year-end-month (one month earlier, wraps Jan→December).
  case lower(cast("FiscalYearStartMonth" as varchar))
    when 'january'   then 'December'
    when 'february'  then 'January'
    when 'march'     then 'February'
    when 'april'     then 'March'
    when 'may'       then 'April'
    when 'june'      then 'May'
    when 'july'      then 'June'
    when 'august'    then 'July'
    when 'september' then 'August'
    when 'october'   then 'September'
    when 'november'  then 'October'
    when 'december'  then 'November'
    else null
  end as fiscal_year_end
from source
