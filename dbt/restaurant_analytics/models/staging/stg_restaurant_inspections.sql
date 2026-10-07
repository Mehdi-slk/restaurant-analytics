select
    {{ clean_text('camis') }}                         as restaurant_business_id,
    {{ clean_text('dba') }}                           as restaurant_name,
    case when {{ clean_text('boro') }} in ('0','Missing') then null
         else {{ clean_text('boro') }} end            as borough,
    {{ clean_text('building') }}                      as building_number,
    {{ clean_text('street') }}                        as street_name,
    {{ clean_text('zipcode') }}                       as zip_code,
    case when {{ clean_text('phone') }} ~ '^0+$' then null
         else {{ clean_text('phone') }} end           as phone_number,
    {{ clean_text('cuisine_description') }}           as cuisine,
    case when inspection_date::timestamp::date <= date '1900-01-01' then null
         else inspection_date::timestamp end          as inspection_datetime,
    {{ clean_text('action') }}                        as inspection_action,
    {{ clean_text('violation_code') }}                as violation_code,
    {{ clean_text('violation_description') }}         as violation_description,
    {{ clean_text('critical_flag') }}                 as violation_critical_flag,
    score::numeric                                    as inspection_score,
    {{ clean_text('grade') }}                         as inspection_grade,
    grade_date::timestamp                             as grade_datetime,
    record_date::timestamp                            as record_datetime,
    {{ clean_text('inspection_type') }}               as inspection_type,
    {{ clean_text('community_board') }}               as community_board,
    {{ clean_text('council_district') }}              as council_district,
    {{ clean_text('census_tract') }}                  as census_tract,
    case when {{ clean_text('bin') }} in
         ('1000000','2000000','3000000','4000000','5000000') then null
         else {{ clean_text('bin') }} end             as building_identification_number,
    case
    when {{ clean_text('bbl') }} in ('1','2','3','4','5') then null
    else {{ clean_text('bbl') }}
    end as borough_block_lot,
    {{ clean_text('nta') }}                           as neighborhood_tabulation_area,
    case when inspection_date::timestamp::date > date '1900-01-01'
        then md5(
            camis::text || '|' ||
            inspection_date::timestamp::date::text || '|' ||
            coalesce({{ clean_text('inspection_type') }}, 'Unknown')
        )
    end as inspection_id
from {{ source('restaurant', 'restaurant_inspections') }}