with base as (
    select *
    from {{ ref('stg_restaurant_inspections') }}
),

inspection_recovery as (
    select
        inspection_id,

        case when count(distinct restaurant_name) filter (where restaurant_name is not null) = 1
             then min(restaurant_name) end as restaurant_name,

        case when count(distinct phone_number) filter (where phone_number is not null) = 1
             then min(phone_number) end as phone_number,

        case when count(distinct cuisine) filter (where cuisine is not null) = 1
             then min(cuisine) end as cuisine,

        case when count(distinct borough) filter (where borough is not null) = 1
             then min(borough) end as borough,

        case when count(distinct building_number) filter (where building_number is not null) = 1
             then min(building_number) end as building_number,

        case when count(distinct street_name) filter (where street_name is not null) = 1
             then min(street_name) end as street_name,

        case when count(distinct zip_code) filter (where zip_code is not null) = 1
             then min(zip_code) end as zip_code,

        case when count(distinct community_board) filter (where community_board is not null) = 1
             then min(community_board) end as community_board,

        case when count(distinct council_district) filter (where council_district is not null) = 1
             then min(council_district) end as council_district,

        case when count(distinct census_tract) filter (where census_tract is not null) = 1
             then min(census_tract) end as census_tract,

        case when count(distinct building_identification_number)
                  filter (where building_identification_number is not null) = 1
             then min(building_identification_number) end as building_identification_number,

        case when count(distinct borough_block_lot)
                  filter (where borough_block_lot is not null) = 1
             then min(borough_block_lot) end as borough_block_lot,

        case when count(distinct neighborhood_tabulation_area)
                  filter (where neighborhood_tabulation_area is not null) = 1
             then min(neighborhood_tabulation_area) end as neighborhood_tabulation_area,

        case when count(distinct inspection_action) filter (where inspection_action is not null) = 1
             then min(inspection_action) end as inspection_action,

        case when count(distinct inspection_score) filter (where inspection_score is not null) = 1
             then min(inspection_score) end as inspection_score,

        case when count(distinct inspection_grade) filter (where inspection_grade is not null) = 1
             then min(inspection_grade) end as inspection_grade,

        case when count(distinct grade_datetime) filter (where grade_datetime is not null) = 1
             then min(grade_datetime) end as grade_datetime

    from base
    where inspection_id is not null
    group by inspection_id
),

restaurant_recovery as (
    select
        restaurant_business_id,

        case when count(distinct restaurant_name) filter (where restaurant_name is not null) = 1
             then min(restaurant_name) end as restaurant_name,

        case when count(distinct phone_number) filter (where phone_number is not null) = 1
             then min(phone_number) end as phone_number,

        case when count(distinct cuisine) filter (where cuisine is not null) = 1
             then min(cuisine) end as cuisine,

        case when count(distinct borough) filter (where borough is not null) = 1
             then min(borough) end as borough,

        case when count(distinct building_number) filter (where building_number is not null) = 1
             then min(building_number) end as building_number,

        case when count(distinct street_name) filter (where street_name is not null) = 1
             then min(street_name) end as street_name,

        case when count(distinct zip_code) filter (where zip_code is not null) = 1
             then min(zip_code) end as zip_code,

        case when count(distinct community_board) filter (where community_board is not null) = 1
             then min(community_board) end as community_board,

        case when count(distinct council_district) filter (where council_district is not null) = 1
             then min(council_district) end as council_district,

        case when count(distinct census_tract) filter (where census_tract is not null) = 1
             then min(census_tract) end as census_tract,

        case when count(distinct building_identification_number)
                  filter (where building_identification_number is not null) = 1
             then min(building_identification_number) end as building_identification_number,

        case when count(distinct borough_block_lot)
                  filter (where borough_block_lot is not null) = 1
             then min(borough_block_lot) end as borough_block_lot,

        case when count(distinct neighborhood_tabulation_area)
                  filter (where neighborhood_tabulation_area is not null) = 1
             then min(neighborhood_tabulation_area) end as neighborhood_tabulation_area

    from base
    where restaurant_business_id is not null
    group by restaurant_business_id
),

bbl_recovery as (
    select
        borough_block_lot,

        case when count(distinct zip_code) filter (where zip_code is not null) = 1
             then min(zip_code) end as zip_code,

        case when count(distinct community_board) filter (where community_board is not null) = 1
             then min(community_board) end as community_board,

        case when count(distinct neighborhood_tabulation_area)
                  filter (where neighborhood_tabulation_area is not null) = 1
             then min(neighborhood_tabulation_area) end as neighborhood_tabulation_area

    from base
    where borough_block_lot is not null
    group by borough_block_lot
)

select
    b.restaurant_business_id,

    coalesce(b.restaurant_name, ir.restaurant_name, rr.restaurant_name)
        as restaurant_name,

    coalesce(b.borough, ir.borough, rr.borough)
        as borough,

    coalesce(b.building_number, ir.building_number, rr.building_number)
        as building_number,

    coalesce(b.street_name, ir.street_name, rr.street_name)
        as street_name,

    coalesce(
        b.zip_code,
        ir.zip_code,
        rr.zip_code,
        br.zip_code
    ) as zip_code,

    coalesce(b.phone_number, ir.phone_number, rr.phone_number)
        as phone_number,

    coalesce(b.cuisine, ir.cuisine, rr.cuisine)
        as cuisine,

    b.inspection_datetime,

    coalesce(b.inspection_action, ir.inspection_action)
        as inspection_action,

    b.violation_code,
    b.violation_description,
    b.violation_critical_flag,

    coalesce(b.inspection_score, ir.inspection_score)
        as inspection_score,

    coalesce(b.inspection_grade, ir.inspection_grade)
        as inspection_grade,

    coalesce(b.grade_datetime, ir.grade_datetime)
        as grade_datetime,

    b.record_datetime,
    b.inspection_type,

    coalesce(
        b.community_board,
        ir.community_board,
        rr.community_board,
        br.community_board
    ) as community_board,

    coalesce(b.council_district, ir.council_district, rr.council_district)
        as council_district,

    coalesce(b.census_tract, ir.census_tract, rr.census_tract)
        as census_tract,

    coalesce(
        b.building_identification_number,
        ir.building_identification_number,
        rr.building_identification_number
    ) as building_identification_number,

    coalesce(
        b.borough_block_lot,
        ir.borough_block_lot,
        rr.borough_block_lot
    ) as borough_block_lot,

    coalesce(
        b.neighborhood_tabulation_area,
        ir.neighborhood_tabulation_area,
        rr.neighborhood_tabulation_area,
        br.neighborhood_tabulation_area
    ) as neighborhood_tabulation_area,

    b.inspection_id

from base b

left join inspection_recovery ir
    on ir.inspection_id = b.inspection_id

left join restaurant_recovery rr
    on rr.restaurant_business_id = b.restaurant_business_id

left join bbl_recovery br
    on br.borough_block_lot = b.borough_block_lot