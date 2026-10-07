with ranked as (
    select distinct on (violation_code)
        {{ dbt_utils.generate_surrogate_key(['violation_code']) }} as violation_key,
        violation_code,
        coalesce(violation_description,'Unknown')   as violation_description,
        coalesce(violation_critical_flag,'Unknown') as violation_critical_flag
    from {{ ref('int_restaurant_inspections_recovered') }}
    where violation_code is not null
    order by violation_code, record_datetime desc nulls last
)
select * from ranked
union all
select '-1','Unknown','Unknown','Unknown'