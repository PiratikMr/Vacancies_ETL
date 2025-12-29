with data as (
    select
        concat('fn_', v.id) as id,
        l.name as region,
        l.country as country,
        v.address_lat as lat,
        v.address_lng as lng
    from {{ source('raw_data', 'fn_vacancies') }} as v
    join {{ source('raw_data', 'fn_locations') }} as l on l.id = v.id

    -- union all

    -- select
    --     concat('gj_', v.id) as id,
    --     concat('Типа город ',l.name) as region,
    --     concat('Типа страна ',l.name) as contry,
    --     null as lat,
    --     null as lng
    -- from gj_vacancies as v
    -- join gj_locations as l on l.id = v.id

    union all

    select
        concat('hc_', v.id) as id,
        l.name as region,
        'Россия' as contry,
        null as lat,
        null as lng
    from {{ source('raw_data', 'hc_vacancies') }} as v
    join {{ source('raw_data', 'hc_locations') }} as l on l.id = v.id

    union all

    select
        concat('hh_', v.id) as id,
        ar.name as region,
        ac.name as contry,
        v.address_lat as lat,
        v.address_lng as lng
    from {{ source('raw_data', 'hh_vacancies') }} as v
    join {{ source('raw_data', 'hh_areas') }} as ar on ar.id = v.area_id
    join {{ source('raw_data', 'hh_areas') }} as ac on ac.id = ar.parent_id
)

select
    id,
    region,
    country,
    lat,
    lng
from data