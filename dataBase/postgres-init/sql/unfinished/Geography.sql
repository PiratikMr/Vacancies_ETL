create materialized view world_map as
    with data as (
        select
            address_lat as lat,
            address_lng as lng,
            is_active,
            url
        from hh_vacancies
        where address_lat is not null and address_lng is not null

        union all

        select
            address_lat as lat,
            address_lng as lng,
            is_active,
            url
        from fn_vacancies
        where address_lat is not null and address_lng is not null

    ) select lat, lng, is_active, url from data;