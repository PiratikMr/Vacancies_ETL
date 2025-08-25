create materialized view external.lnglatmap as
    select 
        address_lat as lat,
        address_lng as lng,
        is_active
    from hh_vacancies
    where address_lat is not null and address_lng is not null;


