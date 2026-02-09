create table dim_country (
    country_id                          bigserial               primary key,
    country                             text                    not null,
    iso                                 text,

    unique (country)
);
insert into dim_country (country_id, country) values (0, 'Неизвестно');

create table mapping_dim_country (
    country_id                          bigint                  not null references dim_country(country_id),
    mapped_value                        text                    not null,
    is_canonical                        boolean                 not null,

    primary key (country_id, mapped_value)
);

create unique index uq_canonical_per_country
on mapping_dim_country (country_id)
where (is_canonical = true);



create table dim_location (
    location_id                         bigserial               primary key,
    location                            text                    not null,
    country_id                          bigint                  not null default(0) references dim_country (country_id),

    unique (location, country_id)
);

create table mapping_dim_location (
    location_id                         bigint                  not null references dim_location(location_id),
    mapped_value                        text                    not null,
    is_canonical                        boolean                 not null,

    primary key (location_id, mapped_value)
);

create unique index uq_canonical_per_location
on mapping_dim_location (location_id)
where (is_canonical = true);