create table dim_employment (
    employment_id                         bigserial               primary key,
    employment                            text                    not null,

    unique(employment)
);

create table mapping_dim_employment (
    employment_id                         bigint                  not null references dim_employment(employment_id),
    mapped_value                        text                    not null,
    is_canonical                        boolean                 not null,

    primary key (employment_id, mapped_value)
);

create unique index uq_canonical_per_employment
on mapping_dim_employment (employment_id)
where (is_canonical = true);