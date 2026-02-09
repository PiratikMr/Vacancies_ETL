create table dim_platform (
    platform_id                         bigserial               primary key,
    platform                            text                    not null,

    unique(platform)
);

create table mapping_dim_platform (
    platform_id                         bigint                  not null references dim_platform(platform_id),
    mapped_value                        text                    not null,
    is_canonical                        boolean                 not null,

    primary key (platform_id, mapped_value)
);

create unique index uq_canonical_per_platform
on mapping_dim_platform (platform_id)
where (is_canonical = true);