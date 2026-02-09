create table dim_language_level (
    language_level_id                         bigserial               primary key,
    language_level                            text                    not null,

    unique(language_level)
);

create table mapping_dim_language_level (
    language_level_id                         bigint                  not null references dim_language_level(language_level_id),
    mapped_value                        text                    not null,
    is_canonical                        boolean                 not null,

    primary key (language_level_id, mapped_value)
);

create unique index uq_canonical_per_language_level
on mapping_dim_language_level (language_level_id)
where (is_canonical = true);