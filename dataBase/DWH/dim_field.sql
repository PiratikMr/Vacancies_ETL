create table dim_field (
    field_id                         bigserial               primary key,
    field                            text                    not null,

    unique(field)
);

create table mapping_dim_field (
    field_id                         bigint                  not null references dim_field(field_id),
    mapped_value                     text                    not null,
    is_canonical                     boolean                 not null,

    primary key (field_id, mapped_value)
);

create unique index uq_canonical_per_field
on mapping_dim_field (field_id)
where (is_canonical = true);