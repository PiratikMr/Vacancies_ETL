create table dim_employer (
    employer_id                         bigserial               primary key,
    employer                            text                    not null,

    unique(employer)
);

create table mapping_dim_employer (
    employer_id                         bigint                  not null references dim_employer(employer_id),
    mapped_value                        text                    not null,
    is_canonical                        boolean                 not null,

    primary key (employer_id, mapped_value)
);

create unique index uq_canonical_per_employer
on mapping_dim_employer (employer_id)
where (is_canonical = true);