create table dim_experience (
    experience_id                       bigserial               primary key,
    experience                          text                    not null,

    unique(experience)
);

create table mapping_dim_experience (
    experience_id                       bigint                  not null references dim_experience(experience_id),
    mapped_value                        text                    not null,
    is_canonical                        boolean                 not null,

    primary key (experience_id, mapped_value)
);

create unique index uq_canonical_per_experience
on mapping_dim_experience (experience_id)
where (is_canonical = true);