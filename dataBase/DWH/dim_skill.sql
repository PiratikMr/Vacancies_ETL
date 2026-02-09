create table dim_skill (
    skill_id                         bigserial               primary key,
    skill                            text                    not null,

    unique(skill)
);

create table mapping_dim_skill (
    skill_id                         bigint                  not null references dim_skill(skill_id),
    mapped_value                     text                    not null,
    is_canonical                     boolean                 not null,

    primary key (skill_id, mapped_value)
);

create unique index uq_canonical_per_skill
on mapping_dim_skill (skill_id)
where (is_canonical = true);