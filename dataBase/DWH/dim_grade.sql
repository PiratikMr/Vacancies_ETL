create table dim_grade (
    grade_id                         bigserial               primary key,
    grade                            text                    not null,

    unique(grade)
);

create table mapping_dim_grade (
    grade_id                         bigint                  not null references dim_grade(grade_id),
    mapped_value                     text                    not null,
    is_canonical                     boolean                 not null,

    primary key (grade_id, mapped_value)
);

create unique index uq_canonical_per_grade
on mapping_dim_grade (grade_id)
where (is_canonical = true);