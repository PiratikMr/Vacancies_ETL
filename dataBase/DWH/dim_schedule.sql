create table dim_schedule (
    schedule_id                         bigserial               primary key,
    schedule                            text                    not null,

    unique(schedule)
);

create table mapping_dim_schedule (
    schedule_id                         bigint                  not null references dim_schedule(schedule_id),
    mapped_value                        text                    not null,
    is_canonical                        boolean                 not null,

    primary key (schedule_id, mapped_value)
);

create unique index uq_canonical_per_schedule
on mapping_dim_schedule (schedule_id)
where (is_canonical = true);