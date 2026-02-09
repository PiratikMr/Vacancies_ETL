create table fact_vacancy (
    vacancy_id                          bigserial                       primary key,

    external_id                         text                            not null,
    platform_id                         bigint                          not null references dim_platform (platform_id),
    employer_id                         bigint                          references dim_employer (employer_id),
    currency_id                         bigint                          references dim_currency (currency_id),
    experience_id                       bigint                          references dim_experience (experience_id),
    
    latitude                            double precision,
    longitude                           double precision,

    salary_from                         double precision,
    salary_to                           double precision,

    published_at                        timestamp without time zone     not null,
    title                               text                            not null,
    url                                 text                            not null,

    created_at                          timestamp without time zone     not null default now(),
    closed_at                           timestamp without time zone,

    unique(external_id, platform_id)
);


create table bridge_vacancy_skill (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    skill_id                            bigint                          not null references dim_skill (skill_id),

    primary key (vacancy_id, skill_id)
);

create table bridge_vacancy_schedule (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    schedule_id                         bigint                          not null references dim_schedule (schedule_id),

    primary key (vacancy_id, schedule_id)
);

create table bridge_vacancy_location (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    location_id                         bigint                          not null references dim_location (location_id),

    primary key (vacancy_id, location_id)
);

create table bridge_vacancy_field (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    field_id                            bigint                          not null references dim_field (field_id),

    primary key (vacancy_id, field_id)
);

create table bridge_vacancy_grade (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    grade_id                            bigint                          not null references dim_grade (grade_id),

    primary key (vacancy_id, grade_id)
);

create table bridge_vacancy_employment (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    employment_id                       bigint                          not null references dim_employment (employment_id),

    primary key (vacancy_id, employment_id)
);

create table bridge_vacancy_language (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    language_id                         bigint                          not null references dim_language (language_id),
    language_level_id                   bigint                          not null references dim_language_level (language_level_id),

    primary key (vacancy_id, language_id, language_level_id)
);