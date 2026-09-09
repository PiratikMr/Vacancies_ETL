alter table mapping_dim_currency drop constraint mapping_dim_currency_pkey;
alter table mapping_dim_currency
    add column mapping_id                   bigserial,
    add column is_active                    boolean                 not null default true,
    add column origin                       text                    not null default 'legacy',
    add column link_score                   real;
alter table mapping_dim_currency
    add constraint mapping_dim_currency_pkey primary key (mapping_id),
    add constraint uq_mapping_dim_currency_value unique (currency_id, mapped_value),
    add constraint ch_mapping_dim_currency_origin
        check (origin in ('reference', 'etl', 'nlp_merge', 'manual', 'legacy', 'unknown')),
    alter column origin set default 'unknown';

alter table mapping_dim_employer drop constraint mapping_dim_employer_pkey;
alter table mapping_dim_employer
    add column mapping_id                   bigserial,
    add column is_active                    boolean                 not null default true,
    add column origin                       text                    not null default 'legacy',
    add column link_score                   real;
alter table mapping_dim_employer
    add constraint mapping_dim_employer_pkey primary key (mapping_id),
    add constraint uq_mapping_dim_employer_value unique (employer_id, mapped_value),
    add constraint ch_mapping_dim_employer_origin
        check (origin in ('reference', 'etl', 'nlp_merge', 'manual', 'legacy', 'unknown')),
    alter column origin set default 'unknown';

alter table mapping_dim_employment drop constraint mapping_dim_employment_pkey;
alter table mapping_dim_employment
    add column mapping_id                   bigserial,
    add column is_active                    boolean                 not null default true,
    add column origin                       text                    not null default 'legacy',
    add column link_score                   real;
alter table mapping_dim_employment
    add constraint mapping_dim_employment_pkey primary key (mapping_id),
    add constraint uq_mapping_dim_employment_value unique (employment_id, mapped_value),
    add constraint ch_mapping_dim_employment_origin
        check (origin in ('reference', 'etl', 'nlp_merge', 'manual', 'legacy', 'unknown')),
    alter column origin set default 'unknown';

alter table mapping_dim_experience drop constraint mapping_dim_experience_pkey;
alter table mapping_dim_experience
    add column mapping_id                   bigserial,
    add column is_active                    boolean                 not null default true,
    add column origin                       text                    not null default 'legacy',
    add column link_score                   real;
alter table mapping_dim_experience
    add constraint mapping_dim_experience_pkey primary key (mapping_id),
    add constraint uq_mapping_dim_experience_value unique (experience_id, mapped_value),
    add constraint ch_mapping_dim_experience_origin
        check (origin in ('reference', 'etl', 'nlp_merge', 'manual', 'legacy', 'unknown')),
    alter column origin set default 'unknown';

alter table mapping_dim_field drop constraint mapping_dim_field_pkey;
alter table mapping_dim_field
    add column mapping_id                   bigserial,
    add column is_active                    boolean                 not null default true,
    add column origin                       text                    not null default 'legacy',
    add column link_score                   real;
alter table mapping_dim_field
    add constraint mapping_dim_field_pkey primary key (mapping_id),
    add constraint uq_mapping_dim_field_value unique (field_id, mapped_value),
    add constraint ch_mapping_dim_field_origin
        check (origin in ('reference', 'etl', 'nlp_merge', 'manual', 'legacy', 'unknown')),
    alter column origin set default 'unknown';

alter table mapping_dim_grade drop constraint mapping_dim_grade_pkey;
alter table mapping_dim_grade
    add column mapping_id                   bigserial,
    add column is_active                    boolean                 not null default true,
    add column origin                       text                    not null default 'legacy',
    add column link_score                   real;
alter table mapping_dim_grade
    add constraint mapping_dim_grade_pkey primary key (mapping_id),
    add constraint uq_mapping_dim_grade_value unique (grade_id, mapped_value),
    add constraint ch_mapping_dim_grade_origin
        check (origin in ('reference', 'etl', 'nlp_merge', 'manual', 'legacy', 'unknown')),
    alter column origin set default 'unknown';

alter table mapping_dim_language_level drop constraint mapping_dim_language_level_pkey;
alter table mapping_dim_language_level
    add column mapping_id                   bigserial,
    add column is_active                    boolean                 not null default true,
    add column origin                       text                    not null default 'legacy',
    add column link_score                   real;
alter table mapping_dim_language_level
    add constraint mapping_dim_language_level_pkey primary key (mapping_id),
    add constraint uq_mapping_dim_language_level_value unique (language_level_id, mapped_value),
    add constraint ch_mapping_dim_language_level_origin
        check (origin in ('reference', 'etl', 'nlp_merge', 'manual', 'legacy', 'unknown')),
    alter column origin set default 'unknown';

alter table mapping_dim_language drop constraint mapping_dim_language_pkey;
alter table mapping_dim_language
    add column mapping_id                   bigserial,
    add column is_active                    boolean                 not null default true,
    add column origin                       text                    not null default 'legacy',
    add column link_score                   real;
alter table mapping_dim_language
    add constraint mapping_dim_language_pkey primary key (mapping_id),
    add constraint uq_mapping_dim_language_value unique (language_id, mapped_value),
    add constraint ch_mapping_dim_language_origin
        check (origin in ('reference', 'etl', 'nlp_merge', 'manual', 'legacy', 'unknown')),
    alter column origin set default 'unknown';

alter table mapping_dim_country drop constraint mapping_dim_country_pkey;
alter table mapping_dim_country
    add column mapping_id                   bigserial,
    add column is_active                    boolean                 not null default true,
    add column origin                       text                    not null default 'legacy',
    add column link_score                   real;
alter table mapping_dim_country
    add constraint mapping_dim_country_pkey primary key (mapping_id),
    add constraint uq_mapping_dim_country_value unique (country_id, mapped_value),
    add constraint ch_mapping_dim_country_origin
        check (origin in ('reference', 'etl', 'nlp_merge', 'manual', 'legacy', 'unknown')),
    alter column origin set default 'unknown';

alter table mapping_dim_location drop constraint mapping_dim_location_pkey;
alter table mapping_dim_location
    add column mapping_id                   bigserial,
    add column is_active                    boolean                 not null default true,
    add column origin                       text                    not null default 'legacy',
    add column link_score                   real;
alter table mapping_dim_location
    add constraint mapping_dim_location_pkey primary key (mapping_id),
    add constraint uq_mapping_dim_location_value unique (location_id, mapped_value),
    add constraint ch_mapping_dim_location_origin
        check (origin in ('reference', 'etl', 'nlp_merge', 'manual', 'legacy', 'unknown')),
    alter column origin set default 'unknown';

alter table mapping_dim_platform drop constraint mapping_dim_platform_pkey;
alter table mapping_dim_platform
    add column mapping_id                   bigserial,
    add column is_active                    boolean                 not null default true,
    add column origin                       text                    not null default 'legacy',
    add column link_score                   real;
alter table mapping_dim_platform
    add constraint mapping_dim_platform_pkey primary key (mapping_id),
    add constraint uq_mapping_dim_platform_value unique (platform_id, mapped_value),
    add constraint ch_mapping_dim_platform_origin
        check (origin in ('reference', 'etl', 'nlp_merge', 'manual', 'legacy', 'unknown')),
    alter column origin set default 'unknown';

alter table mapping_dim_schedule drop constraint mapping_dim_schedule_pkey;
alter table mapping_dim_schedule
    add column mapping_id                   bigserial,
    add column is_active                    boolean                 not null default true,
    add column origin                       text                    not null default 'legacy',
    add column link_score                   real;
alter table mapping_dim_schedule
    add constraint mapping_dim_schedule_pkey primary key (mapping_id),
    add constraint uq_mapping_dim_schedule_value unique (schedule_id, mapped_value),
    add constraint ch_mapping_dim_schedule_origin
        check (origin in ('reference', 'etl', 'nlp_merge', 'manual', 'legacy', 'unknown')),
    alter column origin set default 'unknown';

alter table mapping_dim_skill drop constraint mapping_dim_skill_pkey;
alter table mapping_dim_skill
    add column mapping_id                   bigserial,
    add column is_active                    boolean                 not null default true,
    add column origin                       text                    not null default 'legacy',
    add column link_score                   real;
alter table mapping_dim_skill
    add constraint mapping_dim_skill_pkey primary key (mapping_id),
    add constraint uq_mapping_dim_skill_value unique (skill_id, mapped_value),
    add constraint ch_mapping_dim_skill_origin
        check (origin in ('reference', 'etl', 'nlp_merge', 'manual', 'legacy', 'unknown')),
    alter column origin set default 'unknown';


create table match_log_currency (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    mapping_id                          bigint                          not null references mapping_dim_currency (mapping_id),
    raw_value                           text,
    score                               real,

    unique nulls not distinct (vacancy_id, mapping_id, raw_value)
);

create index on match_log_currency (mapping_id);

create table match_log_employer (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    mapping_id                          bigint                          not null references mapping_dim_employer (mapping_id),
    raw_value                           text,
    score                               real,

    unique nulls not distinct (vacancy_id, mapping_id, raw_value)
);

create index on match_log_employer (mapping_id);

create table match_log_employment (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    mapping_id                          bigint                          not null references mapping_dim_employment (mapping_id),
    raw_value                           text,
    score                               real,

    unique nulls not distinct (vacancy_id, mapping_id, raw_value)
);

create index on match_log_employment (mapping_id);

create table match_log_experience (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    mapping_id                          bigint                          not null references mapping_dim_experience (mapping_id),
    raw_value                           text,
    score                               real,

    unique nulls not distinct (vacancy_id, mapping_id, raw_value)
);

create index on match_log_experience (mapping_id);

create table match_log_field (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    mapping_id                          bigint                          not null references mapping_dim_field (mapping_id),
    raw_value                           text,
    score                               real,

    unique nulls not distinct (vacancy_id, mapping_id, raw_value)
);

create index on match_log_field (mapping_id);

create table match_log_grade (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    mapping_id                          bigint                          not null references mapping_dim_grade (mapping_id),
    raw_value                           text,
    score                               real,

    unique nulls not distinct (vacancy_id, mapping_id, raw_value)
);

create index on match_log_grade (mapping_id);

create table match_log_language_level (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    mapping_id                          bigint                          not null references mapping_dim_language_level (mapping_id),
    raw_value                           text,
    score                               real,

    unique nulls not distinct (vacancy_id, mapping_id, raw_value)
);

create index on match_log_language_level (mapping_id);

create table match_log_language (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    mapping_id                          bigint                          not null references mapping_dim_language (mapping_id),
    raw_value                           text,
    score                               real,

    unique nulls not distinct (vacancy_id, mapping_id, raw_value)
);

create index on match_log_language (mapping_id);

create table match_log_country (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    mapping_id                          bigint                          not null references mapping_dim_country (mapping_id),
    raw_value                           text,
    score                               real,

    unique nulls not distinct (vacancy_id, mapping_id, raw_value)
);

create index on match_log_country (mapping_id);

create table match_log_location (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    mapping_id                          bigint                          not null references mapping_dim_location (mapping_id),
    raw_value                           text,
    score                               real,

    unique nulls not distinct (vacancy_id, mapping_id, raw_value)
);

create index on match_log_location (mapping_id);

create table match_log_platform (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    mapping_id                          bigint                          not null references mapping_dim_platform (mapping_id),
    raw_value                           text,
    score                               real,

    unique nulls not distinct (vacancy_id, mapping_id, raw_value)
);

create index on match_log_platform (mapping_id);

create table match_log_schedule (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    mapping_id                          bigint                          not null references mapping_dim_schedule (mapping_id),
    raw_value                           text,
    score                               real,

    unique nulls not distinct (vacancy_id, mapping_id, raw_value)
);

create index on match_log_schedule (mapping_id);

create table match_log_skill (
    vacancy_id                          bigint                          not null references fact_vacancy (vacancy_id),
    mapping_id                          bigint                          not null references mapping_dim_skill (mapping_id),
    raw_value                           text,
    score                               real,

    unique nulls not distinct (vacancy_id, mapping_id, raw_value)
);

create index on match_log_skill (mapping_id);


insert into match_log_currency (vacancy_id, mapping_id)
select f.vacancy_id, m.mapping_id
from fact_vacancy f
join mapping_dim_currency m on m.currency_id = f.currency_id and m.is_canonical
where f.currency_id is not null;

insert into match_log_employer (vacancy_id, mapping_id)
select f.vacancy_id, m.mapping_id
from fact_vacancy f
join mapping_dim_employer m on m.employer_id = f.employer_id and m.is_canonical
where f.employer_id is not null;

insert into match_log_experience (vacancy_id, mapping_id)
select f.vacancy_id, m.mapping_id
from fact_vacancy f
join mapping_dim_experience m on m.experience_id = f.experience_id and m.is_canonical
where f.experience_id is not null;

insert into match_log_platform (vacancy_id, mapping_id)
select f.vacancy_id, m.mapping_id
from fact_vacancy f
join mapping_dim_platform m on m.platform_id = f.platform_id and m.is_canonical;

insert into match_log_employment (vacancy_id, mapping_id)
select b.vacancy_id, m.mapping_id
from bridge_vacancy_employment b
join mapping_dim_employment m on m.employment_id = b.employment_id and m.is_canonical;

insert into match_log_field (vacancy_id, mapping_id)
select b.vacancy_id, m.mapping_id
from bridge_vacancy_field b
join mapping_dim_field m on m.field_id = b.field_id and m.is_canonical;

insert into match_log_grade (vacancy_id, mapping_id)
select b.vacancy_id, m.mapping_id
from bridge_vacancy_grade b
join mapping_dim_grade m on m.grade_id = b.grade_id and m.is_canonical;

insert into match_log_location (vacancy_id, mapping_id)
select b.vacancy_id, m.mapping_id
from bridge_vacancy_location b
join mapping_dim_location m on m.location_id = b.location_id and m.is_canonical;

insert into match_log_schedule (vacancy_id, mapping_id)
select b.vacancy_id, m.mapping_id
from bridge_vacancy_schedule b
join mapping_dim_schedule m on m.schedule_id = b.schedule_id and m.is_canonical;

insert into match_log_skill (vacancy_id, mapping_id)
select b.vacancy_id, m.mapping_id
from bridge_vacancy_skill b
join mapping_dim_skill m on m.skill_id = b.skill_id and m.is_canonical;

insert into match_log_language (vacancy_id, mapping_id)
select distinct b.vacancy_id, m.mapping_id
from bridge_vacancy_language b
join mapping_dim_language m on m.language_id = b.language_id and m.is_canonical;

insert into match_log_language_level (vacancy_id, mapping_id)
select distinct b.vacancy_id, m.mapping_id
from bridge_vacancy_language b
join mapping_dim_language_level m on m.language_level_id = b.language_level_id and m.is_canonical;

insert into match_log_country (vacancy_id, mapping_id)
select distinct b.vacancy_id, m.mapping_id
from bridge_vacancy_location b
join dim_location l on l.location_id = b.location_id
join mapping_dim_country m on m.country_id = l.country_id and m.is_canonical;


do $$
declare
    rec record;
begin
    for rec in
        select 'currency' as entity, count(*) as cnt from dim_currency d
            where not exists (select 1 from mapping_dim_currency m
                              where m.currency_id = d.currency_id and m.is_canonical)
        union all
        select 'employer', count(*) from dim_employer d
            where not exists (select 1 from mapping_dim_employer m
                              where m.employer_id = d.employer_id and m.is_canonical)
        union all
        select 'employment', count(*) from dim_employment d
            where not exists (select 1 from mapping_dim_employment m
                              where m.employment_id = d.employment_id and m.is_canonical)
        union all
        select 'experience', count(*) from dim_experience d
            where not exists (select 1 from mapping_dim_experience m
                              where m.experience_id = d.experience_id and m.is_canonical)
        union all
        select 'field', count(*) from dim_field d
            where not exists (select 1 from mapping_dim_field m
                              where m.field_id = d.field_id and m.is_canonical)
        union all
        select 'grade', count(*) from dim_grade d
            where not exists (select 1 from mapping_dim_grade m
                              where m.grade_id = d.grade_id and m.is_canonical)
        union all
        select 'language_level', count(*) from dim_language_level d
            where not exists (select 1 from mapping_dim_language_level m
                              where m.language_level_id = d.language_level_id and m.is_canonical)
        union all
        select 'language', count(*) from dim_language d
            where not exists (select 1 from mapping_dim_language m
                              where m.language_id = d.language_id and m.is_canonical)
        union all
        select 'country', count(*) from dim_country d
            where not exists (select 1 from mapping_dim_country m
                              where m.country_id = d.country_id and m.is_canonical)
        union all
        select 'location', count(*) from dim_location d
            where not exists (select 1 from mapping_dim_location m
                              where m.location_id = d.location_id and m.is_canonical)
        union all
        select 'platform', count(*) from dim_platform d
            where not exists (select 1 from mapping_dim_platform m
                              where m.platform_id = d.platform_id and m.is_canonical)
        union all
        select 'schedule', count(*) from dim_schedule d
            where not exists (select 1 from mapping_dim_schedule m
                              where m.schedule_id = d.schedule_id and m.is_canonical)
        union all
        select 'skill', count(*) from dim_skill d
            where not exists (select 1 from mapping_dim_skill m
                              where m.skill_id = d.skill_id and m.is_canonical)
    loop
        if rec.cnt > 0 then
            raise notice 'dim_%: % записей без канонического маппинга, их связи не попали в match_log_%',
                rec.entity, rec.cnt, rec.entity;
        end if;
    end loop;
end $$;
