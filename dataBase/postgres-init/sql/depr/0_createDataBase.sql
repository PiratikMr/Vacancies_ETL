--// Common
    create table public.currency (
        id text primary key,
        name text,
        rate double precision,
        code text
    );
    insert into public.currency (id, name, rate, code) values
        ('AZN', 'Манаты', 1, '₼'),
        ('BYR', 'Белорусские рубли', 1, 'Br'),
        ('EUR', 'Евро', 1, '€'),
        ('GEL', 'Грузинский лари', 1, '₾'),
        ('KGS', 'Кыргызский сом', 1, 'сом'),
        ('KZT', 'Тенге', 1, '₸'),
        ('RUR', 'Рубли', 1, '₽'),
        ('UAH', 'Гривны', 1, '₴'),
        ('USD', 'Доллары', 1, '$'),
        ('UZS', 'Узбекский сум', 1, 'so’m');
--//


--// hh
    create table public.hh_vacancies (
        id bigint primary key,
        name text,
        archived boolean,
        region_area_id bigint,
        country_area_id bigint,
        salary_from bigint,
        salary_to bigint,
        close_to_metro boolean,
        publish_date timestamp without time zone,
        schedule_id text,
        experience_id text,
        employment_id text,
        employer_id bigint,
        currency_id text,
        role_id bigint
    );
    create index idx_hh_vacancies_name_lower on public.hh_vacancies using btree (lower(name));

    create table public.hh_areas (
        id bigint primary key,
        name text,
        parent_id bigint
    );

    create table public.hh_employers (
        id bigint primary key,
        name text
    );

    create table public.hh_employment (
        id text primary key,
        name text
    );

    create table public.hh_experience (
        id text primary key,
        name text
    );

    create table public.hh_roles (
        id bigint primary key,
        name text
    );

    create table public.hh_schedule (
        id text primary key,
        name text
    );

    create table public.hh_skills (
        id bigint,
        name text,
        primary key(id, name)
    );
--//


--// gj
    create table public.gj_vacancies (
        id text primary key,
        name text,
        employer text,
        experience text,
        publish_date timestamp without time zone,
        salary_from bigint,
        salary_to bigint,
        currency_id text
    );
    create index idx_gj_vacancies_name_lower on public.gj_vacancies using btree (lower(name));

    create table public.gj_fields (
        id text,
        name text,
        primary key(id, name)
    );

    create table public.gj_jobformat (
        id text,
        name text,
        primary key(id, name)
    );

    create table public.gj_level (
        id text,
        name text,
        primary key(id, name)
    );

    create table public.gj_locations (
        id text,
        name text,
        primary key(id, name)
    );
    create index idx_gj_locations_name_lower on public.gj_locations using btree (lower(name));

    create table public.gj_skills (
        id text,
        name text,
        primary key(id, name)
    );
--//


--// gm
    create table public.gm_vacancies (
        id bigint primary key,
        name text,
        publish_date timestamp without time zone,
        salary_from bigint,
        salary_to bigint,
        salary_hidden boolean,
        currency_id text,
        english_lvl text,
        remote_op text,
        office_op text,
        employer text
    );
    create index idx_gm_vacancies_name_lower on public.gm_vacancies using btree (lower(name));

    create table public.gm_skills (
        id bigint,
        name text,
        primary key(id, name)
    );

    create table public.gm_locations (
        id bigint,
        city text,
        country text,
        primary key(id, city, country)
    );
    create index idx_gm_locations_country_lower on public.gm_locations using btree (lower(country));
--//