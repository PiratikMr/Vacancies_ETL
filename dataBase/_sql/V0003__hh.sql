create table public.hh_areas (
    id bigint primary key,
    name text,
    parent_id bigint
);

create table public.hh_employers (
    id text primary key,
    name text,
    trusted boolean
);

create table public.hh_employment (
    id text primary key,
    name text
);

create table public.hh_experience (
    id text primary key,
    name text
);

create table public.hh_professionalroles (
    id bigint primary key,
    field_id bigint,
    name text
);

create table public.hh_schedule (
    id text primary key,
    name text
);


create table public.hh_vacancies (
    id bigint primary key,
    address_lat double precision,
    address_lng double precision,
    address_has_metro boolean,
    url text,
    area_id bigint references public.hh_areas(id),
    employer_id text references public.hh_employers(id),
    employment_id text references public.hh_employment(id),
    experience_id text references public.hh_experience(id),
    title text,
    are_night_shifts boolean,
    role_id bigint references public.hh_professionalroles(id),
    published_at timestamp without time zone,
    closed_at timestamp without time zone,
    salary_currency_id text references public.exchangerate_currency(id),
    salary_frequency text,
    salary_from double precision,
    salary_could_gross boolean,
    salary_mode text,
    salary_to double precision,
    schedule_id text references public.hh_schedule(id)
);

create index idx_hh_vacancies_published_closed_null
    on public.hh_vacancies (published_at)
    where closed_at is null;


create table public.hh_skills (
    id bigint not null references public.hh_vacancies(id),
    name text not null,
    primary key (id, name)
);

create table public.hh_languages (
    id bigint references public.hh_vacancies(id),
    name text not null,
    level text not null,
    primary key (id, name, level)
);