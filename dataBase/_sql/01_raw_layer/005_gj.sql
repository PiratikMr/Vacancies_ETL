create table public.gj_vacancies (
    id text primary key,
    title text,
    employer text,
    experience text,
    salary_from bigint,
    salary_to bigint,
    salary_currency_id text references public.exchangerate_currency(id),
    url text,
    published_at timestamp without time zone,
    closed_at timestamp without time zone
);

create index idx_vacancies_published_closed_null
    on public.gj_vacancies (published_at)
    where closed_at is null;


create table public.gj_skills (
    id text references public.gj_vacancies(id),
    name text not null,
    primary key (id, name)
);

create table public.gj_fields (
    id text references public.gj_vacancies(id),
    name text not null,
    primary key (id, name)
);

create table public.gj_grades (
    id text references public.gj_vacancies(id),
    name text not null,
    primary key (id, name)
);

create table public.gj_jobformats (
    id text references public.gj_vacancies(id),
    name text not null,
    primary key (id, name)
);

create table public.gj_locations (
    id text references public.gj_vacancies(id),
    name text not null,
    primary key (id, name)
);