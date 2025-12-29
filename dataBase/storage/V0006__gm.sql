create table public.gm_vacancies (
    id bigint primary key,
    published_at timestamp without time zone,
    closed_at timestamp without time zone,
    title text,
    salary_from bigint,
    salary_to bigint,
    salary_currency_id text references public.exchangerate_currency(id),
    url text,
    english_level text,
    remote_options text,
    office_options text,
    employer text,
    level text,
    experience_years integer
);

create index idx_gm_vacancies_published_closed_null
    on public.gm_vacancies (published_at)
    where closed_at is null;


create table public.gm_skills (
    id bigint references public.gm_vacancies(id),
    name text not null,
    primary key (id, name)
);