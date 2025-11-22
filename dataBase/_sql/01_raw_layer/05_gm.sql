-- главная таблица

create table public.gm_vacancies (
    id bigint primary key,
    published_at timestamp without time zone,
    is_active boolean,
    title text,
    salary_from bigint,
    salary_to bigint,
    salary_currency_id text references public.currency(id),
    url text,
    english_level text,
    remote_options text,
    office_options text,
    employer text,
    level text,
    experience_years integer
);


-- списки

create table public.gm_skills (
    id bigint references public.gm_vacancies(id),
    name text not null,
    primary key (id, name)
);