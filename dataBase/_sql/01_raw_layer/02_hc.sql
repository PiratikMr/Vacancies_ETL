-- главная таблица

create table public.hc_vacancies (
    id bigint primary key,
    title text,
    remote_work boolean,
    grade text,
    published_at timestamp with time zone,
    employer text,
    employment_type text,
    salary_from bigint,
    salary_to bigint,
    salary_currency_id text references public.currency(id),
    is_active boolean,
    url text
);


-- списки

create table public.hc_skills (
    id bigint references public.hc_vacancies(id),
    name text not null,
    primary key (id, name)
);

create table public.hc_fields (
    id bigint references public.hc_vacancies(id),
    name text not null,
    primary key (id, name)
);

create table public.hc_locations (
    id bigint references public.hc_vacancies(id),
    name text not null,
    primary key (id, name)
);