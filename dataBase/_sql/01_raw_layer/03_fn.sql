-- главная таблица

create table public.fn_vacancies (
    id bigint primary key,
    title text,
    employment_type text,
    salary_from bigint,
    salary_to bigint,
    salary_currency_id text references public.currency(id),
    published_at timestamp without time zone,
    url text,
    experience text,
    distant_work boolean,
    employer text,
    address_lat double precision,
    address_lng double precision,
    is_active boolean
);


-- списки

create table public.fn_fields (
    id bigint references public.fn_vacancies(id),
    name text not null,
    primary key (id, name)
);

create table public.fn_locations (
    id bigint references public.fn_vacancies(id),
    name text not null,
    country text,
    primary key (id, name)
);