create table public.az_vacancies (
    id bigint primary key,
    title text,
    published_at timestamp with time zone,
    closed_at timestamp with time zone,
    address_lat double precision,
    address_lng double precision,
    country text,
    region text,
    employer text,
    url text,
    salary_from bigint,
    salary_to bigint,
    salary_currency_id text references public.exchangerate_currency(id),
    employment_type text
);

create index idx_az_vacancies_published_closed_null
    on public.az_vacancies (published_at)
    where closed_at is null;