--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5 (Debian 17.5-1.pgdg120+1)
-- Dumped by pg_dump version 17.5

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


CREATE TABLE public.hh_vacancies (
    address_lat double precision,
    address_lng double precision,
    address_has_metro boolean,
    url text,
    is_active boolean,
    area_id bigint,
    employer_id text,
    employment_id text,
    experience_id text,
    id bigint NOT NULL,
    title text,
    are_night_shifts boolean,
    role_id bigint,
    published_at timestamp without time zone,
    salary_currency_id text,
    salary_frequency text,
    salary_from double precision,
    salary_could_gross boolean,
    salary_mode text,
    salary_to double precision,
    schedule_id text
);


ALTER TABLE public.hh_vacancies OWNER TO postgres;


CREATE TABLE public.currency (
    id text NOT NULL,
    name text,
    rate double precision,
    code text
);


ALTER TABLE public.currency OWNER TO postgres;


CREATE TABLE public.gj_vacancies (
    id text NOT NULL,
    title text,
    employer text,
    experience text,
    salary_from bigint,
    salary_to bigint,
    salary_currency_id text,
    url text,
    published_at timestamp without time zone,
    is_active boolean
);


ALTER TABLE public.gj_vacancies OWNER TO postgres;

--
-- Name: gm_vacancies; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.gm_vacancies (
    published_at timestamp without time zone,
    is_active boolean,
    title text,
    id bigint NOT NULL,
    salary_from bigint,
    salary_to bigint,
    salary_currency_id text,
    url text,
    english_level text,
    remote_options text,
    office_options text,
    employer text,
    level text,
    experience_years integer
);


ALTER TABLE public.gm_vacancies OWNER TO postgres;


CREATE TABLE public.hh_employers (
    id text NOT NULL,
    name text,
    trusted boolean
);


ALTER TABLE public.hh_employers OWNER TO postgres;


CREATE TABLE public.fn_fields (
    id bigint NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.fn_fields OWNER TO postgres;


CREATE TABLE public.fn_locations (
    id bigint NOT NULL,
    name text NOT NULL,
    country text
);


ALTER TABLE public.fn_locations OWNER TO postgres;

CREATE TABLE public.gj_fields (
    id text NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.gj_fields OWNER TO postgres;

CREATE TABLE public.gj_grades (
    id text NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.gj_grades OWNER TO postgres;

CREATE TABLE public.gj_jobformats (
    id text NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.gj_jobformats OWNER TO postgres;

CREATE TABLE public.gj_levels (
    id text,
    name text
);


ALTER TABLE public.gj_levels OWNER TO postgres;

CREATE TABLE public.gj_locations (
    id text NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.gj_locations OWNER TO postgres;

CREATE TABLE public.gj_skills (
    id text NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.gj_skills OWNER TO postgres;

CREATE TABLE public.gm_skills (
    id bigint NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.gm_skills OWNER TO postgres;

CREATE TABLE public.hh_areas (
    id bigint NOT NULL,
    name text,
    parent_id bigint
);


ALTER TABLE public.hh_areas OWNER TO postgres;

CREATE TABLE public.hh_employment (
    id text NOT NULL,
    name text
);


ALTER TABLE public.hh_employment OWNER TO postgres;

CREATE TABLE public.hh_experience (
    id text NOT NULL,
    name text
);


ALTER TABLE public.hh_experience OWNER TO postgres;


CREATE TABLE public.hh_languages (
    id bigint NOT NULL,
    name text NOT NULL,
    level text NOT NULL
);


ALTER TABLE public.hh_languages OWNER TO postgres;


CREATE TABLE public.hh_professionalroles (
    id bigint NOT NULL,
    name text
);


ALTER TABLE public.hh_professionalroles OWNER TO postgres;


CREATE TABLE public.hh_schedule (
    id text NOT NULL,
    name text
);


ALTER TABLE public.hh_schedule OWNER TO postgres;

CREATE TABLE public.hh_skills (
    id bigint NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.hh_skills OWNER TO postgres;


COPY public.currency (id, name, rate, code) FROM stdin;
AZN	Манаты	0.021244	₼
BYR	Белорусские рубли	0.037183	Br
EUR	Евро	0.010671	€
GEL	Грузинский лари	0.034069	₾
KGS	Кыргызский сом	1.092644	сом
KZT	Тенге	6.723503	₸
RUR	Рубли	1	₽
UAH	Гривны	0.516606	₴
USD	Доллары	0.012497	$
UZS	Узбекский сум	157.230479	so’m
\.
