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

--
-- Name: overview; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA overview;


ALTER SCHEMA overview OWNER TO postgres;

--
-- Name: salary; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA salary;


ALTER SCHEMA salary OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: currency; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.currency (
    id text NOT NULL,
    name text,
    rate double precision,
    code text
);


ALTER TABLE public.currency OWNER TO postgres;

--
-- Name: fn_vacancies; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.fn_vacancies (
    id bigint NOT NULL,
    title text,
    employment_type text,
    salary_from bigint,
    salary_to bigint,
    salary_currency_id text,
    published_at timestamp without time zone,
    url text,
    experience text,
    distant_work boolean,
    employer text,
    address_lat double precision,
    address_lng double precision,
    is_active boolean
);


ALTER TABLE public.fn_vacancies OWNER TO postgres;

--
-- Name: fn; Type: MATERIALIZED VIEW; Schema: salary; Owner: postgres
--

CREATE MATERIALIZED VIEW salary.fn AS
 WITH salary AS (
         SELECT v.id,
                CASE
                    WHEN ((v.salary_from IS NOT NULL) AND (v.salary_to IS NOT NULL)) THEN (((v.salary_from + v.salary_to))::double precision / ((2)::double precision * c.rate))
                    WHEN (v.salary_from IS NOT NULL) THEN ((v.salary_from)::double precision / c.rate)
                    WHEN (v.salary_to IS NOT NULL) THEN ((v.salary_to)::double precision / c.rate)
                    ELSE NULL::double precision
                END AS salary,
                CASE
                    WHEN ((v.salary_from IS NOT NULL) AND (v.salary_to IS NOT NULL)) THEN true
                    ELSE false
                END AS has_range
           FROM (public.fn_vacancies v
             JOIN public.currency c ON ((c.id = v.salary_currency_id)))
          WHERE (((v.salary_from IS NOT NULL) OR (v.salary_to IS NOT NULL)) AND (v.salary_currency_id IS NOT NULL))
        )
 SELECT id,
    has_range,
    salary
   FROM salary s
  WHERE ((salary >= (0)::double precision) AND (salary <= (1000000)::double precision))
  WITH NO DATA;


ALTER MATERIALIZED VIEW salary.fn OWNER TO postgres;

--
-- Name: fn; Type: MATERIALIZED VIEW; Schema: overview; Owner: postgres
--

CREATE MATERIALIZED VIEW overview.fn AS
 SELECT v.id,
    'Finder'::text AS platform,
    v.is_active,
    v.published_at,
    s.salary
   FROM (public.fn_vacancies v
     LEFT JOIN salary.fn s ON ((s.id = v.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW overview.fn OWNER TO postgres;

--
-- Name: gj_vacancies; Type: TABLE; Schema: public; Owner: postgres
--

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
-- Name: gj; Type: MATERIALIZED VIEW; Schema: salary; Owner: postgres
--

CREATE MATERIALIZED VIEW salary.gj AS
 WITH salary AS (
         SELECT v.id,
                CASE
                    WHEN ((v.salary_from IS NOT NULL) AND (v.salary_to IS NOT NULL)) THEN (((v.salary_from + v.salary_to))::double precision / ((2)::double precision * c.rate))
                    WHEN (v.salary_from IS NOT NULL) THEN ((v.salary_from)::double precision / c.rate)
                    WHEN (v.salary_to IS NOT NULL) THEN ((v.salary_to)::double precision / c.rate)
                    ELSE NULL::double precision
                END AS salary,
                CASE
                    WHEN ((v.salary_from IS NOT NULL) AND (v.salary_to IS NOT NULL)) THEN true
                    ELSE false
                END AS has_range
           FROM (public.gj_vacancies v
             JOIN public.currency c ON ((c.id = v.salary_currency_id)))
          WHERE (((v.salary_from IS NOT NULL) OR (v.salary_to IS NOT NULL)) AND (v.salary_currency_id IS NOT NULL))
        )
 SELECT id,
    has_range,
    salary
   FROM salary s
  WHERE ((salary >= (0)::double precision) AND (salary <= (1000000)::double precision))
  WITH NO DATA;


ALTER MATERIALIZED VIEW salary.gj OWNER TO postgres;

--
-- Name: gj; Type: MATERIALIZED VIEW; Schema: overview; Owner: postgres
--

CREATE MATERIALIZED VIEW overview.gj AS
 SELECT v.id,
    'GeekJOB'::text AS platform,
    v.is_active,
    v.published_at,
    s.salary
   FROM (public.gj_vacancies v
     LEFT JOIN salary.gj s ON ((s.id = v.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW overview.gj OWNER TO postgres;

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

--
-- Name: gm; Type: MATERIALIZED VIEW; Schema: salary; Owner: postgres
--

CREATE MATERIALIZED VIEW salary.gm AS
 WITH salary AS (
         SELECT v.id,
                CASE
                    WHEN ((v.salary_from IS NOT NULL) AND (v.salary_to IS NOT NULL)) THEN (((v.salary_from + v.salary_to))::double precision / ((2)::double precision * c.rate))
                    WHEN (v.salary_from IS NOT NULL) THEN ((v.salary_from)::double precision / c.rate)
                    WHEN (v.salary_to IS NOT NULL) THEN ((v.salary_to)::double precision / c.rate)
                    ELSE NULL::double precision
                END AS salary,
                CASE
                    WHEN ((v.salary_from IS NOT NULL) AND (v.salary_to IS NOT NULL)) THEN true
                    ELSE false
                END AS has_range
           FROM (public.gm_vacancies v
             JOIN public.currency c ON ((c.id = v.salary_currency_id)))
          WHERE (((v.salary_from IS NOT NULL) OR (v.salary_to IS NOT NULL)) AND (v.salary_currency_id IS NOT NULL))
        )
 SELECT id,
    has_range,
    salary
   FROM salary s
  WHERE ((salary >= (0)::double precision) AND (salary <= (1000000)::double precision))
  WITH NO DATA;


ALTER MATERIALIZED VIEW salary.gm OWNER TO postgres;

--
-- Name: gm; Type: MATERIALIZED VIEW; Schema: overview; Owner: postgres
--

CREATE MATERIALIZED VIEW overview.gm AS
 SELECT v.id,
    'GetMatch'::text AS platform,
    v.is_active,
    v.published_at,
    s.salary
   FROM (public.gm_vacancies v
     LEFT JOIN salary.gm s ON ((s.id = v.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW overview.gm OWNER TO postgres;

--
-- Name: hh_vacancies; Type: TABLE; Schema: public; Owner: postgres
--

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

--
-- Name: hh; Type: MATERIALIZED VIEW; Schema: salary; Owner: postgres
--

CREATE MATERIALIZED VIEW salary.hh AS
 WITH salary AS (
         SELECT v.id,
                CASE
                    WHEN ((v.salary_from IS NOT NULL) AND (v.salary_to IS NOT NULL)) THEN ((v.salary_from + v.salary_to) / ((2)::double precision * c.rate))
                    WHEN (v.salary_from IS NOT NULL) THEN (v.salary_from / c.rate)
                    WHEN (v.salary_to IS NOT NULL) THEN (v.salary_to / c.rate)
                    ELSE NULL::double precision
                END AS salary,
                CASE
                    WHEN ((v.salary_from IS NOT NULL) AND (v.salary_to IS NOT NULL)) THEN true
                    ELSE false
                END AS has_range
           FROM (public.hh_vacancies v
             JOIN public.currency c ON ((c.id = v.salary_currency_id)))
          WHERE (((v.salary_from IS NOT NULL) OR (v.salary_to IS NOT NULL)) AND (v.salary_currency_id IS NOT NULL))
        )
 SELECT id,
    has_range,
    salary
   FROM salary s
  WHERE ((salary >= (0)::double precision) AND (salary <= (1000000)::double precision))
  WITH NO DATA;


ALTER MATERIALIZED VIEW salary.hh OWNER TO postgres;

--
-- Name: hh; Type: MATERIALIZED VIEW; Schema: overview; Owner: postgres
--

CREATE MATERIALIZED VIEW overview.hh AS
 SELECT v.id,
    'HeadHunter'::text AS platform,
    v.is_active,
    v.published_at,
    s.salary
   FROM (public.hh_vacancies v
     LEFT JOIN salary.hh s ON ((s.id = v.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW overview.hh OWNER TO postgres;

--
-- Name: hh_employers; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hh_employers (
    id text NOT NULL,
    name text,
    trusted boolean
);


ALTER TABLE public.hh_employers OWNER TO postgres;

--
-- Name: employers; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.employers AS
 SELECT (o.id)::text AS id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    v.employer
   FROM (overview.fn o
     JOIN public.fn_vacancies v ON ((v.id = o.id)))
UNION ALL
 SELECT o.id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    v.employer
   FROM (overview.gj o
     JOIN public.gj_vacancies v ON ((v.id = o.id)))
UNION ALL
 SELECT (o.id)::text AS id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    v.employer
   FROM (overview.gm o
     JOIN public.gm_vacancies v ON ((v.id = o.id)))
UNION ALL
 SELECT (o.id)::text AS id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    e.name AS employer
   FROM ((overview.hh o
     JOIN public.hh_vacancies v ON ((v.id = o.id)))
     JOIN public.hh_employers e ON ((e.id = v.employer_id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.employers OWNER TO postgres;

--
-- Name: fn_fields; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.fn_fields (
    id bigint NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.fn_fields OWNER TO postgres;

--
-- Name: fn_locations; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.fn_locations (
    id bigint NOT NULL,
    name text NOT NULL,
    country text
);


ALTER TABLE public.fn_locations OWNER TO postgres;

--
-- Name: gj_fields; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.gj_fields (
    id text NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.gj_fields OWNER TO postgres;

--
-- Name: gj_grades; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.gj_grades (
    id text NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.gj_grades OWNER TO postgres;

--
-- Name: gj_jobformats; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.gj_jobformats (
    id text NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.gj_jobformats OWNER TO postgres;

--
-- Name: gj_locations; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.gj_locations (
    id text NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.gj_locations OWNER TO postgres;

--
-- Name: gj_skills; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.gj_skills (
    id text NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.gj_skills OWNER TO postgres;

--
-- Name: gm_skills; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.gm_skills (
    id bigint NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.gm_skills OWNER TO postgres;

--
-- Name: hh_experience; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hh_experience (
    id text NOT NULL,
    name text
);


ALTER TABLE public.hh_experience OWNER TO postgres;

--
-- Name: grades; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.grades AS
 SELECT (o.id)::text AS id,
    o.is_active,
    o.platform,
    o.published_at,
    o.salary,
        CASE
            WHEN (v.experience = 'three_years_more'::text) THEN 'Более 3 лет'::text
            WHEN (v.experience = 'no_experience'::text) THEN 'Нет опыта'::text
            WHEN (v.experience = 'up_to_one_year'::text) THEN 'Менее 1 года'::text
            WHEN (v.experience = 'two_years_more'::text) THEN 'Более 2 лет'::text
            WHEN (v.experience = 'five_years_more'::text) THEN 'Более 5 лет'::text
            WHEN (v.experience = 'year_minimum'::text) THEN 'От 1 года'::text
            ELSE 'Не указан'::text
        END AS experience,
        CASE
            WHEN (lower(v.title) ~ '.*(intern|стаж).*'::text) THEN 'Стажер'::text
            WHEN (lower(v.title) ~ '.*(jun|джун).*'::text) THEN 'Джуниор'::text
            WHEN (lower(v.title) ~ '.*(midd|мидд).*'::text) THEN 'Миддл'::text
            WHEN (lower(v.title) ~ '.*(sen|сеньор).*'::text) THEN 'Сеньор'::text
            ELSE 'Не указано'::text
        END AS grade
   FROM (overview.fn o
     JOIN public.fn_vacancies v ON ((v.id = o.id)))
UNION ALL
 SELECT o.id,
    o.is_active,
    o.platform,
    o.published_at,
    o.salary,
    COALESCE(v.experience, 'Не указан'::text) AS experience,
    g.name AS grade
   FROM ((overview.gj o
     JOIN public.gj_vacancies v ON ((v.id = o.id)))
     LEFT JOIN public.gj_grades g ON ((g.id = o.id)))
UNION ALL
 SELECT (o.id)::text AS id,
    o.is_active,
    o.platform,
    o.published_at,
    o.salary,
        CASE
            WHEN (v.experience_years < 1) THEN 'Нет опыта'::text
            WHEN ((v.experience_years >= 1) AND (v.experience_years <= 3)) THEN 'От 1 года до 3 лет'::text
            WHEN ((v.experience_years >= 3) AND (v.experience_years <= 6)) THEN 'От 3 до 6 лет'::text
            WHEN (v.experience_years > 6) THEN 'Более 6 лет'::text
            ELSE 'Не указан'::text
        END AS experience,
        CASE
            WHEN (v.level = 'Senior'::text) THEN 'Сеньор'::text
            WHEN (v.level = 'Lead'::text) THEN 'Тимлид/Руководитель группы'::text
            WHEN (v.level = 'Middle'::text) THEN 'Миддл'::text
            WHEN (v.level = 'Middle-to-Senior'::text) THEN 'Миддл'::text
            WHEN (v.level = 'C-level'::text) THEN 'Руководитель отдела/подразделения'::text
            WHEN (v.level = 'Junior'::text) THEN 'Джуниор'::text
            ELSE 'Не указано'::text
        END AS grade
   FROM (overview.gm o
     JOIN public.gm_vacancies v ON ((v.id = o.id)))
UNION ALL
 SELECT (o.id)::text AS id,
    o.is_active,
    o.platform,
    o.published_at,
    o.salary,
    COALESCE(exp.name, 'Не указан'::text) AS experience,
        CASE
            WHEN (lower(v.title) ~ '.*(intern|стаж).*'::text) THEN 'Стажер'::text
            WHEN (lower(v.title) ~ '.*(jun|джун).*'::text) THEN 'Джуниор'::text
            WHEN (lower(v.title) ~ '.*(midd|мидд).*'::text) THEN 'Миддл'::text
            WHEN (lower(v.title) ~ '.*(sen|сеньор).*'::text) THEN 'Сеньор'::text
            ELSE 'Не указано'::text
        END AS grade
   FROM ((overview.hh o
     JOIN public.hh_vacancies v ON ((v.id = o.id)))
     LEFT JOIN public.hh_experience exp ON ((exp.id = v.experience_id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.grades OWNER TO postgres;

--
-- Name: hh_areas; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hh_areas (
    id bigint NOT NULL,
    name text,
    parent_id bigint
);


ALTER TABLE public.hh_areas OWNER TO postgres;

--
-- Name: hh_employment; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hh_employment (
    id text NOT NULL,
    name text
);


ALTER TABLE public.hh_employment OWNER TO postgres;

--
-- Name: hh_languages; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hh_languages (
    id bigint NOT NULL,
    name text NOT NULL,
    level text NOT NULL
);


ALTER TABLE public.hh_languages OWNER TO postgres;

--
-- Name: hh_professionalroles; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hh_professionalroles (
    id bigint NOT NULL,
    name text
);


ALTER TABLE public.hh_professionalroles OWNER TO postgres;

--
-- Name: hh_schedule; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hh_schedule (
    id text NOT NULL,
    name text
);


ALTER TABLE public.hh_schedule OWNER TO postgres;

--
-- Name: hh_skills; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hh_skills (
    id bigint NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.hh_skills OWNER TO postgres;

--
-- Name: languages; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.languages AS
 SELECT (o.id)::text AS id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
        CASE
            WHEN (v.english_level IS NOT NULL) THEN 'Английский'::text
            ELSE 'Не указан'::text
        END AS language,
        CASE
            WHEN (v.english_level ~~ '%A1%'::text) THEN 'A1 — Начальный'::text
            WHEN (v.english_level ~~ '%A2%'::text) THEN 'A2 — Элементарный'::text
            WHEN (v.english_level ~~ '%B1%'::text) THEN 'B1 — Средний'::text
            WHEN (v.english_level ~~ '%B2%'::text) THEN 'B2 — Средне-продвинутый'::text
            WHEN (v.english_level ~~ '%C1%'::text) THEN 'C1 — Продвинутый'::text
            ELSE 'Не указан'::text
        END AS level
   FROM (overview.gm o
     JOIN public.gm_vacancies v ON ((v.id = o.id)))
UNION ALL
 SELECT (o.id)::text AS id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    COALESCE(l.name, 'Не указан'::text) AS language,
    COALESCE(l.level, 'Не указан'::text) AS level
   FROM ((overview.hh o
     JOIN public.hh_vacancies v ON ((v.id = o.id)))
     LEFT JOIN public.hh_languages l ON ((l.id = o.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.languages OWNER TO postgres;

--
-- Name: overview; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.overview AS
 SELECT (o.id)::text AS id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    s.has_range
   FROM (overview.fn o
     LEFT JOIN salary.fn s ON ((s.id = o.id)))
UNION ALL
 SELECT o.id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    s.has_range
   FROM (overview.gj o
     LEFT JOIN salary.gj s ON ((s.id = o.id)))
UNION ALL
 SELECT (o.id)::text AS id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    s.has_range
   FROM (overview.gm o
     LEFT JOIN salary.gm s ON ((s.id = o.id)))
UNION ALL
 SELECT (o.id)::text AS id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    s.has_range
   FROM (overview.hh o
     LEFT JOIN salary.hh s ON ((s.id = o.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.overview OWNER TO postgres;

--
-- Name: places; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.places AS
 SELECT (o.id)::text AS id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    v.title,
    v.url,
    v.address_lat AS lat,
    v.address_lng AS lng,
    l.name AS region,
    l.country
   FROM ((overview.fn o
     JOIN public.fn_vacancies v ON ((v.id = o.id)))
     LEFT JOIN public.fn_locations l ON ((l.id = o.id)))
UNION ALL
 SELECT o.id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    v.title,
    v.url,
    NULL::double precision AS lat,
    NULL::double precision AS lng,
    l.name AS region,
    NULL::text AS country
   FROM ((overview.gj o
     JOIN public.gj_vacancies v ON ((v.id = o.id)))
     LEFT JOIN public.gj_locations l ON ((l.id = o.id)))
UNION ALL
 SELECT (o.id)::text AS id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    v.title,
    v.url,
    v.address_lat AS lat,
    v.address_lng AS lng,
    a1.name AS region,
    a2.name AS country
   FROM (((overview.hh o
     JOIN public.hh_vacancies v ON ((v.id = o.id)))
     LEFT JOIN public.hh_areas a1 ON ((a1.id = o.id)))
     LEFT JOIN public.hh_areas a2 ON ((a2.id = a1.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.places OWNER TO postgres;

--
-- Name: skills; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.skills AS
 SELECT o.id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    sk.name AS skill
   FROM (overview.gj o
     JOIN public.gj_skills sk ON ((sk.id = o.id)))
UNION ALL
 SELECT (o.id)::text AS id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    sk.name AS skill
   FROM ((overview.gm o
     LEFT JOIN salary.gm s ON ((s.id = o.id)))
     JOIN public.gm_skills sk ON ((sk.id = o.id)))
UNION ALL
 SELECT (o.id)::text AS id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    sk.name AS skill
   FROM (overview.hh o
     JOIN public.hh_skills sk ON ((sk.id = o.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.skills OWNER TO postgres;

--
-- Name: skills_pair; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.skills_pair AS
 SELECT a.id,
    a.platform,
    a.is_active,
    a.published_at,
    a.skill,
    b.skill AS skill2
   FROM (public.skills a
     JOIN public.skills b ON (((a.id = b.id) AND (a.skill < b.skill))))
  WHERE (a.skill <> b.skill)
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.skills_pair OWNER TO postgres;

--
-- Name: vacancies; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.vacancies AS
 SELECT (o.id)::text AS id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    v.title,
    v.url
   FROM (overview.fn o
     JOIN public.fn_vacancies v ON ((v.id = o.id)))
UNION
 SELECT o.id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    v.title,
    v.url
   FROM (overview.gj o
     JOIN public.gj_vacancies v ON ((v.id = o.id)))
UNION
 SELECT (o.id)::text AS id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    v.title,
    v.url
   FROM (overview.gm o
     JOIN public.gm_vacancies v ON ((v.id = o.id)))
UNION
 SELECT (o.id)::text AS id,
    o.platform,
    o.is_active,
    o.published_at,
    o.salary,
    v.title,
    v.url
   FROM (overview.hh o
     JOIN public.hh_vacancies v ON ((v.id = o.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.vacancies OWNER TO postgres;

--
-- Data for Name: currency; Type: TABLE DATA; Schema: public; Owner: postgres
--

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

--
-- Name: currency currency_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.currency
    ADD CONSTRAINT currency_pkey PRIMARY KEY (id);


--
-- Name: fn_fields fn_fields_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fn_fields
    ADD CONSTRAINT fn_fields_pkey PRIMARY KEY (id, name);


--
-- Name: fn_locations fn_locations_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fn_locations
    ADD CONSTRAINT fn_locations_pkey PRIMARY KEY (id, name);


--
-- Name: fn_vacancies fn_vacancies_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fn_vacancies
    ADD CONSTRAINT fn_vacancies_pkey PRIMARY KEY (id);


--
-- Name: gj_fields gj_fields_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.gj_fields
    ADD CONSTRAINT gj_fields_pkey PRIMARY KEY (id, name);


--
-- Name: gj_jobformats gj_jobformats_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.gj_jobformats
    ADD CONSTRAINT gj_jobformats_pkey PRIMARY KEY (id, name);


--
-- Name: gj_grades gj_levels_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.gj_grades
    ADD CONSTRAINT gj_levels_pkey PRIMARY KEY (id, name);


--
-- Name: gj_locations gj_locations_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.gj_locations
    ADD CONSTRAINT gj_locations_pkey PRIMARY KEY (id, name);


--
-- Name: gj_skills gj_skills_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.gj_skills
    ADD CONSTRAINT gj_skills_pkey PRIMARY KEY (id, name);


--
-- Name: gj_vacancies gj_vacancies_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.gj_vacancies
    ADD CONSTRAINT gj_vacancies_pkey PRIMARY KEY (id);


--
-- Name: gm_skills gm_skills_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.gm_skills
    ADD CONSTRAINT gm_skills_pkey PRIMARY KEY (id, name);


--
-- Name: gm_vacancies gm_vacancies_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.gm_vacancies
    ADD CONSTRAINT gm_vacancies_pkey PRIMARY KEY (id);


--
-- Name: hh_areas hh_areas_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_areas
    ADD CONSTRAINT hh_areas_pkey PRIMARY KEY (id);


--
-- Name: hh_employers hh_employers_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_employers
    ADD CONSTRAINT hh_employers_pkey PRIMARY KEY (id);


--
-- Name: hh_employment hh_employment_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_employment
    ADD CONSTRAINT hh_employment_pkey PRIMARY KEY (id);


--
-- Name: hh_experience hh_experience_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_experience
    ADD CONSTRAINT hh_experience_pkey PRIMARY KEY (id);


--
-- Name: hh_languages hh_languages_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_languages
    ADD CONSTRAINT hh_languages_pkey PRIMARY KEY (id, name, level);


--
-- Name: hh_professionalroles hh_professionalroles_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_professionalroles
    ADD CONSTRAINT hh_professionalroles_pkey PRIMARY KEY (id);


--
-- Name: hh_schedule hh_schedule_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_schedule
    ADD CONSTRAINT hh_schedule_pkey PRIMARY KEY (id);


--
-- Name: hh_skills hh_skills_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_skills
    ADD CONSTRAINT hh_skills_pkey PRIMARY KEY (id, name);


--
-- Name: hh_vacancies hh_vacancies_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_vacancies
    ADD CONSTRAINT hh_vacancies_pkey PRIMARY KEY (id);


--
-- Name: hh_vacancies fk_area_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_vacancies
    ADD CONSTRAINT fk_area_id FOREIGN KEY (area_id) REFERENCES public.hh_areas(id);


--
-- Name: gj_vacancies fk_currency_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.gj_vacancies
    ADD CONSTRAINT fk_currency_id FOREIGN KEY (salary_currency_id) REFERENCES public.currency(id);


--
-- Name: hh_vacancies fk_currency_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_vacancies
    ADD CONSTRAINT fk_currency_id FOREIGN KEY (salary_currency_id) REFERENCES public.currency(id);


--
-- Name: gm_vacancies fk_currency_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.gm_vacancies
    ADD CONSTRAINT fk_currency_id FOREIGN KEY (salary_currency_id) REFERENCES public.currency(id);


--
-- Name: fn_vacancies fk_currency_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fn_vacancies
    ADD CONSTRAINT fk_currency_id FOREIGN KEY (salary_currency_id) REFERENCES public.currency(id);


--
-- Name: hh_vacancies fk_employer_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_vacancies
    ADD CONSTRAINT fk_employer_id FOREIGN KEY (employer_id) REFERENCES public.hh_employers(id);


--
-- Name: hh_vacancies fk_employment_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_vacancies
    ADD CONSTRAINT fk_employment_id FOREIGN KEY (employment_id) REFERENCES public.hh_employment(id);


--
-- Name: hh_vacancies fk_experience_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_vacancies
    ADD CONSTRAINT fk_experience_id FOREIGN KEY (experience_id) REFERENCES public.hh_experience(id);


--
-- Name: hh_vacancies fk_profrole_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_vacancies
    ADD CONSTRAINT fk_profrole_id FOREIGN KEY (role_id) REFERENCES public.hh_professionalroles(id);


--
-- Name: hh_vacancies fk_schedule_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_vacancies
    ADD CONSTRAINT fk_schedule_id FOREIGN KEY (schedule_id) REFERENCES public.hh_schedule(id);


--
-- Name: hh_skills fk_vacancy_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_skills
    ADD CONSTRAINT fk_vacancy_id FOREIGN KEY (id) REFERENCES public.hh_vacancies(id);


--
-- Name: hh_languages fk_vacancy_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hh_languages
    ADD CONSTRAINT fk_vacancy_id FOREIGN KEY (id) REFERENCES public.hh_vacancies(id);


--
-- Name: gm_skills fk_vacancy_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.gm_skills
    ADD CONSTRAINT fk_vacancy_id FOREIGN KEY (id) REFERENCES public.gm_vacancies(id);


--
-- Name: gj_skills fk_vacancy_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.gj_skills
    ADD CONSTRAINT fk_vacancy_id FOREIGN KEY (id) REFERENCES public.gj_vacancies(id);


--
-- Name: gj_locations fk_vacancy_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.gj_locations
    ADD CONSTRAINT fk_vacancy_id FOREIGN KEY (id) REFERENCES public.gj_vacancies(id);


--
-- Name: gj_grades fk_vacancy_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.gj_grades
    ADD CONSTRAINT fk_vacancy_id FOREIGN KEY (id) REFERENCES public.gj_vacancies(id);


--
-- Name: gj_jobformats fk_vacancy_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.gj_jobformats
    ADD CONSTRAINT fk_vacancy_id FOREIGN KEY (id) REFERENCES public.gj_vacancies(id);


--
-- Name: gj_fields fk_vacancy_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.gj_fields
    ADD CONSTRAINT fk_vacancy_id FOREIGN KEY (id) REFERENCES public.gj_vacancies(id);


--
-- Name: fn_locations fk_vacancy_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fn_locations
    ADD CONSTRAINT fk_vacancy_id FOREIGN KEY (id) REFERENCES public.fn_vacancies(id);


--
-- Name: fn_fields fk_vacancy_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fn_fields
    ADD CONSTRAINT fk_vacancy_id FOREIGN KEY (id) REFERENCES public.fn_vacancies(id);


--
-- Name: fn; Type: MATERIALIZED VIEW DATA; Schema: salary; Owner: postgres
--

REFRESH MATERIALIZED VIEW salary.fn;


--
-- Name: fn; Type: MATERIALIZED VIEW DATA; Schema: overview; Owner: postgres
--

REFRESH MATERIALIZED VIEW overview.fn;


--
-- Name: gj; Type: MATERIALIZED VIEW DATA; Schema: salary; Owner: postgres
--

REFRESH MATERIALIZED VIEW salary.gj;


--
-- Name: gj; Type: MATERIALIZED VIEW DATA; Schema: overview; Owner: postgres
--

REFRESH MATERIALIZED VIEW overview.gj;


--
-- Name: gm; Type: MATERIALIZED VIEW DATA; Schema: salary; Owner: postgres
--

REFRESH MATERIALIZED VIEW salary.gm;


--
-- Name: gm; Type: MATERIALIZED VIEW DATA; Schema: overview; Owner: postgres
--

REFRESH MATERIALIZED VIEW overview.gm;


--
-- Name: hh; Type: MATERIALIZED VIEW DATA; Schema: salary; Owner: postgres
--

REFRESH MATERIALIZED VIEW salary.hh;


--
-- Name: hh; Type: MATERIALIZED VIEW DATA; Schema: overview; Owner: postgres
--

REFRESH MATERIALIZED VIEW overview.hh;


--
-- Name: employers; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: postgres
--

REFRESH MATERIALIZED VIEW public.employers;


--
-- Name: grades; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: postgres
--

REFRESH MATERIALIZED VIEW public.grades;


--
-- Name: languages; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: postgres
--

REFRESH MATERIALIZED VIEW public.languages;


--
-- Name: overview; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: postgres
--

REFRESH MATERIALIZED VIEW public.overview;


--
-- Name: places; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: postgres
--

REFRESH MATERIALIZED VIEW public.places;


--
-- Name: skills; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: postgres
--

REFRESH MATERIALIZED VIEW public.skills;


--
-- Name: skills_pair; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: postgres
--

REFRESH MATERIALIZED VIEW public.skills_pair;


--
-- Name: vacancies; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: postgres
--

REFRESH MATERIALIZED VIEW public.vacancies;


--
-- PostgreSQL database dump complete
--