--
-- PostgreSQL database dump
--

\restrict XaibDDiuDEtQhc1hlDsVJT1no0CBVbLJ22QCXKd7a7FLrmLgDdJqcqzd268rz4r

-- Dumped from database version 17.6 (Debian 17.6-1.pgdg13+1)
-- Dumped by pg_dump version 17.6 (Debian 17.6-1.pgdg13+1)

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
-- Name: output; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA output;


ALTER SCHEMA output OWNER TO postgres;

--
-- Name: private; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA private;


ALTER SCHEMA private OWNER TO postgres;

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
-- Name: hc_vacancies; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hc_vacancies (
    id bigint NOT NULL,
    title text,
    remote_work boolean,
    grade text,
    published_at timestamp with time zone,
    employer text,
    employment_type text,
    salary_from bigint,
    salary_to bigint,
    salary_currency_id text,
    is_active boolean,
    url text
);


ALTER TABLE public.hc_vacancies OWNER TO postgres;

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
  WHERE ((salary >= (1000)::double precision) AND (salary <= (1000000)::double precision))
  WITH NO DATA;


ALTER MATERIALIZED VIEW salary.fn OWNER TO postgres;

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
  WHERE ((salary >= (1000)::double precision) AND (salary <= (1000000)::double precision))
  WITH NO DATA;


ALTER MATERIALIZED VIEW salary.gj OWNER TO postgres;

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
  WHERE ((salary >= (1000)::double precision) AND (salary <= (1000000)::double precision))
  WITH NO DATA;


ALTER MATERIALIZED VIEW salary.gm OWNER TO postgres;

--
-- Name: hc; Type: MATERIALIZED VIEW; Schema: salary; Owner: postgres
--

CREATE MATERIALIZED VIEW salary.hc AS
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
           FROM (public.hc_vacancies v
             JOIN public.currency c ON ((c.id = v.salary_currency_id)))
          WHERE (((v.salary_from IS NOT NULL) OR (v.salary_to IS NOT NULL)) AND (v.salary_currency_id IS NOT NULL))
        )
 SELECT id,
    has_range,
    salary
   FROM salary s
  WHERE ((salary >= (1000)::double precision) AND (salary <= (1000000)::double precision))
  WITH NO DATA;


ALTER MATERIALIZED VIEW salary.hc OWNER TO postgres;

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
  WHERE ((salary >= (1000)::double precision) AND (salary <= (1000000)::double precision))
  WITH NO DATA;


ALTER MATERIALIZED VIEW salary.hh OWNER TO postgres;

--
-- Name: overview; Type: MATERIALIZED VIEW; Schema: output; Owner: postgres
--

CREATE MATERIALIZED VIEW output.overview AS
 SELECT 'fn'::text AS source,
    count(*) AS total,
    count(
        CASE
            WHEN (v.is_active IS TRUE) THEN 1
            ELSE NULL::integer
        END) AS active,
    count(
        CASE
            WHEN (s.salary IS NOT NULL) THEN 1
            ELSE NULL::integer
        END) AS with_salary,
    count(
        CASE
            WHEN (s.has_range IS TRUE) THEN 1
            ELSE NULL::integer
        END) AS with_range,
    round(avg(s.salary)) AS salary
   FROM (public.fn_vacancies v
     LEFT JOIN salary.fn s ON ((s.id = v.id)))
UNION ALL
 SELECT 'gj'::text AS source,
    count(*) AS total,
    count(
        CASE
            WHEN (v.is_active IS TRUE) THEN 1
            ELSE NULL::integer
        END) AS active,
    count(
        CASE
            WHEN (s.salary IS NOT NULL) THEN 1
            ELSE NULL::integer
        END) AS with_salary,
    count(
        CASE
            WHEN (s.has_range IS TRUE) THEN 1
            ELSE NULL::integer
        END) AS with_range,
    round(avg(s.salary)) AS salary
   FROM (public.gj_vacancies v
     LEFT JOIN salary.gj s ON ((s.id = v.id)))
UNION ALL
 SELECT 'gm'::text AS source,
    count(*) AS total,
    count(
        CASE
            WHEN (v.is_active IS TRUE) THEN 1
            ELSE NULL::integer
        END) AS active,
    count(
        CASE
            WHEN (s.salary IS NOT NULL) THEN 1
            ELSE NULL::integer
        END) AS with_salary,
    count(
        CASE
            WHEN (s.has_range IS TRUE) THEN 1
            ELSE NULL::integer
        END) AS with_range,
    round(avg(s.salary)) AS salary
   FROM (public.gm_vacancies v
     LEFT JOIN salary.gm s ON ((s.id = v.id)))
UNION ALL
 SELECT 'hc'::text AS source,
    count(*) AS total,
    count(
        CASE
            WHEN (v.is_active IS TRUE) THEN 1
            ELSE NULL::integer
        END) AS active,
    count(
        CASE
            WHEN (s.salary IS NOT NULL) THEN 1
            ELSE NULL::integer
        END) AS with_salary,
    count(
        CASE
            WHEN (s.has_range IS TRUE) THEN 1
            ELSE NULL::integer
        END) AS with_range,
    round(avg(s.salary)) AS salary
   FROM (public.hc_vacancies v
     LEFT JOIN salary.hc s ON ((s.id = v.id)))
UNION ALL
 SELECT 'hh'::text AS source,
    count(*) AS total,
    count(
        CASE
            WHEN (v.is_active IS TRUE) THEN 1
            ELSE NULL::integer
        END) AS active,
    count(
        CASE
            WHEN (s.salary IS NOT NULL) THEN 1
            ELSE NULL::integer
        END) AS with_salary,
    count(
        CASE
            WHEN (s.has_range IS TRUE) THEN 1
            ELSE NULL::integer
        END) AS with_range,
    round(avg(s.salary)) AS salary
   FROM (public.hh_vacancies v
     LEFT JOIN salary.hh s ON ((s.id = v.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW output.overview OWNER TO postgres;

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
-- Name: hc_skills; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hc_skills (
    id bigint NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.hc_skills OWNER TO postgres;

--
-- Name: hh_skills; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hh_skills (
    id bigint NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.hh_skills OWNER TO postgres;

--
-- Name: skills_view; Type: VIEW; Schema: private; Owner: postgres
--

CREATE VIEW private.skills_view AS
 SELECT concat('gj', v.id) AS id,
    sk.name AS skill,
    s.salary
   FROM ((public.gj_vacancies v
     JOIN public.gj_skills sk ON ((sk.id = v.id)))
     LEFT JOIN salary.gj s ON ((s.id = v.id)))
UNION ALL
 SELECT concat('gm', v.id) AS id,
    sk.name AS skill,
    s.salary
   FROM ((public.gm_vacancies v
     JOIN public.gm_skills sk ON ((sk.id = v.id)))
     LEFT JOIN salary.gm s ON ((s.id = v.id)))
UNION ALL
 SELECT concat('hc', v.id) AS id,
    sk.name AS skill,
    s.salary
   FROM ((public.hc_vacancies v
     JOIN public.hc_skills sk ON ((sk.id = v.id)))
     LEFT JOIN salary.hc s ON ((s.id = v.id)))
UNION ALL
 SELECT concat('hh', v.id) AS id,
    sk.name AS skill,
    s.salary
   FROM ((public.hh_vacancies v
     JOIN public.hh_skills sk ON ((sk.id = v.id)))
     LEFT JOIN salary.hh s ON ((s.id = v.id)));


ALTER VIEW private.skills_view OWNER TO postgres;

--
-- Name: skills; Type: MATERIALIZED VIEW; Schema: output; Owner: postgres
--

CREATE MATERIALIZED VIEW output.skills AS
 SELECT skill,
    count(*) AS total,
    round(avg(salary)) AS salary
   FROM private.skills_view s
  GROUP BY skill
  ORDER BY (count(*)) DESC
 LIMIT 100
  WITH NO DATA;


ALTER MATERIALIZED VIEW output.skills OWNER TO postgres;

--
-- Name: skills_pairs; Type: MATERIALIZED VIEW; Schema: output; Owner: postgres
--

CREATE MATERIALIZED VIEW output.skills_pairs AS
 SELECT a.skill AS skill1,
    b.skill AS skill2,
    count(*) AS total,
    round(avg(a.salary)) AS salary
   FROM (private.skills_view a
     JOIN private.skills_view b ON (((a.id = b.id) AND (a.skill < b.skill))))
  WHERE (a.skill <> b.skill)
  GROUP BY a.skill, b.skill
  ORDER BY (count(*)) DESC
 LIMIT 100
  WITH NO DATA;


ALTER MATERIALIZED VIEW output.skills_pairs OWNER TO postgres;

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
-- Name: hc_fields; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hc_fields (
    id bigint NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.hc_fields OWNER TO postgres;

--
-- Name: hc_locations; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hc_locations (
    id bigint NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.hc_locations OWNER TO postgres;

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
-- Name: hh_employers; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hh_employers (
    id text NOT NULL,
    name text,
    trusted boolean
);


ALTER TABLE public.hh_employers OWNER TO postgres;

--
-- Name: hh_employment; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hh_employment (
    id text NOT NULL,
    name text
);


ALTER TABLE public.hh_employment OWNER TO postgres;

--
-- Name: hh_experience; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hh_experience (
    id text NOT NULL,
    name text
);


ALTER TABLE public.hh_experience OWNER TO postgres;

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
-- Name: hc_fields hc_fields_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hc_fields
    ADD CONSTRAINT hc_fields_pkey PRIMARY KEY (id, name);


--
-- Name: hc_locations hc_locations_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hc_locations
    ADD CONSTRAINT hc_locations_pkey PRIMARY KEY (id, name);


--
-- Name: hc_skills hc_skills_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hc_skills
    ADD CONSTRAINT hc_skills_pkey PRIMARY KEY (id, name);


--
-- Name: hc_vacancies hc_vacancies_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hc_vacancies
    ADD CONSTRAINT hc_vacancies_pkey PRIMARY KEY (id);


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
-- Name: hc_vacancies fk_currency_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hc_vacancies
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
-- Name: hc_locations fk_vacancy_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hc_locations
    ADD CONSTRAINT fk_vacancy_id FOREIGN KEY (id) REFERENCES public.hc_vacancies(id);


--
-- Name: hc_skills fk_vacancy_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hc_skills
    ADD CONSTRAINT fk_vacancy_id FOREIGN KEY (id) REFERENCES public.hc_vacancies(id);


--
-- Name: hc_fields fk_vacancy_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hc_fields
    ADD CONSTRAINT fk_vacancy_id FOREIGN KEY (id) REFERENCES public.hc_vacancies(id);


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
-- PostgreSQL database dump complete
--

\unrestrict XaibDDiuDEtQhc1hlDsVJT1no0CBVbLJ22QCXKd7a7FLrmLgDdJqcqzd268rz4r

