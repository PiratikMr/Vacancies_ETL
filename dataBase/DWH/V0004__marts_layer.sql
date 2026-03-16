CREATE SCHEMA IF NOT EXISTS marts;

CREATE MATERIALIZED VIEW marts.dict_platform AS
SELECT DISTINCT platform
FROM internal.mv_core_vacancy
WHERE platform IS NOT NULL;

CREATE MATERIALIZED VIEW marts.dict_employer AS
SELECT DISTINCT employer
FROM internal.mv_core_vacancy
WHERE employer IS NOT NULL;

CREATE MATERIALIZED VIEW marts.dict_currency AS
SELECT DISTINCT currency
FROM internal.mv_core_vacancy
WHERE currency IS NOT NULL;

CREATE MATERIALIZED VIEW marts.dict_experience AS
SELECT DISTINCT experience
FROM internal.mv_core_vacancy
WHERE experience IS NOT NULL;

CREATE MATERIALIZED VIEW marts.dict_skills AS
SELECT DISTINCT unnest(skills) AS skill
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.dict_schedules AS
SELECT DISTINCT unnest(schedules) AS schedule
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.dict_locations AS
SELECT DISTINCT unnest(locations) AS location
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.dict_countries AS
SELECT DISTINCT unnest(countries) AS country
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.dict_fields AS
SELECT DISTINCT unnest(fields) AS field
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.dict_grades AS
SELECT DISTINCT unnest(grades) AS grade
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.dict_employments AS
SELECT DISTINCT unnest(employments) AS employment
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.dict_languages AS
SELECT DISTINCT unnest(languages) AS language
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.dict_language_levels AS
SELECT DISTINCT unnest(language_levels) AS language_level
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.mv_vacancy_schedule AS
SELECT 
    f.vacancy_id,
    f.published_at,
    d.schedule
FROM fact_vacancy f
JOIN bridge_vacancy_schedule b ON f.vacancy_id = b.vacancy_id
JOIN dim_schedule d ON b.schedule_id = d.schedule_id;

CREATE MATERIALIZED VIEW marts.mv_vacancy_field AS
SELECT 
    b.vacancy_id,
    d.field
FROM bridge_vacancy_field b
JOIN dim_field d ON b.field_id = d.field_id;

CREATE MATERIALIZED VIEW marts.mv_vacancy_employment_experience AS
SELECT 
    f.vacancy_id,
    emp.employment,
    e.experience
FROM fact_vacancy f
JOIN bridge_vacancy_employment b ON f.vacancy_id = b.vacancy_id
JOIN dim_employment emp ON b.employment_id = emp.employment_id
JOIN dim_experience e ON f.experience_id = e.experience_id;