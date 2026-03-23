CREATE SCHEMA IF NOT EXISTS marts;

CREATE MATERIALIZED VIEW marts.dict_platform AS
SELECT DISTINCT platform AS filter_platform
FROM internal.mv_core_vacancy
WHERE platform IS NOT NULL;

CREATE MATERIALIZED VIEW marts.dict_employer AS
SELECT DISTINCT employer AS filter_employer
FROM internal.mv_core_vacancy
WHERE employer IS NOT NULL;

CREATE MATERIALIZED VIEW marts.dict_currency AS
SELECT DISTINCT currency AS filter_currency
FROM internal.mv_core_vacancy
WHERE currency IS NOT NULL;

CREATE MATERIALIZED VIEW marts.dict_experience AS
SELECT DISTINCT experience AS filter_experience
FROM internal.mv_core_vacancy
WHERE experience IS NOT NULL;

CREATE MATERIALIZED VIEW marts.dict_skills AS
SELECT DISTINCT unnest(skills) AS filter_skill
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.dict_schedules AS
SELECT DISTINCT unnest(schedules) AS filter_schedule
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.dict_locations AS
SELECT DISTINCT unnest(locations) AS filter_location
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.dict_countries AS
SELECT DISTINCT unnest(countries) AS filter_country
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.dict_fields AS
SELECT DISTINCT unnest(fields) AS filter_field
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.dict_grades AS
SELECT DISTINCT unnest(grades) AS filter_grade
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.dict_employments AS
SELECT DISTINCT unnest(employments) AS filter_employment
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.dict_languages AS
SELECT DISTINCT unnest(languages) AS filter_language
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.dict_language_levels AS
SELECT DISTINCT unnest(language_levels) AS filter_language_level
FROM internal.mv_core_vacancy;

CREATE MATERIALIZED VIEW marts.mv_vacancy_field_salary AS
SELECT 
    v.vacancy_id,
    d.field,
    v.salary
FROM internal.mv_core_vacancy v
JOIN bridge_vacancy_field b ON v.vacancy_id = b.vacancy_id
JOIN dim_field d ON b.field_id = d.field_id
WHERE v.salary IS NOT NULL;

CREATE INDEX idx_marts_mv_vacancy_field_salary_vacancy_id ON marts.mv_vacancy_field_salary(vacancy_id);



CREATE MATERIALIZED VIEW marts.mv_vacancy_grade_salary AS
SELECT 
    v.vacancy_id,
    v.salary,
    v.published_at,
    g.grade
FROM internal.mv_core_vacancy v
JOIN bridge_vacancy_grade b ON v.vacancy_id = b.vacancy_id
JOIN dim_grade g ON b.grade_id = g.grade_id;

CREATE INDEX idx_marts_mv_vacancy_grade_salary_vacancy_id ON marts.mv_vacancy_grade_salary(vacancy_id);



CREATE MATERIALIZED VIEW marts.mv_vacancy_grade_schedule AS
SELECT 
    bg.vacancy_id,
    g.grade,
    s.schedule
FROM bridge_vacancy_grade bg
JOIN dim_grade g ON bg.grade_id = g.grade_id
JOIN bridge_vacancy_schedule bs ON bg.vacancy_id = bs.vacancy_id
JOIN dim_schedule s ON bs.schedule_id = s.schedule_id
WHERE g.grade IS NOT NULL AND s.schedule IS NOT NULL;

CREATE INDEX idx_marts_mv_vacancy_grade_schedule_vacancy_id ON marts.mv_vacancy_grade_schedule(vacancy_id);



CREATE MATERIALIZED VIEW marts.mv_vacancy_experience AS
SELECT 
    vacancy_id,
    experience
FROM internal.mv_core_vacancy
WHERE experience IS NOT NULL;

CREATE UNIQUE INDEX idx_marts_mv_vacancy_experience_vacancy_id ON marts.mv_vacancy_experience(vacancy_id);



CREATE MATERIALIZED VIEW marts.mv_vacancy_grade_duration AS
SELECT 
    v.vacancy_id,
    g.grade,
    (v.closed_at::date - v.published_at::date) AS days_open
FROM internal.mv_core_vacancy v
JOIN bridge_vacancy_grade b ON v.vacancy_id = b.vacancy_id
JOIN dim_grade g ON b.grade_id = g.grade_id
WHERE g.grade IS NOT NULL 
  AND v.closed_at IS NOT NULL 
  AND v.published_at IS NOT NULL;

CREATE INDEX idx_marts_mv_vacancy_grade_duration_vacancy_id ON marts.mv_vacancy_grade_duration(vacancy_id);



CREATE MATERIALIZED VIEW marts.mv_vacancy_skill_salary AS
SELECT 
    v.vacancy_id,
    s.skill,
    v.salary
FROM internal.mv_core_vacancy v
JOIN bridge_vacancy_skill bs ON bs.vacancy_id = v.vacancy_id
JOIN dim_skill s ON bs.skill_id = s.skill_id
WHERE v.salary IS NOT NULL AND s.skill IS NOT NULL;

CREATE INDEX idx_marts_mv_vacancy_skill_salary_vacancy_id ON marts.mv_vacancy_skill_salary(vacancy_id);



CREATE MATERIALIZED VIEW marts.mv_vacancy_grade_skill AS
SELECT 
    bg.vacancy_id,
    g.grade,
    s.skill
FROM bridge_vacancy_grade bg
JOIN dim_grade g ON bg.grade_id = g.grade_id
JOIN bridge_vacancy_skill bs ON bg.vacancy_id = bs.vacancy_id
JOIN dim_skill s ON bs.skill_id = s.skill_id
WHERE g.grade IS NOT NULL AND s.skill IS NOT NULL;

CREATE INDEX idx_marts_mv_vacancy_grade_skill_vacancy_id ON marts.mv_vacancy_grade_skill(vacancy_id);



CREATE MATERIALIZED VIEW marts.mv_vacancy_schedule AS
SELECT 
    bs.vacancy_id,
    s.schedule
FROM bridge_vacancy_schedule bs
JOIN dim_schedule s ON bs.schedule_id = s.schedule_id
WHERE s.schedule IS NOT NULL;

CREATE INDEX idx_marts_mv_vacancy_schedule_vacancy_id ON marts.mv_vacancy_schedule(vacancy_id);



CREATE MATERIALIZED VIEW marts.mv_vacancy_location_salary AS
SELECT 
    v.vacancy_id,
    l.location,
    c.country,
    c.iso,
    v.salary
FROM internal.mv_core_vacancy v
JOIN bridge_vacancy_location bl ON v.vacancy_id = bl.vacancy_id
JOIN dim_location l ON bl.location_id = l.location_id
LEFT JOIN dim_country c ON l.country_id = c.country_id
WHERE v.salary IS NOT NULL 
  AND l.location IS NOT NULL;

CREATE INDEX idx_marts_mv_vacancy_location_salary_vacancy_id ON marts.mv_vacancy_location_salary(vacancy_id);



CREATE MATERIALIZED VIEW marts.mv_vacancy_full_info AS
SELECT 
    v.published_at,
    v.vacancy_id,
    v.platform,
    COALESCE(v.employer, 'Не указано') AS employer,
    v.title,
    COALESCE(g.grade, 'Не указано') AS grade,
    COALESCE(v.experience, 'Не указано') AS experience,
    COALESCE(s.schedule, 'Не указано') AS schedule,
    v.salary,
    v.url
FROM internal.mv_core_vacancy v
LEFT JOIN bridge_vacancy_grade bg ON v.vacancy_id = bg.vacancy_id
LEFT JOIN dim_grade g ON bg.grade_id = g.grade_id
LEFT JOIN bridge_vacancy_schedule bs ON v.vacancy_id = bs.vacancy_id
LEFT JOIN dim_schedule s ON bs.schedule_id = s.schedule_id
WHERE closed_at IS NULL;

CREATE INDEX idx_marts_mv_vacancy_full_info_vacancy_id ON marts.mv_vacancy_full_info(vacancy_id);



CREATE MATERIALIZED VIEW marts.mv_vacancy_english_level AS
SELECT 
    v.vacancy_id,
    COALESCE(eng.language_level, 'Не требуется') AS language_level,
    v.salary
FROM internal.mv_core_vacancy v
LEFT JOIN (
    SELECT 
        bvl.vacancy_id,
        dll.language_level
    FROM bridge_vacancy_language bvl
    JOIN dim_language dl ON bvl.language_id = dl.language_id
    JOIN dim_language_level dll ON bvl.language_level_id = dll.language_level_id
    WHERE dl.language = 'Английский'
) eng ON v.vacancy_id = eng.vacancy_id
WHERE v.salary IS NOT NULL;

CREATE INDEX idx_marts_mv_vacancy_english_level_vacancy_id ON marts.mv_vacancy_english_level(vacancy_id);
