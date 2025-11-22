create materialized view interactive.fields_salary as
    select
        f.id as id,
        f.field as field,
        v.salary as salary
    from interactive_active.fields as f
    join interactive_active.vacancies as v on v.id = f.id;

create index on interactive.fields_salary (id);


create materialized view interactive.grades_salary as
    select
        g.id as id,
        g.grade as grade,
        v.salary as salary
    from interactive_active.grades as g
    join interactive_active.vacancies as v on v.id = g.id;

create index on interactive.grades_salary (id);


create materialized view interactive.locations_salary as
    select
        l.id as id,
        l.region as region,
        l.country as country,
        l.lat as lat,
        l.lng as lng,
        v.salary as salary
    from interactive_active.locations as l
    join interactive_active.vacancies as v on v.id = l.id;

create index on interactive.locations_salary (id);


create materialized view interactive.skills_salary as
    select
        v.id as id,
        v.salary as salary,
        s.skill as skill
    from interactive_active.vacancies as v
    join interactive_active.skills as s on s.id = v.id;

create index on interactive.skills_salary (id);


create materialized view interactive.skills_pairs as
    select
        a.id as id,
        a.skill as skill1,
        b.skill as skill2
    from interactive_active.skills as a
    join interactive_active.skills as b on a.id = b.id and a.skill < b.skill;

create index on interactive.skills_pairs (id);


create materialized view interactive.skills_fields as
    select
        f.id as id,
        f.field as field,
        s.skill as skill
    from interactive_active.fields as f
    join interactive_active.skills as s on s.id = f.id;

create index on interactive.skills_fields (id);


create materialized view interactive.employers_grades_salary as
    select
        v.id as id,
        v.employer as employer,
        v.salary as salary,
        g.grade as grade
    from interactive_active.vacancies as v
    join interactive_active.grades as g on g.id = v.id;

create index on interactive.employers_grades_salary (id);


create materialized view interactive.languages_salary as
    select
        l.id as id,
        l.language as language,
        l.level as level,
        v.salary as salary
    from interactive_active.languages as l
    join interactive_active.vacancies as v on v.id = l.id;

create index on interactive.languages_salary (id);


create materialized view interactive.vacancies_language as
    select
        v.id as id,
        v.salary as salary,
        l.language as language
    from interactive_active.vacancies as v
    left join interactive_active.languages as l on l.id = v.id;

create index on interactive.vacancies_language (id);


create materialized view interactive.schedules_salary as
    select
        v.id as id,
        v.salary as salary,
        s.schedule as schedule
    from interactive_active.vacancies as v
    join interactive_active.schedules as s on s.id = v.id;

create index on interactive.schedules_salary (id);


create materialized view interactive.schedules_fields as 
    select
        s.id as id,
        s.schedule as schedule,
        f.field as field
    from interactive_active.schedules as s
    join interactive_active.fields as f on f.id = s.id;

create index on interactive.schedules_fields (id);