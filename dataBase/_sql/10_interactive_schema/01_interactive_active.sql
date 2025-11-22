create materialized view interactive_active.vacancies as
    select
        v.id as id,
        v.source as source,
        v.published_at as published_at,
        s.salary as salary,
        s.has_range as has_range,
        s.currency as currency,
        emplr.employer as employer,
        emplm.employment as employment,
        ex.experience as experience

    from core.vacancies as v
    left join core.salaries as s on s.id = v.id
    left join core.employers as emplr on emplr.id = v.id
    left join core.employments as emplm on emplm.id = v.id
    left join core.experiences as ex on ex.id = v.id
    where v.is_active is true;

create unique index on interactive_active.vacancies (id);
create index on interactive_active.vacancies (source);
create index on interactive_active.vacancies (published_at);
create index on interactive_active.vacancies (salary);
create index on interactive_active.vacancies (has_range);
create index on interactive_active.vacancies (currency);
create index on interactive_active.vacancies (employer);
create index on interactive_active.vacancies (employment);
create index on interactive_active.vacancies (experience);


create materialized view interactive_active.languages as
    select
        l.id as id,
        l.language as language,
        l.level as level
    from core.languages as l
    join core.vacancies as v on v.id = l.id
    where v.is_active is true;

create index on interactive_active.languages (id, language, level);


create materialized view interactive_active.locations as
    select
        l.id as id,
        l.region as region,
        l.country as country,
        l.lat as lat,
        l.lng as lng
    from core.locations as l
    join core.vacancies as v on v.id = l.id
    where v.is_active is true;

create index on interactive_active.locations (id, region, country);


create materialized view interactive_active.fields as
    select
        f.id as id,
        f.field as field
    from core.fields as f
    join core.vacancies as v on v.id = f.id
    where v.is_active is true;

create index on interactive_active.fields (id, field);


create materialized view interactive_active.grades as
    select
        g.id as id,
        g.grade as grade
    from core.grades as g
    join core.vacancies as v on v.id = g.id
    where v.is_active is true;

create index on interactive_active.grades (id, grade);


create materialized view interactive_active.schedules as
    select
        s.id as id,
        s.type as schedule
    from core.schedules as s
    join core.vacancies as v on v.id = s.id
    where v.is_active is true;

create index on interactive_active.schedules (id, schedule);

create materialized view interactive_active.skills as
    select
        s.id as id,
        s.skill as skill
    from core.vacancies as v
    join core.skills as s on s.id = v.id
    where v.is_active is true;

create index on interactive_active.skills (id, skill);