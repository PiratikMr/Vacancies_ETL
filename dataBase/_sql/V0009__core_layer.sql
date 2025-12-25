create materialized view core.vacancies as
    select * from staging.vacancies;

create materialized view core.employers as
    select * from staging.employers;

create materialized view core.employments as
    select * from staging.employments;

create materialized view core.experiences as
    select * from staging.experiences;

create materialized view core.fields as
    select * from staging.fields;

create materialized view core.grades as
    select * from staging.grades;

create materialized view core.languages as
    select * from staging.languages;

create materialized view core.locations as
    select * from staging.locations;

create materialized view core.salaries as 
    select * from staging.salaries;

create materialized view core.schedules as
    select * from staging.schedules;

create materialized view core.skills as
    select * from staging.skills;