create materialized view internal.hh_grades as
    with grade_flags as (
        select
            id,
            lower(title) ~ '.*(jun|джун).*' as jun,
            lower(title) ~ '.*(midd|мидд).*' as midd,
            lower(title) ~ '.*(sen|сеньор).*' as sen,
            lower(title) ~ '.*(intern|стаж).*' as intern
        from hh_vacancies
    ) select
        id,
        unnest(array_remove(array[
            case when jun then 'Джуниор' end,
            case when midd then 'Миддл' end,
            case when sen then 'Сеньор' end,
            case when intern then 'Стажер' end
        ], null)) as grade
    from grade_flags;


create materialized view internal.gm_grades as
    with grade_flags as (
        select
            id,
            lower(title) ~ '.*(jun|джун).*' as jun,
            lower(title) ~ '.*(midd|мидд).*' as midd,
            lower(title) ~ '.*(sen|сеньор).*' as sen,
            lower(title) ~ '.*(intern|стаж).*' as intern
        from gm_vacancies
    ) select
        id,
        unnest(array_remove(array[
            case when jun then 'Джуниор' end,
            case when midd then 'Миддл' end,
            case when sen then 'Сеньор' end,
            case when intern then 'Стажер' end
        ], null)) as grade
    from grade_flags;