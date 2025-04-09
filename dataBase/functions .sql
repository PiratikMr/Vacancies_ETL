create or replace function fill_salary()
returns integer as $$
declare 
    updated_rows integer;
begin
    update vacancies v
    set 
        salary_from = median.med_salary_from,
        salary_to = median.med_salary_to,
        currency_id = 'RUR'
    from (
        select 
            role_id,
            percentile_cont(0.5) within group (order by (v.salary_from / c.rate)) as med_salary_from,
            percentile_cont(0.5) within group (order by (v.salary_to / c.rate)) as med_salary_to
        from vacancies as v
        join currency as c on c.id = v.currency_id
        where salary_from is not null or salary_to is not null
        group by role_id
    ) median
    where v.role_id = median.role_id 
    and (v.salary_from is null and v.salary_to is null);

    GET diagnostics updated_rows = row_count;
    return updated_rows;
end;
$$ language plpgsql;