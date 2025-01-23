create or replace function update_old_id_trigger()
returns trigger as $$
begin

if exists (select 1 from vacancies where id = NEW.id)
    then 
        update vacancies
        set 
            name = NEW.name,

            region_area_id = NEW.region_area_id,
            country_area_id = NEW.country_area_id,

            salary_from = NEW.salary_from,
            salary_to = NEW.salary_to,

            close_to_metro = NEW.close_to_metro,

            schedule_id = NEW.schedule_id,
            experience_id = NEW.experience_id,
            employment_id = NEW.employment_id,
            currency_id = NEW.currency_id,

            role_id = NEW.role_id           
        where id = NEW.id;
        return null;
    else 
        return NEW;
    end if;
    
end;
$$ language plpgsql;

create trigger update_old_id_trigger
before insert on vacancies
for each row
execute function update_old_id_trigger();