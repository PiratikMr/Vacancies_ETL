--// HeadHunter
    --// update existing hh_vacancies
        create or replace function update_hh_vac()
        returns trigger as $$
        begin

        if exists (select 1 from hh_vacancies where id = NEW.id)
            then 
                update hh_vacancies
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

        create or replace trigger update_hh_vac
        before insert on hh_vacancies
        for each row
        execute function update_hh_vac();
--//

--// GetMatch
    --// update existing gm_vacancies
        create or replace function update_gm_vac()
        returns trigger as $$
        begin

        if exists (select 1 from gm_vacancies where id = NEW.id)
            then 
                update gm_vacancies
                set 
                    name = NEW.name,

                    salary_from = NEW.salary_from,
                    salary_to = NEW.salary_to,
                    salary_hidden = NEW.salary_hidden,

                    currency_id = NEW.currency_id,
                    english_lvl = NEW.english_lvl,

                    remote_op = NEW.remote_op,
                    office_op = NEW.office_op,
                    employer = NEW.employer

                where id = NEW.id;
                return null;
            else 
                return NEW;
            end if;
            
        end;
        $$ language plpgsql;

        create or replace trigger update_gm_vac
        before insert on gm_vacancies
        for each row
        execute function update_gm_vac();
--//



--// update existing currency
    create or replace function update_currency()
    returns trigger as $$
    begin

    if exists (select 1 from currency where id = NEW.id)
        then
            update currency
            set 
                rate = NEW.rate
            where id = NEW.id;
            return null;
        else
            return NEW;
        end if;

    end;
    $$ language plpgsql;

    create or replace trigger update_currency_trigger
    before insert on currency
    for each row
    execute function update_currency();
--//