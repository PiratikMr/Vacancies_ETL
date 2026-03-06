INSERT INTO dim_currency (currency) VALUES
    ('RUB'),
    ('BYN');

INSERT INTO mapping_dim_currency (currency_id, mapped_value, is_canonical) VALUES
    (1, '_rub_', true),
    (1, '_rur_', false),

    (2, '_byn_', true),
    (2, '_byr_', false);




INSERT INTO dim_grade (grade) VALUES
    ('Стажер'),       -- id 1
    ('Джуниор'),      -- id 2
    ('Миддл'),        -- id 3
    ('Сеньор'),       -- id 4
    ('Тимлид'),       -- id 5
    ('Архитектор'),   -- id 6
    ('Руководитель'), -- id 7
    ('Консультант');  -- id 8

INSERT INTO mapping_dim_grade (grade_id, mapped_value, is_canonical) VALUES
    -- 1. Стажер
    (1, '_стажер_', true),
    (1, '_intern_ _стажер_', false),

    -- 2. Джуниор
    (2, '_джуниор_', true),
    (2, '_junior_', false),
    (2, '_junior_ _младш_', false),

    -- 3. Миддл
    (3, '_миддл_', true),
    (3, '_middl_', false),
    (3, '_middl_ _средн_', false),

    -- 4. Сеньор
    (4, '_сеньор_', true),
    (4, '_senior_', false),
    (4, '_senior_ _старш_', false),

    -- 5. Тимлид
    (5, '_тимлид_', true),
    (5, '_lead_', false),
    (5, '_lead_ _ведущ_', false),
    (5, '_групп_ _руководител_ _тимлид_', false),

    -- 6. Архитектор
    (6, '_архитектор_', true),

    -- 7. Руководитель
    (7, '_руководител_', true),
    (7, '_отдел_ _подразделен_ _руководител_', false),
    (7, '_директор_', false),
    (7, '_vp_', false),

    -- 8. Консультант
    (8, '_консультант_', true);




INSERT INTO dim_experience (experience) VALUES
    ('Нет опыта'),          -- id 1
    ('Менее 1 года'),       -- id 2
    ('От 1 года до 3 лет'), -- id 3
    ('От 3 до 6 лет'),      -- id 4
    ('Более 6 лет'),        -- id 5
    ('Любой');              -- id 6

INSERT INTO mapping_dim_experience (experience_id, mapped_value, is_canonical) VALUES
    -- 1. Нет опыта
    (1, '_нет_ _опыт_', true),
    (1, '_0_', false),
    (1, '_experi_ _no_', false),

    -- 2. Менее 1 года
    (2, '_1_ _год_ _мен_', true),
    (2, '_one_ _to_ _up_ _year_', false),

    -- 3. От 1 года до 3 лет
    (3, '_1_ _3_ _год_ _до_ _лет_ _от_', true),
    (3, '_1_', false),
    (3, '_2_', false),
    (3, '_more_ _two_ _year_', false),
    (3, '_minimum_ _year_', false),

    -- 4. От 3 до 6 лет
    (4, '_3_ _6_ _до_ _лет_ _от_', true),
    (4, '_3_ _5_ _до_ _лет_ _от_', false),
    (4, '_3_', false),
    (4, '_4_', false),
    (4, '_5_', false),
    (4, '_more_ _three_ _year_', false),

    -- 5. Более 6 лет
    (5, '_6_ _бол_ _лет_', true),
    (5, '_5_ _бол_ _лет_', false),
    (5, '_6_', false),
    (5, '_7_', false),
    (5, '_8_', false),
    (5, '_10_', false),
    (5, '_five_ _more_ _year_', false),

    -- 6. Любой
    (6, '_любо_', true);




INSERT INTO dim_employment (employment) VALUES
    ('Полная занятость'),     -- id 1
    ('Частичная занятость'),  -- id 2
    ('Проектная работа'),     -- id 3
    ('Волонтерство'),         -- id 4
    ('Стажировка');           -- id 5

INSERT INTO mapping_dim_employment (employment_id, mapped_value, is_canonical) VALUES
    -- 1. Полная занятость
    (1, '_полн_ _занят_', true),
    (1, '_полн_', false),
    (1, '_full_ _time_', false),

    -- 2. Частичная занятость
    (2, '_частичн_ _занят_', true),
    (2, '_частичн_', false),
    (2, '_подработк_', false),
    (2, '_part_ _time_', false),
    (2, '_non_ _standard_', false),

    -- 3. Проектная работа
    (3, '_проектн_ _работ_', true),
    (3, '_проект_', false),
    (3, '_project_', false),

    -- 4. Волонтерство
    (4, '_волонтерств_', true),

    -- 5. Стажировка
    (5, '_стажировк_', true);




INSERT INTO dim_schedule (schedule) VALUES
    ('Полный день'),      -- id 1
    ('Сменный график'),   -- id 2
    ('Гибкий график'),    -- id 3
    ('Удаленная работа'), -- id 4
    ('Вахтовый метод');   -- id 5

INSERT INTO mapping_dim_schedule (schedule_id, mapped_value, is_canonical) VALUES
    -- 1. Полный день
    (1, '_полны_ _ден_', true),

    -- 2. Сменный график
    (2, '_сменны_ _график_', true),

    -- 3. Гибкий график
    (3, '_гибк_ _график_', true),

    -- 4. Удаленная работа
    (4, '_удален_ _работ_', true),

    -- 5. Вахтовый метод
    (5, '_вахтовы_ _метод_', true);




INSERT INTO dim_language_level (language_level) VALUES
    ('A1 — Начальный'),             -- id 1
    ('A2 — Элементарный'),          -- id 2
    ('B1 — Средний'),               -- id 3
    ('B2 — Средне-продвинутый'),    -- id 4
    ('C1 — Продвинутый'),           -- id 5
    ('C2 — В совершенстве');        -- id 6

INSERT INTO mapping_dim_language_level (language_level_id, mapped_value, is_canonical) VALUES
    -- 1. A1
    (1, '_a1_ _начальны_', true),
    (1, '_a1_', false),

    -- 2. A2
    (2, '_a2_ _элементарны_', true),
    (2, '_a2_ _intermedi_ _pre_', false),
    (2, '_a2_', false),

    -- 3. B1
    (3, '_b1_ _средн_', true),
    (3, '_b1_', false),

    -- 4. B2
    (4, '_b2_ _продвинуты_ _средн_', true),
    (4, '_b2_ _intermedi_ _upper_', false),
    (4, '_b2_', false),

    -- 5. C1
    (5, '_c1_ _продвинуты_', true),
    (5, '_advanc_ _c1_ _c2_ _fluent_', false),
    (5, '_c1_', false),

    -- 6. C2
    (6, '_c2_ _в_ _совершенств_', true),
    (6, '_c2_', false);




INSERT INTO dim_country (country) VALUES
    ('США'),                           -- получит id 1
    ('Объединенные Арабские Эмираты'), -- получит id 2
    ('Великобритания'),                -- получит id 3
    ('Кипр'),                          -- получит id 4
    ('Сербия'),                        -- получит id 5
    ('Болгария'),                      -- получит id 6
    ('Польша'),                        -- получит id 7
    ('Испания');                       -- получит id 8

INSERT INTO mapping_dim_country (country_id, mapped_value, is_canonical) VALUES
    -- 1. США
    (1, '_сша_', true),
    (1, '_us_', false),

    -- 2. ОАЭ
    (2, '_арабск_ _объединен_ _эмират_', true),
    (2, '_arab_ _emir_ _unit_', false),

    -- 3. Великобритания
    (3, '_великобритан_', true),
    (3, '_uk_', false),

    -- 4. Кипр
    (4, '_кипр_', true),
    (4, '_cyprus_', false),

    -- 5. Сербия
    (5, '_серб_', true),
    (5, '_serbia_', false),

    -- 6. Болгария
    (6, '_болгар_', true),
    (6, '_bulgaria_', false),

    -- 7. Польша
    (7, '_польш_', true),
    (7, '_polska_', false),

    -- 8. Испания
    (8, '_испан_', true),
    (8, '_spain_', false);