INSERT INTO dim_currency (currency) VALUES
    ('RUB'),
    ('BYN');

INSERT INTO mapping_dim_currency (currency_id, mapped_value, is_canonical) VALUES
    (1, '_rub_', true),
    (1, '_rur_', false),

    (2, '_byn_', true),
    (2, '_byr_', false);


INSERT INTO dim_grade (grade) VALUES
    ('Сеньор'),
    ('Миддл'),
    ('Тимлид')
    ('Джуниор');

INSERT INTO mapping_dim_grade (grade_id, mapped_value, is_canonical) VALUES
    (1, '_сеньор_', true),
    (1, '_senior_', false),

    (2, '_миддл_', true),
    (2, '_middl_', false),

    (3, '_тимлид_', true),
    (3, '_lead_', false),
    (3, '_групп_ _руководител_ _тимлид_', false),

    (4, '_джуниор_', true),
    (4, '_junior_', false);