create table dim_currency (
    currency_id                         bigserial               primary key,
    currency                            text                    not null,
    rate                                double precision        not null default (1.0),

    unique(currency)
);

create table mapping_dim_currency (
    currency_id                         bigint                  not null references dim_currency(currency_id),
    mapped_value                        text                    not null,
    is_canonical                        boolean                 not null,

    primary key (currency_id, mapped_value)
);

create unique index uq_canonical_per_currency
on mapping_dim_currency (currency_id)
where (is_canonical = true);