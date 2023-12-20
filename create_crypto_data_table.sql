CREATE TABLE CryptoData (
    date date,
    coin varchar(256),
    opening float8,
    closing float8,
    lowest float8,
    highest float8,
    volume float8,
    quantity float8,
    amount int8,
    avg_price float8,
    primary key (date,coin)
);