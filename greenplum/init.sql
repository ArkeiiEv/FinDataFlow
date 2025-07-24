CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.stock_prices (
    date DATE,
    open_price NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close_price NUMERIC,
    volume BIGINT
);