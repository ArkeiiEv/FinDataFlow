CREATE TABLE IF NOT EXISTS staging.stock_prices (
    date DATE,
    open_price NUMERIC(10, 4),
    high NUMERIC(10, 4),
    low NUMERIC(10 ,4),
    close_price NUMERIC(10, 4),
    volume BIGINT,
    symbol VARCHAR(10) NOT NULL
) DISTRIBUTED BY (date);

CREATE TABLE IF NOT EXISTS public.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week INT,
    day_name VARCHAR(10),
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    month_number INT,
    month_name VARCHAR(10),
    quarter_number INT,
    quarter_name VARCHAR(2),
    year INT
) DISTRIBUTED BY (date_key);

CREATE TABLE IF NOT EXISTS public.dim_symbol (
    symbol_key SERIAL,
    symbol_code VARCHAR(10) NOT NULL PRIMARY KEY,
    company_name VARCHAR(255)
) DISTRIBUTED BY (symbol_code);

CREATE TABLE IF NOT EXISTS public.fact_stock_prices (
    date_key INT NOT NULL REFERENCES public.dim_date(date_key),
    symbol_code VARCHAR(10) NOT NULL REFERENCES public.dim_symbol(symbol_code),
    open_price NUMERIC(10, 4),
    high_price NUMERIC(10, 4),
    low_price NUMERIC(10, 4),
    close_price NUMERIC(10, 4),
    volume BIGINT,
    PRIMARY KEY (date_key, symbol_code)
) DISTRIBUTED BY (date_key);