INSERT INTO public.dim_date (date_key, full_date, year, month_number, day_of_month)
SELECT
    TO_CHAR(date, 'YYYYMMDD')::INT,
    date,
    EXTRACT(YEAR FROM date),
    EXTRACT(MONTH FROM date),
    EXTRACT(DAY FROM date)
FROM staging.stock_prices
ON CONFLICT (date_key) DO NOTHING;