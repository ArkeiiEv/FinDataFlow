INSERT INTO public.fact_stock_prices (
    date_key, symbol_code, open_price, high_price, low_price, close_price, volume -- CHANGED: Column names (data_key -> date_key, symbol_key -> symbol_code, vol -> volume)
)
SELECT
    dd.date_key,
    s.symbol,
    s.open_price,
    s.high,
    s.low,
    s.close_price,
    s.volume
FROM staging.stock_prices s
JOIN public.dim_date dd ON s.date = dd.full_date
WHERE s.symbol = :symbol_code
ON CONFLICT (date_key, symbol_code) DO UPDATE SET
    open_price = EXCLUDED.open_price,
    high_price = EXCLUDED.high_price,
    low_price = EXCLUDED.low_price,
    close_price = EXCLUDED.close_price,
    volume = EXCLUDED.volume;