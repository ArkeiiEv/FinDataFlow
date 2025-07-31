INSERT INTO public.fact_stock_prices (
    data_key, symbol_key, open_price, high_price, low_price, close_price, vol
)
SELECT
    dd.date_key,
    ds.symbol_key,
    s.open_price,
    s.high,
    s.low,
    s.close_price,
    s.volume
FROM staging.stock_prices s
JOIN public.dim_date dd ON s.date = dd.full_date
JOIN public.dim_symbol ds ON s.symbol = ds.symbol_code
ON CONFLICT (data_key, symbol_key) DO UPDATE SEt
    open_price = EXCLUDED.open_price,
    high_price = EXCLUDED.high_price,
    low_price = EXCLUDED.low_price,
    close_price = EXCLUDED.close_price,
    volume = EXCLUDED.volume;