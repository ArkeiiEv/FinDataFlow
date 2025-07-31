INSERT INTO public.dim_symbol (symbol_code)
SELECT DISTINCT symbol
FROM staging.stock_prices
ON CONFLICT (symbol_code) DO NOTHING