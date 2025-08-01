INSERT INTO public.dim_symbol (symbol_code, company_name)
VALUES (:symbol_code, 'N/A')
ON CONFLICT (symbol_code) DO NOTHING;