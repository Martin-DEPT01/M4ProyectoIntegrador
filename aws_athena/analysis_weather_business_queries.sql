---------------------------------------------------------------------
-- 1) ¿Cómo varía el potencial solar estimado a lo largo del día y del mes en Riohacha y en la Patagonia?

-- POR DIA
SELECT 
    year, 
    month, 
    day,
    ROUND(MAX(CASE WHEN region = 'Patagonia' THEN potencia_solar_w ELSE NULL END), 2) as var_pot_solar_patagonia,
    ROUND(MAX(CASE WHEN region = 'Riohacha' THEN potencia_solar_w ELSE NULL END), 2) as var_pot_solar_riohacha
FROM gold_weather_metrics
GROUP BY year, month, day
ORDER BY year DESC, month DESC, day DESC;


-- POR MES
WITH avg_pot_sol AS (
    SELECT 
        region, 
        year, 
        month, 
        day, 
        AVG(potencia_solar_w) AS avg_pot_sol_daily
    FROM gold_weather_metrics
    GROUP BY region, year, month, day
)
SELECT 
    year, month, 
    -- pat_max_avg_pot_sol, pat_min_avg_pot_sol, 
    ROUND((pat_max_avg_pot_sol - pat_min_avg_pot_sol), 2) AS pat_var_pot_sol_monthly,
    -- rio_max_avg_pot_sol, rio_min_avg_pot_sol,
    ROUND((rio_max_avg_pot_sol - rio_min_avg_pot_sol), 2) AS rio_var_pot_sol_monthly
FROM 
(
SELECT 
    year, 
    month, 
    ROUND(MIN(CASE WHEN region = 'Patagonia' THEN avg_pot_sol_daily ELSE NULL END), 2) as pat_min_avg_pot_sol,
    ROUND(MAX(CASE WHEN region = 'Patagonia' THEN avg_pot_sol_daily ELSE NULL END), 2) as pat_max_avg_pot_sol,
    ROUND(MIN(CASE WHEN region = 'Riohacha' THEN avg_pot_sol_daily ELSE NULL END), 2) as rio_min_avg_pot_sol,
    ROUND(MAX(CASE WHEN region = 'Riohacha' THEN avg_pot_sol_daily ELSE NULL END), 2) as rio_max_avg_pot_sol
FROM avg_pot_sol
GROUP BY year, month
ORDER BY year DESC, month DESC
)
ORDER BY year DESC, month DESC;

---------------------------------------------------------------------
-- 2) ¿Qué patrones históricos se observan en el potencial eólico de ambas ubicaciones?

-- POR DIA
SELECT
    year, 
    month, 
    day,
    ROUND((max_pot_eolico_patagonia - min_pot_eolico_patagonia), 2) AS daily_var_pot_eolico_patagonia,
    ROUND((max_pot_eolico_riohacha - min_pot_eolico_riohacha), 2) AS daily_var_pot_eolico_riohacha
FROM
(
SELECT 
    year, 
    month, 
    day,
    ROUND(MAX(CASE WHEN region = 'Patagonia' THEN potencial_eolico_w ELSE NULL END), 2) as max_pot_eolico_patagonia,
    ROUND(MIN(CASE WHEN region = 'Patagonia' THEN potencial_eolico_w ELSE NULL END), 2) as min_pot_eolico_patagonia,
    ROUND(MAX(CASE WHEN region = 'Riohacha' THEN potencial_eolico_w ELSE NULL END), 2) as max_pot_eolico_riohacha,
    ROUND(MIN(CASE WHEN region = 'Riohacha' THEN potencial_eolico_w ELSE NULL END), 2) as min_pot_eolico_riohacha
FROM gold_weather_metrics
GROUP BY year, month, day
ORDER BY year DESC, month DESC, day DESC
);


-- POR MES
WITH avg_pot_eolico AS (
    SELECT 
        region, 
        year, 
        month, 
        day, 
        AVG(potencial_eolico_w) AS avg_pot_eolico_daily
    FROM gold_weather_metrics
    GROUP BY region, year, month, day
)
SELECT 
    year, month, 
    -- pat_max_avg_pot_eolico, pat_min_avg_pot_eolico, 
    ROUND((pat_max_avg_pot_eolico - pat_min_avg_pot_eolico), 2) AS pat_var_pot_eolico_monthly,
    -- rio_max_avg_pot_eolico, rio_min_avg_pot_eolico,
    ROUND((rio_max_avg_pot_eolico - rio_min_avg_pot_eolico), 2) AS rio_var_pot_eolico_monthly
FROM 
(
SELECT 
    year, 
    month, 
    ROUND(MIN(CASE WHEN region = 'Patagonia' THEN avg_pot_eolico_daily ELSE NULL END), 2) as pat_min_avg_pot_eolico,
    ROUND(MAX(CASE WHEN region = 'Patagonia' THEN avg_pot_eolico_daily ELSE NULL END), 2) as pat_max_avg_pot_eolico,
    ROUND(MIN(CASE WHEN region = 'Riohacha' THEN avg_pot_eolico_daily ELSE NULL END), 2) as rio_min_avg_pot_eolico,
    ROUND(MAX(CASE WHEN region = 'Riohacha' THEN avg_pot_eolico_daily ELSE NULL END), 2) as rio_max_avg_pot_eolico
FROM avg_pot_eolico
GROUP BY year, month
ORDER BY year DESC, month DESC
)
ORDER BY year DESC, month DESC;

---------------------------------------------------------------------
-- 3) ¿Qué condiciones climáticas están asociadas con reducciones significativas en el potencial renovable?

---------------------------------------------------------------------
-- 4) ¿Cuáles fueron los días con mayor y menor potencial energético en cada ubicación durante el periodo de análisis?

-- MAX ENERGY PATAGONIA
SELECT 
    region, 
    year, 
    month, 
    day, 
    ROUND(AVG(potencial_renovable_total_w), 2) AS avg_pot_energy_daily
FROM gold_weather_metrics
WHERE region = 'Patagonia'
GROUP BY region, year, month, day
ORDER BY ROUND(AVG(potencial_renovable_total_w), 2) DESC
LIMIT 10;

-- MAX ENERGY RIOHACHA
SELECT 
    region, 
    year, 
    month, 
    day, 
    ROUND(AVG(potencial_renovable_total_w), 2) AS avg_pot_energy_daily
FROM gold_weather_metrics
WHERE region = 'Riohacha'
GROUP BY region, year, month, day
ORDER BY ROUND(AVG(potencial_renovable_total_w), 2) DESC
LIMIT 10;

-- MIN ENERGY PATAGONIA
SELECT 
    region, 
    year, 
    month, 
    day, 
    ROUND(AVG(potencial_renovable_total_w), 2) AS avg_pot_energy_daily
FROM gold_weather_metrics
WHERE region = 'Patagonia'
GROUP BY region, year, month, day
ORDER BY ROUND(AVG(potencial_renovable_total_w), 2) ASC
LIMIT 10;

-- MIN ENERGY RIOHACHA
SELECT 
    region, 
    year, 
    month, 
    day, 
    ROUND(AVG(potencial_renovable_total_w), 2) AS avg_pot_energy_daily
FROM gold_weather_metrics
WHERE region = 'Riohacha'
GROUP BY region, year, month, day
ORDER BY ROUND(AVG(potencial_renovable_total_w), 2) ASC
LIMIT 10;