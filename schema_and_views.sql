DROP TABLE IF EXISTS ds_salaries CASCADE;

CREATE TABLE ds_salaries (
  id                INT,
  work_year         INT NOT NULL,
  experience_level  TEXT NOT NULL,
  employment_type   TEXT NOT NULL,
  job_title         TEXT NOT NULL,
  salary            NUMERIC,
  salary_currency   TEXT,
  salary_in_usd     NUMERIC NOT NULL,
  employee_residence TEXT,
  remote_ratio      INT,
  company_location  TEXT,
  company_size      TEXT
);

WITH params AS (
  SELECT
    2020::INT  AS year_from,
    2022::INT  AS year_to,
    NULL::TEXT AS country_filter,
    NULL::TEXT AS title_filter,
    NULL::TEXT AS exp_filter,
    NULL::INT  AS min_remote,
    NULL::TEXT AS size_filter
),
filtered AS (
  SELECT *
  FROM ds_salaries d, params p
  WHERE d.work_year BETWEEN p.year_from AND p.year_to
    AND (p.country_filter IS NULL OR d.company_location = p.country_filter)
    AND (p.title_filter   IS NULL OR d.job_title = p.title_filter)
    AND (p.exp_filter     IS NULL OR d.experience_level = p.exp_filter)
    AND (p.min_remote     IS NULL OR d.remote_ratio >= p.min_remote)
    AND (p.size_filter    IS NULL OR d.company_size = p.size_filter)
)
SELECT
  COUNT(*)                               AS roles_count,
  ROUND(AVG(salary_in_usd))              AS avg_salary_usd,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_in_usd)::NUMERIC(12,2) AS median_salary_usd,
  MIN(salary_in_usd)                     AS min_salary_usd,
  MAX(salary_in_usd)                     AS max_salary_usd
FROM filtered;

CREATE OR REPLACE VIEW vw_salary_trend_by_year AS
WITH params AS (
  SELECT 2019::INT AS year_from, 2022::INT AS year_to, NULL::TEXT AS country_filter
),
filtered AS (
  SELECT *
  FROM ds_salaries d, params p
  WHERE d.work_year BETWEEN p.year_from AND p.year_to
    AND (p.country_filter IS NULL OR d.company_location = p.country_filter)
)
SELECT
  work_year,
  COUNT(*)                         AS roles,
  ROUND(AVG(salary_in_usd))        AS avg_salary_usd,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_in_usd) AS median_salary_usd
FROM filtered
GROUP BY work_year
ORDER BY work_year;

CREATE OR REPLACE VIEW vw_top_job_titles AS
WITH params AS (
  SELECT 2022::INT AS year_eq, 15::INT AS n
),
filtered AS (
  SELECT *
  FROM ds_salaries d, params p
  WHERE d.work_year = p.year_eq
),
ranked AS (
  SELECT
    job_title,
    COUNT(*)                              AS roles,
    ROUND(AVG(salary_in_usd))             AS avg_salary_usd,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_in_usd) AS median_salary_usd,
    RANK() OVER (ORDER BY AVG(salary_in_usd) DESC) AS r_avg,
    RANK() OVER (ORDER BY COUNT(*)       DESC)     AS r_cnt
  FROM filtered
  GROUP BY job_title
)
SELECT *
FROM ranked
WHERE r_avg <= (SELECT n FROM params)
   OR r_cnt <= (SELECT n FROM params)
ORDER BY r_avg, r_cnt;

CREATE OR REPLACE VIEW vw_country_salary AS
WITH params AS (SELECT 2020::INT AS year_from, 2022::INT AS year_to)
SELECT
  company_location,
  COUNT(*)                              AS roles,
  ROUND(AVG(salary_in_usd))             AS avg_salary_usd,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_in_usd) AS median_salary_usd
FROM ds_salaries d, params p
WHERE d.work_year BETWEEN p.year_from AND p.year_to
GROUP BY company_location
ORDER BY avg_salary_usd DESC;

CREATE OR REPLACE VIEW vw_experience_salary AS
SELECT
  work_year,
  experience_level,
  COUNT(*)                          AS roles,
  ROUND(AVG(salary_in_usd))         AS avg_salary_usd,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_in_usd) AS median_salary_usd
FROM ds_salaries
GROUP BY work_year, experience_level
ORDER BY work_year, experience_level;

CREATE OR REPLACE VIEW vw_company_size_salary AS
SELECT
  work_year,
  company_size,
  COUNT(*)                          AS roles,
  ROUND(AVG(salary_in_usd))         AS avg_salary_usd,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_in_usd) AS median_salary_usd
FROM ds_salaries
GROUP BY work_year, company_size
ORDER BY work_year, company_size;

CREATE OR REPLACE VIEW vw_remote_mix_by_year AS
SELECT
  work_year,
  remote_ratio,
  COUNT(*)                         AS roles,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY work_year), 2) AS pct_of_year
FROM ds_salaries
GROUP BY work_year, remote_ratio
ORDER BY work_year, remote_ratio;

CREATE OR REPLACE VIEW vw_employment_mix_by_year AS
SELECT
  work_year,
  employment_type,
  COUNT(*)                         AS roles,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY work_year), 2) AS pct_of_year
FROM ds_salaries
GROUP BY work_year, employment_type
ORDER BY work_year, employment_type;

CREATE OR REPLACE VIEW vw_top_titles_by_country_year AS
WITH base AS (
  SELECT
    work_year,
    company_location,
    job_title,
    COUNT(*)                  AS roles,
    ROUND(AVG(salary_in_usd)) AS avg_salary_usd
  FROM ds_salaries
  GROUP BY work_year, company_location, job_title
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY work_year, company_location ORDER BY roles DESC, avg_salary_usd DESC) AS rn
  FROM base
)
SELECT *
FROM ranked
WHERE rn <= 5
ORDER BY work_year, company_location, rn;

CREATE OR REPLACE VIEW vw_detail AS
SELECT
  work_year,
  job_title,
  experience_level,
  employment_type,
  company_location,
  employee_residence,
  company_size,
  remote_ratio,
  salary_in_usd
FROM ds_salaries;
