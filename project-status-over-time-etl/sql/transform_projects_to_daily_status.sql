-- Step 1: Calculate the average project duration in days (only for completed projects)
WITH avg_duration AS (
    SELECT 
        ROUND(AVG(end_date - initial_date))::INT AS avg_days
    FROM projects
    WHERE end_date IS NOT NULL
      AND initial_date IS NOT NULL
),
-- Step 2: Determine start and end dates for each project
-- - start_date: the earliest available date between initial, active, and store_date
-- - end_date:
--      → real end_date if exists
--      → otherwise, estimated as latest date + average duration + buffer
expanded_projects AS (
    SELECT
        p.project_id,
        LEAST(p.initial_date, p.active_date, p.store_date) AS start_date,
        COALESCE(
            p.end_date,
            GREATEST(p.initial_date, p.active_date, p.store_date) + (a.avg_days + 15)
        ) AS estimated_end_date,
        p.initial_date,
        p.active_date,
        p.end_date
    FROM projects p
    CROSS JOIN avg_duration a
)
-- Step 3: Expand each project into daily rows and assign status
INSERT INTO daily_project_status (project_id, project_date, status)
SELECT
    ep.project_id,
    gs.day::date AS project_date,
    -- Status assignment logic
    CASE
        -- Project is before active_date → 'initial'
        WHEN ep.active_date IS NOT NULL AND gs.day < ep.active_date THEN 'initial'
        -- Between active_date and end_date (exclusive) → 'active'
        WHEN ep.active_date IS NOT NULL AND gs.day >= ep.active_date AND gs.day < COALESCE(ep.end_date, ep.estimated_end_date) THEN 'active'
        -- On end_date exactly → 'completed'
        WHEN ep.end_date IS NOT NULL AND gs.day = ep.end_date THEN 'completed'
        -- If only initial_date exists, or active_date is null → fallback to 'initial'
        ELSE 'initial'
    END AS status
FROM expanded_projects ep
-- Generate one row per day between start_date and end_date (inclusive)
JOIN LATERAL generate_series(
    ep.start_date,                       -- dynamic start date per project
    COALESCE(ep.end_date, ep.estimated_end_date),  -- real or estimated end date
    interval '1 day'
) AS gs(day) ON TRUE;  -- ON TRUE allows generate_series to apply per-row