CREATE TYPE films AS(
                    film TEXT,
                    year INTEGER,
                    votes INTEGER,
                    rating REAL,
                    filmid TEXT);

CREATE TYPE quality_class AS ENUM ('star','good','average','bad');

CREATE TABLE actors (
    actor TEXT,
    actorid TEXT,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER,
    PRIMARY KEY(actorid, current_year)
); 

-- Alternative: Create a unique index instead
CREATE UNIQUE INDEX actors_actorid_year_idx 
ON actors (actorid, current_year);

-- INITIAL DATA POPULATION 
INSERT INTO actors
WITH years AS (
    SELECT generate_series(1970, 2021) AS year
),
a AS (
    SELECT
        actor,
        MIN(year) AS first_year
    FROM actor_films
    GROUP BY actor
),
actors_and_years AS (
    SELECT *
    FROM a
    JOIN years 
        ON a.first_year <= years.year
),
windowed AS (
    SELECT
        ay.actor,
        ay.year,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN a1.year IS NOT NULL 
                    THEN CAST(ROW(
                        a1.film,
                        a1.year,
                        a1.votes,
                        a1.rating,
                        a1.filmid
                    ) AS films)
                END
            ) OVER (
                PARTITION BY ay.actor 
                ORDER BY COALESCE(a1.year, ay.year)
            ), NULL
        ) AS years
    FROM actors_and_years ay
    LEFT JOIN actor_films a1
        ON ay.actor = a1.actor
        AND ay.year = a1.year
    ORDER BY ay.actor, ay.year
),
static AS (
    SELECT
        actor,
        MAX(actorid) AS actorid
    FROM actor_films
    GROUP BY actor
),
main AS (
SELECT
    w.actor,
    s.actorid,
    w.years AS films,
    CASE
        WHEN (w.years[CARDINALITY(w.years)]).rating > 8 THEN 'star'
        WHEN (w.years[CARDINALITY(w.years)]).rating > 7 THEN 'good'
        WHEN (w.years[CARDINALITY(w.years)]).rating > 6 THEN 'average'
        ELSE 'bad'
    END::quality_class AS quality_class,
    (w.years[CARDINALITY(w.years)]).year = w.year AS is_active,
    w.year AS current_year
FROM windowed w
JOIN static s
    ON w.actor = s.actor
)
SELECT
    DISTINCT ON (actorid, current_year)
    actor,
    actorid,
    films,
    quality_class,
    is_active,
    current_year
FROM main
ORDER BY actorid, current_year
ON CONFLICT (actorid, current_year)
DO UPDATE SET           -- Update if data exists
    films = EXCLUDED.films,
    quality_class = EXCLUDED.quality_class,
    is_active = EXCLUDED.is_active;


-- CUMULATIVE TABLE DESIGN QUERY INSERT
-- ONLY PROCESS NEW DATA
-- DO NOT PROCESS HISTORICAL DATA
-- INTERVAL SCHEDULING
INSERT INTO actors
WITH yesterday AS (
    SELECT * FROM actors
    WHERE current_year = 2021
),
today AS (
    SELECT * FROM actor_films
    WHERE year = 2022
),
combined AS (
    SELECT 
        COALESCE(t.actor, y.actor) AS actor,
        COALESCE(t.actorid, y.actorid) AS actorid,
        CASE WHEN y.films IS NULL THEN ARRAY[ROW(
            t.film,
            t.year,
            t.votes,
            t.rating,
            t.filmid)::films]
            WHEN t.year IS NOT NULL THEN y.films || ARRAY[ROW(
            t.film,
            t.year,
            t.votes,
            t.rating,
            t.filmid)::films]
            ELSE y.films
        END AS films,
        t.year IS NOT NULL AS is_active,
        COALESCE(t.year, y.current_year + 1) AS current_year
    FROM today t
    FULL OUTER JOIN yesterday y
        ON t.actor = y.actor
),
main AS (
SELECT 
    actor,
    actorid,
    films,
    CASE 
        WHEN (films[CARDINALITY(films)]).rating > 8 THEN 'star'
        WHEN (films[CARDINALITY(films)]).rating > 7 THEN 'good'
        WHEN (films[CARDINALITY(films)]).rating > 6 THEN 'average'
        ELSE 'bad'
    END::quality_class AS quality_class,
    is_active,
    current_year
FROM combined
)
SELECT
    DISTINCT ON (actorid, current_year)
    actor,
    actorid,
    films,
    quality_class,
    is_active,
    current_year
FROM main
ORDER BY actorid, current_year
ON CONFLICT (actorid, current_year)
DO UPDATE SET           -- Update if data exists
    films = EXCLUDED.films,
    quality_class = EXCLUDED.quality_class,
    is_active = EXCLUDED.is_active;
