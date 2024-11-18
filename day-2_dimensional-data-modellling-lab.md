# Building Slowly Changing Dimensions (SCD) Data Modelling

- Data will be based on data dump in the postgresql database. Follow the step [here](https://github.com/meoflaxz/data-engineering-bootcamp-2024/tree/main/1-dimensional-data-modeling) to setup the postgres using docker.
- Video lab - [Youtube]()

- We are gonna create a new players schema for this table. This also mean you have to drop the previous one it has already created.

    ```sql
        DROP TABLE players;
    ```

    ```sql
        CREATE TABLE players (
            player_name TEXT,
            height TEXT,
            college TEXT,
            country TEXT,
            draft_year TEXT,
            draft_round TEXT,
            draft_number TEXT,
            season_stats season_stats[],
            scoring_class scoring_class,
            years_since_last_season INTEGER,
            current_season INTEGER,
            is_active BOOLEAN,
            PRIMARY KEY (player_name, current_season));
    ```

    ```sql
        WITH years AS (
            SELECT *
            FROM generate_series(1996, 2022) AS season
        ),
        p AS (
            SELECT
                player_name,
                MIN(season) AS first_season
                FROM player_seasons
                GROUP BY player_name
        ),
        players_and_seasons AS (
            SELECT *
            FROM p
            JOIN years y
                ON p.first_season <= y.season
        ),
        windowed AS (
            SELECT
                ps.player_name, ps.season,
                ARRAY_REMOVE(
                    ARRAY_AGG(
                        CASE
                            WHEN p1.season IS NOT NULL THEN CAST(ROW(
                                                            p1.season,
                                                            p1.gp,
                                                            p1.pts,
                                                            p1.reb,
                                                            p1.ast
                                                        ) AS season_stats)
                        END ) OVER (PARTITION BY ps.player_name ORDER BY COALESCE(p1.season, ps.season)), null) AS seasons
            FROM players_and_seasons ps
            LEFT JOIN player_seasons p1
                ON ps.player_name = p1.player_name
                AND ps.season = p1.season
                ORDER BY ps.player_name, ps.season
        ),
        static AS (
            SELECT
                player_name,
                MAX(height) AS height,
                MAX(college) AS college,
                MAX(country) AS country,
                MAX(draft_year) AS draft_year,
                MAX(draft_round) AS draft_round,
                MAX(draft_number) AS draft_number
            FROM player_seasons ps
            GROUP BY player_name
        )
        SELECT
            w.player_name,
            s.height,
            s.college,
            s.country,
            s.draft_year,
            s.draft_round,
            s.draft_number,
            w.seasons AS season_stats,
            (seasons[CARDINALITY(seasons)]).pts,
            CASE
                WHEN (w.seasons[CARDINALITY(w.seasons)]).pts > 20 THEN 'star'
                WHEN (w.seasons[CARDINALITY(w.seasons)]).pts > 15 THEN 'good'
                WHEN (w.seasons[CARDINALITY(w.seasons)]).pts > 10 THEN 'average'
                ELSE 'bad'
            END::scoring_class AS scoring_class,
            w.season - (w.seasons[CARDINALITY(w.seasons)]).season as years_since_last_season,
            w.season,
            (w.seasons[CARDINALITY(w.seasons)]).season = w.season AS is_active
        FROM windowed w
        JOIN static s
            ON w.player_name = s.player_name
    ```