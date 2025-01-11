

-- QUERY 1 - query that does state change tracking 
WITH previous_season AS (
    SELECT * FROM players_scd
    WHERE current_season = '2002'
),
current_season AS (
    SELECT * FROM players_scd
    WHERE current_season = '2003'
)
SELECT
    COALESCE(c.player_name, p.player_name) AS player_name,
    COALESCE(p.start_season, c.current_season) AS first_active_season,
    COALESCE(c.current_season, CAST(p.start_season AS INTEGER)) AS last_active_season,
    CASE
        WHEN p.player_name IS NULL THEN 'New'
        WHEN p.is_active IS TRUE AND c.is_active IS TRUE THEN 'Continued Playing'
        WHEN p.is_active IS TRUE AND c.is_active IS FALSE THEN 'Retired'
        WHEN p.is_active IS FALSE AND c.is_active IS TRUE THEN 'Returned from Retirement'
        ELSE 'Stayed Retired'
    END AS year_active_state,
    COALESCE(CAST(c.current_season AS INTEGER), p.current_season) AS year
FROM current_season c
FULL OUTER JOIN previous_season p
    ON c.player_name = p.player_name
ORDER BY player_name;



-- QUERY 2 - query that uses GROUPING SETS to do efficient aggregation of game_details

SELECT 
    CASE
        WHEN GROUPING(game_details.player_name) = 0
            AND GROUPING(game_details.team_city) = 0 THEN 'player_name__team_city'
        WHEN GROUPING(game_details.player_name) = 0 
            AND GROUPING(player_seasons.season) = 0 THEN 'player_name__season'
        WHEN GROUPING(game_details.team_city) = 0 THEN 'team_city'
        ELSE 'Overall'
    END AS aggregation_level,
    COALESCE(game_details.player_name, 'Overall') AS player_name,
    COALESCE(game_details.team_city, 'Overall') AS team_city,
    COALESCE(CAST(player_seasons.season AS TEXT), 'Overall') AS season,
    SUM(game_details.pts) AS total_points
FROM game_details 
JOIN player_seasons 
    ON game_details.player_name = player_seasons.player_name
GROUP BY GROUPING SETS (
    (game_details.player_name, game_details.team_city),  -- Player and team aggregation
    (game_details.player_name, player_seasons.season),  -- Player and season aggregation
    (game_details.team_city)  -- Team-only aggregation
)
ORDER BY aggregation_level;


-- QUERY 3 - Who scored the most points playing for one team
WITH total_points AS (
    SELECT 
        game_details.player_name,
        game_details.team_city,
        SUM(game_details.pts) AS total_points
    FROM game_details
    JOIN player_seasons 
        ON game_details.player_name = player_seasons.player_name
    GROUP BY game_details.player_name, game_details.team_city
)
SELECT player_name, team_city, total_points
FROM total_points
ORDER BY total_points DESC
LIMIT 1;



-- QUERY 4 - Who scored the most points in one season

WITH total_points_per_season AS (
    SELECT 
        game_details.player_name,
        player_seasons.season,
        SUM(game_details.pts) AS total_points
    FROM game_details
    JOIN player_seasons 
        ON game_details.player_name = player_seasons.player_name
    GROUP BY game_details.player_name, player_seasons.season
)
SELECT player_name, season, total_points
FROM total_points_per_season
ORDER BY total_points DESC
LIMIT 1;


-- QUERY 5 Which team has the most won the most games
WITH win_team AS (
    SELECT
        game_id,
        team_city,
        plus_minus,
        CASE
            WHEN plus_minus > 0 THEN 1  -- Winning team
            ELSE 0  -- Losing team
        END AS Win
    FROM game_details
    WHERE plus_minus IS NOT NULL
)
SELECT team_city, SUM(Win) 
FROM win_team
GROUP BY team_city
ORDER BY SUM(Win) DESC 
LIMIT 1;


-- QUERY 6 - What is the most games a team has won in a 90 game stretch?

WITH with_previous AS (
    SELECT
        games.game_date_est AS date,
        game_details.team_city AS team,
        CASE
            WHEN games.home_team_wins THEN 1
            WHEN NOT games.home_team_wins THEN 0
            ELSE NULL
        END AS is_winning_team
    FROM games
    LEFT JOIN game_details
        ON games.game_id = game_details.game_id
),
with_streaks AS (
    SELECT *,
        -- Calculate rolling wins over the last 90 games for each team
        SUM(is_winning_team) OVER (
            PARTITION BY team 
            ORDER BY date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS rolling_wins
    FROM with_previous
)
SELECT 
    MAX(rolling_wins) AS most_wins_in_90_game_stretch
FROM with_streaks;



-- Query 7 - How many games in a row did LeBron James score over 10 points a game?

WITH streaks AS (
    SELECT
        games.game_date_est AS date,
        game_details.player_name AS player,
        pts AS points,
        ROW_NUMBER() OVER (ORDER BY games.game_date_est) 
        - ROW_NUMBER() OVER (PARTITION BY (CASE WHEN pts > 10 THEN 1 ELSE 0 END) ORDER BY games.game_date_est) AS streak_id
    FROM games
    LEFT JOIN game_details
        ON games.game_id = game_details.game_id
    WHERE game_details.player_name = 'LeBron James'
)
-- Now count the longest streak where points > 10
SELECT
    COUNT(*) AS streak_length,
    MIN(date) AS streak_start_date,
    MAX(date) AS streak_end_date
FROM streaks
WHERE points > 10
GROUP BY streak_id
ORDER BY streak_length DESC
LIMIT 1;



