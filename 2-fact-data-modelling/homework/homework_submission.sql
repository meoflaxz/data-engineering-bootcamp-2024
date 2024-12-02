
--Deduplication of game_details table
WITH deduped AS (
    SELECT
        *, ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) AS row_num
    FROM game_details
)
SELECT
    game_id,
    team_id,
    player_id,
    start_position,
    comment,
    min,
    fgm,
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk,
    "TO" AS to,
    pf,
    pts,
    plus_minus
FROM deduped
--  take only one copy of each PARTITION
-- removing duplicates
WHERE row_num = 1;