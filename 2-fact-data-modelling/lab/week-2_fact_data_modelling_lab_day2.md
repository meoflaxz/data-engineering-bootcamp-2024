# Buiding DATELIST Fact Data Modelling For Growth Analysis



- Building the users_cumulated table.

    ```sql
        CREATE TABLE users_cumulated (
            user_id TEXT,
            dates_active DATE[],
            date DATE,
            PRIMARY KEY (user_id, date)
        );
    ```

- Build the cumulative table design query insert to populate the table

    ```sql
        INSERT INTO users_cumulated
        WITH yesterday AS (
            SELECT * FROM users_cumulated
            WHERE date = DATE('2023-01-30')
        ),
        today AS (
            SELECT
                CAST(user_id AS TEXT) AS user_id,
                DATE(CAST(event_time AS DATE)) AS date_active
            FROM events
            WHERE DATE(CAST(event_time AS DATE)) = DATE('2023-01-31')
                AND user_id IS NOT NULL
            GROUP BY user_id, DATE(CAST(event_time AS DATE))
        )
        SELECT
            COALESCE(t.user_id, y.user_id) AS user_id,
            CASE
                WHEN y.dates_active IS NULL
                    THEN ARRAY[t.date_active]
                WHEN t.date_active IS NULL THEN y.dates_active
                ELSE ARRAY[t.date_active] || y.dates_active
            END AS dates_active,
            COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
        FROM today t
        FULL OUTER JOIN yesterday y
        ON t.user_id = y.user_id;
    ```

- Select table to see terusltes

    ```sql
        SELECT * FROM users_cumulated
        WHERE date = DATE('2023-01-30');
    ```