#### setup docker compose
- make job

#### run job
- make homework_job

#### sql script
- What is the average number of web events of a session from a user on Tech Creator?

#### run query in postres table
```sql
     SELECT
        event_hour,
        ip,
        host,
        AVG(num_hits) as total_hits
    FROM processed_events_web_traffic
    WHERE host LIKE '%techcreator%'
    GROUP BY event_hour, ip, host;
```

#### sql script
- Compare results between different hosts 
```sql
    SELECT
    DISTINCT
        host,
        AVG(num_hits) as total_hits
    FROM processed_events_web_traffic
    GROUP BY host;
```