## Idempotent pipelines are CRITICAL

Video lecture - [Youtube](https://www.youtube.com/watch?v=emQM9gYh0Io)

- Your pipeline should produces the same results regardless of when it's ran
- Idempotent meaning - denoting an element of a set which is unchanged in value when multiplied or otherwiser operated on by itself.

## Pipelines should produce the same results

- Regardless of the day you run it
- Regardless of how many times you run it
- Regardless of hour that you run it
- That is the idea of idempotency

## Why is troubleshooting non-idempotent pipelines hard?
- Silent failure! - non reproducable data
- Only show when you get data inconsistencies and data analyst yells at you
- Inconsistencies will bleed out throughout data warehouse

## What can make a pipeline not idempotent?

- INSERT INTO without TRUNCATE
    * Use MERGE or INSERT OVERWRITE everytime please
    * Zach despises using INSERT INTO at all
    * MERGE/UPSERT - takes new data and merge into old data. When run again, no duplicates will be created, cause everything matches. You can run them many times as you want, data will be same.
    * INSERT OVERWRITE - sometimes even better, instead of matching, it will overwrite the partition. not a row-by-row delete operation. it simply deletes existing row and inserts new ones based on the SELECT query.
        ```sql
        INSERT OVERWRITE TABLE sales PARTITION (year = 2024)
        SELECT * FROM new_sales_data_2024;
        ```
- Using start_date > without a corresponding end_date <
    * For example, different day of running pipeline will produce different results cause there are no end_date <
- Not using a full set of partition sensors
    * pipeline might run when there is no/partial data
    * this just means pipeline that is still run eventhough dependencies is not ready
- Not using depends_on_past for cumulative pipelines
    * need to setup sequential pipeline in order you have the accurate cumulative pipelines
    * this also means you cannot run them in parallel, but sequentially
- Relying on "latest" partition of a not properly modeled SCD table
    * Bad idea to rely on DAILY DIMENSION and latest partition

## The Pains of Not Having Idempotent Pipelines

- Backfilling causes inconsistencies between old and restated data
- Hard to troubleshoot bugs
- Unit testing cannot replicate the production behaviour
- Silent failures

## Shoud you model as Slowly Changing Dimensions (SCD)?

- Article on why SCD suck - written by creator Airflow. his idea is that storage is so cheap so just bring all the daily dimension while at the same time the cost to fix data quality issue is high.
- How can we model the SCD for table?
    - Latest snapshot (taking latest attribute) - but this is not idempotent since backfilling with SCD will bring wrong data
    - Daily/Monthly/Yearly snapshot - better than latest snapshot
    - SCD - instead of we 365 rows for the same attributes, let's just have one rows that says when the attribute change.

## Why do dimensions/columns change?

- Someone decides they hate iPhone and want Android now
- Someone migrates from US to another country
- ETC ETC

## How you can model dimensions that change?

- Singular snapshots - if backfill, and you only have latest value, then it might be wrong for the old data. for the most part never do this since this is NOT IDEMPOTENT
- Daily partitioned snapshots - partitioned by day
- SCD Types 1, 2, 3

## Types of Slowly Changing Dimensions
- Type 0
    - No SCD
    - Things that dont change, no temporal element
- Type 1
    - Only care about the latest value
    - Overwrites everything
    - NEVER USE THIS TYPE cause not idempotent
- Type 2
    - The only type of SCD that is purely idempotent
    - Care about what value was from start_date to end_date
    - Current values usually have either an end_date that is NULL or far into the future (9999-12-31)
    - Hard to use: Since there are more than 1 row per dimension, need to be careful about filtering for time
    - FAVOURITE type of SCD
- Type 3
    - Only care about original and current
    - Benefits is you have only 1 row per dimension
    - Drawbacks, you lose everything that happened between original and current
    - Not idempotent cause lose the history data

## Which types are idempotent?
- Type 0 and Type 2 are idempotent
    - Type 0 because the values are unchanging
    - Type 2, just need to be careful with how you use start_date and end_date syntax
- Type 1 isnt
    - Only getting the dimensions as it is now, no from history
- Type 3 isnt
    - If you backfill, impossible to know when to pick original vs current

## SCD Type 2 Loading

- 2 ways to load SCD Type 2
    - Load entire history in one query
        - Inefficient but nimble
        - 1 query and you are done
    - Incremental load after previous SCD is generated
        - Has the same depends_on_past constraint
        - Do it cumulatively
        - Efficient but cumbersome
- These 2 is not better over each other

