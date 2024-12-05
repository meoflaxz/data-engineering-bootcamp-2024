# Advanced Apache Spark

#### Spark Server vs Spark Notebooks

- Spark Server (Airbnb)
    - Every run is fresh, things get uncached automatically
    - Session run until complete, then killed
    - Nice for testing
    - Gets slower sometimes, but sometimes better
    - Can take a couple minutes to upload JAR (spark submit)

- Spark Notebook (Netflix)
    - You have to start and stop spark server yourself
    - Make sure to call unpersist()
    - Notebook sometimes not reflecting what will happen in production

#### Databricks considerations

- Consideration
    - Databricks should be connected with Github
        - PR review process for every change
        - CI/CD check

#### Caching and Temporary Views

- Temporary views
    - Always get recomputed unless cached
- Caching
    - Storage levels
        - MEMORY_ONLY
        - DISK_ONLY
        - MEMORY_AND_DISK (the default)
    - Caching really only is good if it fits into memory
        - Otherwise there's probably a staging table in your pipeline you're missing
    - In notebooks
        - Call unpersist() when you're done otherwise the cached data will just hang out
            - Cause spark will just skip the computation if there's cache, which is not really mirrors production
    - Generally avoid disk caching, consider staging table for edge case if there is a situation where disk caching is needed.

#### Caching vs Broadcast

- Caching
    - Stores pre-computed values for re-use
    - Stays partitioned
- Broadcast Join
    - Small data that gets cached and shipped in entirety to each executor

#### Broadcast JOIN Optimization

- Broadcast JOIN prevent shuffle
- The threshold is set
    - spark.sql.autoBroadcastJoinThreshold
    - default value is 10MB, can crank this up as long as you have enough memory, but definitely not tons of GB.
    - You can explicitly wrap a dataset with broadcast(df) too
     - Used this if you do not want to set threshold
     - Now you dont have to update the code every time the dataset goes a little bigger
- So either set the threshold really high or wrap the broadcast dataframe

#### UDF (User Defined Functions) between Python and Scala

- PySpark quirks vs Scala
    - More recent optimization of Apache Arrow have helped PySpark UDFs become more inline with Scala
    - Generally using Python for Spark will be less performant due to translation layer instead of using direct native Scala
- Using Dataset API, you dont even need to use UDF, you can use pure Scala functions instead
- But obviously Scala has a learning curve, it is not as simple as Python

[Spark UDF — Deep Insights in Performance](https://medium.com/quantumblack/spark-udf-deep-insights-in-performance-f0a95a4d8c62)

#### DataFrame vs Dataset vs SparkSQL

- Dataset is Scala only
- DataFrame vs SparkSQL
    - DataFrame is more suited for pipelines that are more hardened and less likely to experience change
        -   Make things more testable
    - SparkSQL is better for pipelines that are used in collaboration with data scientist
        -   Better if you want to change things quickly
    - Dataset is best for pipelines that requiret unit and integration tests.
        - Allow you to create fake schema easily
        - Dataset API has better way to handle NULL
        - Compared to DataFrame and SparkSQL, if NULL it will return NULL, but in Dataset you have to explicitly model whether column can be NULL
        - In a way, it enforces better quality control in the pipeline

#### Parquet

- An amazing file format
    - Run-length enconding allows for powerful compression
- Don't use global.sort() EVER
    - Painful, slow, annoying
- Use .sortWithinPartitions()
    - Parallelizable, gets you good distribution

#### Spark Tuning

- Executor Memory
    - Dont just set to 16GB and call it a day, wastes a lot
- Driver Memory
    - Only needs to be bumped up if:
        - You're calling df.collect()
        - Have a very complex job
- Shuffle Partitions
    - Default is 200
    - Aim for ~100MBs per partition to get the right sized output datasets
- AQE (adaptive query execution) - Skewed data
    - Helps with skewed datasets, wasteful if the dataset isn't skewed