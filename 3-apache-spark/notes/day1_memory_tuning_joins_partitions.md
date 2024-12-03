# APACHE SPARK

#### What is Apache Spark?

- A distributed computing framework to process large data very efficient
- Spark winning the big data processing framework beating the Mapreduce from Hadoop

#### Why Spark?

- Spark leverages RAM (way faster than hive, mapreduce)
- but Spark will spill to disk if RAM is not enough, that we do not want cause basically we are going the ancient way of doing big data
- Spark can read anything / storage agnostic, allowing decouple of compute and storage
- Huge community!

#### When Spark is not so good?

- Nobody else in the company knows Spark
- Company already uses somthing else a lot
    - eg bigquery, snowflake, redshift

#### How Spark Work?

- 3 main pieces
    - The plan
    - The driver
    - The executors

#### The Plan (what to do)

- The transformation you describe in Python, Scala, or SQL
- Plan is evaluated lazily
    - Only happens when it needs to
- When does execution needs to happen?
    - Writing output
    - When part of plan depends on the data itself
        - eg dataframe.collect()

#### The Driver (The configurer)

- Driver reads plan
- Important Spark driver settings
- 2 most important
    - spark.driver.memory - where you'll experience OOM, may need to bump higher if you are doing complex jobs
    - spark.drive.memoryOverheadFactor - the driver also needs some non-heap related memory usually 10%
- Other driver settings shouldnt care much
- Driver needs to determine a few things
    - When to start job and stop being lazy
    - How to JOIN datasets
    - How much parallelism each step needs

#### The Executors (Who is doing the work)

- The driver passes the plan to the executors
    - spark.executor.memory - how much memory each executor gets. low number may cause spark to spill to disk
        - should experiment with different RAM values
        - dont just simply leave it at 16gb and just called it a day.
    - spark.executor.cores - how many task can happen on each machine (default is 4, shouldnt go higher than 6)
        - running task at higher cores will results to bottleneck performance which happen when too many task are running at the same time
        - more likely to OOM if higher
    - spark.executor.memoryOverheadFactor - the executor also needs some non-heap related memory usually 10%
        - bump up for jobs with lots of UDF n complexity
        - you can bump this up without bumping the memory itself - independent with no additional cost

#### Types of JOIN in Spark

- Shuffle sort-merge JOIN
    - Default JOIN strategy
    - least performance
    - most useful cause most versatile
    - works best when both sides are large
- Broadcast Hash Join
    - works well if left side of join is small
    - your best friend in a lot of cases
    - join WITHOUT shuffle
    - works well for small data
- Bucket JOIN
    - join without shuffle
    - bucket for the power of 2
    - how you pick number of buckets?
        - following the data size, not too small, not too big

#### How does the shuffle work?

- As data goes up, shuffle will be such painful
- shuffle partitions and parallelism are linked
    - spark.sql.shuffle.partitions and spark.default.paralleism
    - just use spark.sql.shuffle.partitions! dont use RDD API
- is shuffle good or bad?
    - at low t0 medium
        - really good and makes our lives easier
    - at high volume > 10TB
        - painful
        - at netlix, shuffle killed the IP enrichment pipeline

#### Minimizing shuffle at high volume

- bucket the data if multiple joins or aggregations are happening downstream
- sparkk has ability to bucket data to minimize or elimiate the need for shuffle when doing JOINs
- bucket join are very efficient but have drawbacks
- main drawbacks is the the initial parallelism = number of buckets
- bucket joins only work if the 2 tables number of bucket are multiples of each other
    - always use power of 2 for number of buckets

#### Shuffle and Skew

- sometimes some partitions have more data than others
- Why?
    - not enough partition
    - the natural way the data is
        - beyonce gets a lot more notification that the average facebook user
- skew symptoms
    - jobs will be at 99% and then fail

#### How to tell if your data is skewed?

- most common is a job getting at 99%, taking forever and failing
- a more scientific way, to a box n whisker plot of data to see any extreme outlier

#### Handling Skew

- Adaptive query execution - only in spark 3+
    - set spark.sql.adaptive.enabled = True
    - dont just set it to true all the time, cause spark has to compute extra statistics to jobs
- Salting GROUP BY - best option before spark 3
 
#### Spark on Databrick vs regular Spark

- should you use notebook?
    - managed spark (databrick) - yes
    - regular spark - only for poc
- how to test job?
    - managed spark (databrick) - run notebook
    - regular spark - spark submit
- version control
    - managed spark (databrick) - git or notebook versioning
    - regular spark - git
- generally notebook is easier but invite not good engineering practice - test, integration

#### Spark query plan

- Use explain() on dataframe
    - will show join strategy in spark
    - same like EXPLAIN in databases

#### How can Spark read data?

- From the data lake
    - delta lake, apache iceberg, hive metastore
- From RDBMS
    - postgres, oracle, etc
- From API
    - make a rest call and turn into data
        - be careful because this usually happens on the driver
- From a flat file (CSV, JSON)

#### Spark output datasets

- Spark should always be partition on date
    - execution date of pipeline
    - also called ds partitioning