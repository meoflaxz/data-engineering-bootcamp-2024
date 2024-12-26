# Streaming Needs a Lot of Pieces to Work

#### HTTP Interceptor to intercept request
- API event middleware - function createAPIEventMiddleware(res, res, next)

#### Kafka Producer to produce data as log
- function name - sendMessageToKafka
- provide topic and message

#### Big Computing Architecture for Streaming Pipeline
- Lambda
- Kappa

#### Lambda Architecture
- Optimize for latency and correctness
- Double pipeline/codebase, 1 batch and 1 streaming
    - Batch for backup if streaming fail
- Double code so a little pain in terms of complexity
- But, easier data quality check on batch side
- Most companies do this

#### Kappa Architecture
- Pros
    - Least complex
    - Great latency wins
- Can be painful when need to read lot of history
    - Backfilling is hard
    - Need to read things sequentially
- Delta Lake, Iceberg, Hudi making this architecture much more viable
    - Due to partition so can have data separation in file table format
- Uber as the company that mainly do kappa

#### Flink UDFS
- UDFs generally speaking won't perform as well as builted-in functions
    - Use UDFs for custom transformation, integrations, etc
    - Python UDF are going to be even less performant since Flink is not native Python!
        - Compared to Scala or Java at least

#### Flink Windows (Time/session frame to capture data)
- Data-driven windows
    - Count
- Time-Driven Windows
    - Tumbling
    - Sliding
    - Session

#### Flink Windows (Count)
- Windows stays open until N number of events occur
    - kind of like sql window partition by, eg. flink is based on key by
- Useful for funnels that have a predictable number of events
- Can specify a timeout since not everybody will finish the funnel
    - Not event will eventually being followed through

#### Flink Windows Tumbling (Time)
- Like batch almost similar
- Fixed size
- No overlap data
- Similar to hourly data
- Great for chunking data

#### Flink Windows Sliding (Time)
- OVERLAPPING
- Captures more windows
- Fixed window size with overlapping window slide across 2 windows
- Good for finding peak use windows
- Good at handling across midnight exceptions in batch
- continuous monitoring scenario (keypoint is peak)
    - real time fraud detection
    - moving averages for financial data
    - network monitoring for anomaly
    - IoT rolling metric analysis
- Provides smoother transitions and more frequent updates compared to tumbling

#### Flink Windows Session (Time)
- Based on activity timeouts rather than fixed durations
- Common use case:
    - User behaviour tracking
    - shopping session ends after 30 minutes without clicks
- You have to specify when is the timeout to encapsulate the window session

#### Allowed Lateness vs Watermarking (Handling of late event)
- Watermarks
    - Defines when the computational windows will execute
    - Helps define ordering of events that arrive out-ouf-order
    - Handles idlesness too
- Allowed Lateness
    - Usually set to 0
    - Allows for reprocessing of events that fall within late window
    - CAUTION: WILL GENERATE/MERGE WITH OTHER RECORDS
