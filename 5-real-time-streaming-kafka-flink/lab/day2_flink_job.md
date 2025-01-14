# WINDOWING ON APACHE FLINK


- Tumble.over(lit(5).minutes).on(col("window_timestamp")).alias("w")
    - Window over 5 minutes
    - Based on the timestamp (must be in timestamp format)
    - Can be aliased with "w" to refer back again afterwards

- You can't window without Watermark
    - Watermark should exists in code

- docker compose image
    -   ```yaml
            environment:
            FLINK_PROPERTIES=
            jobmanager.rpc.address: jobmanager
            taskmanager.numberOfTaskSlots: 15
            parallelism.default: 3
        ```
    - taskmanager.numberOfTaskSlots -> resource capacity per TaskManager
        - specific to TaskManagers
        - think resources
    - parallelism.default -> how many tasks your job will try to execute in parallel
        - depends on the available task slots across all TaskManagers

- if upstream struggle to process data, consider only process latest data
    - 'scan.startup.mode' = 'latest-offset' instead of 'earliest-offset'
    - so you are not processing all data, only the newer one

- How to know if things should be run in flink, spark streaming, batch?
    - our use case is 5 minutes aggregation data, that is why flink is used
    - if used spark, it is not suitable due spark needed to startup first and the job runs every 5 minutes
    - flink will run more efficient
    - if hourly job, sure spark can be used
    - batch is better for bigger window
    - flink need more ram to hold the window 