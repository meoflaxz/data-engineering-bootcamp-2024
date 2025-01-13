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
    - taskmanager.numberOfTaskSlots -> Maximum concurrent tasks
        - specific to TaskManagers
    - parallelism.default -> By default, Flink will try to execute this amount of job
        - applied at job level