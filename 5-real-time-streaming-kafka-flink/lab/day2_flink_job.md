# WINDOWING ON APACHE FLINK


- Tumble.over(lit(5).minutes).on(col("window_timestamp")).alias("w")
    - Window over 5 minutes
    - Based on the timestamp (must be in timestamp format)
    - Can be aliased with "w" to refer back again afterwards