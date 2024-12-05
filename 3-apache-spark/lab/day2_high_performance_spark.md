# Advanced Sparks Labs

#### Comparison between Dataset, Dataframe and SparkSQL


- FILTERING DATA

    - Dataset. Dataset is winning slightly because of the quality enforcement

        ```java
            val filteredViaDataset = events.filter(event => event.user_id.isDefined && event.device_id.isDefined)
        ```

    - DataFrame

        ```java
            val filteredViaDataFrame = events.toDF().where($"user_id".isNotNull && $"device_id".isNotNull)
        ```

    - SparkSQL

        ```java
            val filteredViaSparkSql = sparkSession.sql("SELECT * FROM events WHERE user_id IS NOT NULL AND device_id IS NOT NULL")
        ```

- JOINING DATA

    - Dataset

        ```java
            // This will fail if user_id is None
            val combinedViaDatasets = filteredViaDataset
                .joinWith(devices, events("device_id") === devices("device_id"), "inner")
                .map { case (event: Event, device: Device) => 
                    EventWithDeviceInfo(
                        user_id = event.user_id.getOrElse(-1),
                        device_id = device.device_id,
                        browser_type = device.browser_type,
                        os_type = device.os_type,
                        device_type = device.device_type,
                        referrer = event.referrer.getOrElse("unknown"),
                        host = event.host,
                        url = event.url,
                        event_time = event.event_time
                    )
                }
                .map { row => 
                    row.copy(browser_type = row.browser_type.toUpperCase)
                }
        ``` 

    - DataFrame

        ```java
            // DataFrames give up some of the intellisense because you no longer have static typing
            val combinedViaDataFrames = filteredViaDataFrame.as("e")
                //Make sure to use triple equals when using data frames
                .join(devices.as("d"), $"e.device_id" === $"d.device_id", "inner")
                .select(
                $"e.user_id",
                $"d.device_id",
                $"d.browser_type",
                $"d.os_type",
                $"d.device_type",
                $"e.referrer",
                $"e.host",
                $"e.url",
                $"e.event_time"
                )
        ```
    
    - SparkSQL

        ```java
            //Creating temp views is a good strategy if you're leveraging SparkSQL
            filteredViaSparkSql.createOrReplaceTempView("filtered_events")
            val combinedViaSparkSQL = spark.sql(f"""
                SELECT 
                    fe.user_id,
                    d.device_id,
                    d.browser_type,
                    d.os_type,
                    d.device_type,
                    fe. referrer,
                    fe.host,
                    fe.url,
                    fe.event_time
                FROM filtered_events fe 
                JOIN devices d ON fe.device_id = d.device_id
            """)