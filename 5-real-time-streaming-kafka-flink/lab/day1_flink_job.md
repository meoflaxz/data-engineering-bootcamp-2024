

#### INITIAL SETUP // MAIN FLOW 
- log_processing()
	- flink have option to process batch using .in_batch_mode
	- eg. settings = EnvironmentSettings.new_instance().in_batch_mode().build()
	- env.enable_checkpointing(10 * 1000) -> 10 seconds, this is in milliseconds timing
	- StreamTableEnvironment.create() - create table for stream processing, same thing like dataframe
	- create_temporary_function() - create custom UDF to be used in SQL

#### SOURCE
- create_events_source_kafka()
	- watermark can be configured in kafka table using SQL
	- kafka topics and database table is kind of similar
	- offset is setup here
	- data format also here - JSON is used because data is retrieved in JSON format

#### SINK (DESTINATION)
    - the create table statement for postgre sink do not actually create table, it is there to let flink aware of the schema

#### FLINK UI
- task slot - maximum jobs at a time
- "make job" - start job based on job folder
- latest offset will only read new data
- kafka sink generally more performant because of high throughput compared to postgres sink