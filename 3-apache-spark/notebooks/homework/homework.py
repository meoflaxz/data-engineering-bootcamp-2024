from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col
from pyspark.storagelevel import StorageLevel

spark = (SparkSession.builder 
        .appName("Homework") 
        .config("spark.executor.memory", "4g") 
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.files.maxPartitionBytes", "134217728") 
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") 
        .config("spark.dynamicAllocation.enabled", "true") 
        .config("spark.dynamicAllocation.minExecutors", "1") 
        .config("spark.dynamicAllocation.maxExecutors", "50")
        .getOrCreate())


from IPython.core.display import HTML
display(HTML("<style>pre { white-space: pre !important; }</style>"))

# import matches
matches_bucketed = spark.read.option("header", "true") \
                .option("inferSchema", "true") \
                .csv("/home/iceberg/data/matches.csv")

# import match_detail
match_details_bucketed = spark.read.option("header", "true") \
                .option("inferSchema", "true") \
                .csv("/home/iceberg/data/match_details.csv")

# import medals_matches_players
medals_matches_players_bucketed = spark.read.option("header", "true") \
                .option("inferSchema", "true") \
                .csv("/home/iceberg/data/medals_matches_players.csv")

# import medals
medals_bucketed = spark.read.option("header", "true") \
                .option("inferSchema", "true") \
                .csv("/home/iceberg/data/medals.csv") 

# disabling broadcast join threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

matches_bucketedDDL = """CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
                         match_id STRING,
                         is_team_game BOOLEAN,
                         playlist_id STRING,
                         completion_date TIMESTAMP
                     )
                     USING iceberg
                     PARTITIONED BY (completion_date, bucket(16, match_id));
                     """
spark.sql(matches_bucketedDDL)

# Get distinct completion dates
distinctDates = matches_bucketed.select("completion_date").distinct()

# Process data in chunks based on completion_date
for row in distinctDates.collect():
    date = row["completion_date"]
    filteredMatches = matches_bucketed.filter(col("completion_date") == date)
    
    # Repartition and persist the filtered data
    optimizedMatches = filteredMatches \
        .select("match_id", "is_team_game", "playlist_id", "completion_date") \
        .repartition(16, "match_id") \
        .persist(StorageLevel.MEMORY_AND_DISK)
    
    # Write the data to the table
    optimizedMatches.write \
        .mode("append") \
        .bucketBy(16, "match_id") \
        .partitionBy("completion_date") \
        .saveAsTable("bootcamp.matches_bucketed")
    
    # Unpersist the DataFrame to free up memory
    optimizedMatches.unpersist()

# Verify the data in the table
result = spark.sql("SELECT * FROM bootcamp.matches_bucketed")
result.show()


# matches_detail
match_details_bucketedDDL = """CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
                         match_id STRING,
                         player_gamertag STRING,
                         did_win INTEGER,
                         team_id INTEGER
                     )
                     USING iceberg
                     PARTITIONED BY (player_gamertag, bucket(16, match_id));
                     """
spark.sql(match_details_bucketedDDL)

# Get distinct completion dates
distinctPlayer_gamertag = match_details_bucketed.select("player_gamertag").distinct()

# Process data in chunks based on completion_date
for row in distinctPlayer_gamertag.collect():
    gamertag  = row["player_gamertag"]
    filteredMatches = match_details_bucketed.filter(col("player_gamertag") == gamertag )
    
    # Repartition and persist the filtered data
    optimizedMatches = filteredMatches \
        .select("match_id", "player_gamertag", "did_win", "team_id") \
        .repartition(16, "match_id") \
        .persist(StorageLevel.MEMORY_AND_DISK)
    
    # Write the data to the table
    optimizedMatches.write \
        .mode("append") \
        .bucketBy(16, "match_id") \
        .partitionBy("player_gamertag") \
        .saveAsTable("bootcamp.match_details_bucketed")
    
    # Unpersist the DataFrame to free up memory
    optimizedMatches.unpersist()

# Verify the data in the table
result = spark.sql("SELECT * FROM bootcamp.match_details_bucketed")
result.show()