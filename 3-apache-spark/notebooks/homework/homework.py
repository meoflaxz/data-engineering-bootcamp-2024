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

# import maps
maps_bucketed = spark.read.option("header", "true") \
                .option("inferSchema", "true") \
                .csv("/home/iceberg/data/maps.csv") 

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

medals_matches_players_bucketedDDL = """CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players_bucketed (
                         match_id STRING,
                         player_gamertag STRING,
                         match_id INTEGER,
                         count INTEGER
                     )
                     USING iceberg
                     PARTITIONED BY (player_gamertag, bucket(16, match_id));
                     """
spark.sql(medals_matches_players_bucketedDDL)

# Get distinct player_gamertag
distinctPlayer_gamertag = medals_matches_players_bucketed.select("player_gamertag").distinct()

# Process data in chunks based on completion_date
for row in distinctPlayer_gamertag.collect():
    gamertag  = row["player_gamertag"]
    filteredMatches = medals_matches_players_bucketed.filter(col("player_gamertag") == gamertag )
    
    # Repartition and persist the filtered data
    optimizedMatches = filteredMatches \
        .select("match_id", "player_gamertag", "match_id", "count") \
        .repartition(16, "match_id") \
        .persist(StorageLevel.MEMORY_AND_DISK)
    
    # Write the data to the table
    optimizedMatches.write \
        .mode("append") \
        .bucketBy(16, "match_id") \
        .partitionBy("player_gamertag") \
        .saveAsTable("bootcamp.medals_matches_players_bucketed")
    
    # Unpersist the DataFrame to free up memory
    optimizedMatches.unpersist()

# Verify the data in the table
result = spark.sql("SELECT * FROM bootcamp.medals_matches_players_bucketed")
result.show()

medalsDDL = """CREATE TABLE IF NOT EXISTS bootcamp.medals_bucketed (
                         medal_id STRING,
                         name STRING,
                         difficulty INTEGER
                     )
                     USING iceberg
                     PARTITIONED BY (name, bucket(16, medal_id));
                     """
spark.sql(medalsDDL)

# Get distinct medal
distinctMedal_name = medals_bucketed.select("name").distinct()

# Process data in chunks based on completion_date
for row in distinctMedal_name.collect():
    medal  = row["name"]
    filteredMatches = medals_matches_players_bucketed.filter(col("name") == medal )
    
    # Repartition and persist the filtered data
    optimizedMatches = filteredMatches \
        .select("medal_id", "name", "difficulty") \
        .repartition(16, "medal_id") \
        .persist(StorageLevel.MEMORY_AND_DISK)
    
    # Write the data to the table
    optimizedMatches.write \
        .mode("append") \
        .bucketBy(16, "medal_id") \
        .partitionBy("name") \
        .saveAsTable("bootcamp.medals_bucketed")
    
    # Unpersist the DataFrame to free up memory
    optimizedMatches.unpersist()

# Verify the data in the table
result = spark.sql("SELECT * FROM bootcamp.medals_bucketed")
result.show()

## BROADCAST JOIN FOR MEDALS AND MAPS
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1000000000000")


maps_bucketedDDL = """CREATE TABLE IF NOT EXISTS bootcamp.maps_bucketed (
                         mapid STRING,
                         name STRING
                     )
                     USING iceberg
                     PARTITIONED BY (name, bucket(16, mapid));
                     """
spark.sql(maps_bucketedDDL)

# Get distinct map
distinctMap_name = maps_bucketed.select("name").distinct()

# Process data in chunks based on completion_date
for row in distinctMap_name.collect():
    medal  = row["name"]
    filteredMatches = maps_bucketed.filter(col("name") == medal )
    
    # Repartition and persist the filtered data
    optimizedMatches = filteredMatches \
        .select("mapid", "name") \
        .repartition(16, "mapid") \
        .persist(StorageLevel.MEMORY_AND_DISK)
    
    # Write the data to the table
    optimizedMatches.write \
        .mode("append") \
        .bucketBy(16, "medal_id") \
        .partitionBy("name") \
        .saveAsTable("bootcamp.maps_bucketed")
    
    # Unpersist the DataFrame to free up memory
    optimizedMatches.unpersist()

# Verify the data in the table
result = spark.sql("SELECT * FROM bootcamp.maps_bucketed")
result.show()