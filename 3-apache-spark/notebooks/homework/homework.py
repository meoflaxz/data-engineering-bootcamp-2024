from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, avg, count
from pyspark.storagelevel import StorageLevel

# Initialize Spark session
spark = (SparkSession.builder 
        .appName("Homework") 
        .config("spark.executor.memory", "6g") 
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.files.maxPartitionBytes", "134217728") 
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") 
        .config("spark.dynamicAllocation.enabled", "true") 
        .config("spark.dynamicAllocation.minExecutors", "1") 
        .config("spark.dynamicAllocation.maxExecutors", "50")
        .getOrCreate())

# List of CSV files and their corresponding Parquet paths
data_files = [
    {"csv_path": "/home/iceberg/data/matches.csv", "parquet_path": "/home/iceberg/data/matches.parquet"},
    {"csv_path": "/home/iceberg/data/match_details.csv", "parquet_path": "/home/iceberg/data/match_details.parquet"},
    {"csv_path": "/home/iceberg/data/medals_matches_players.csv", "parquet_path": "/home/iceberg/data/medals_matches_players.parquet"},
    {"csv_path": "/home/iceberg/data/medals.csv", "parquet_path": "/home/iceberg/data/medals.parquet"},
    {"csv_path": "/home/iceberg/data/maps.csv", "parquet_path": "/home/iceberg/data/maps.parquet"}
]

# Convert each CSV file to Parquet
for file in data_files:
    csv_path = file["csv_path"]
    parquet_path = file["parquet_path"]
    
    # Step 1: Read the CSV data
    df = spark.read.option("header", "true") \
                  .option("inferSchema", "true") \
                  .csv(csv_path)
    
    # Step 2: Convert CSV to Parquet
    df.write.mode("overwrite").parquet(parquet_path)
    print(f"Converted {csv_path} to {parquet_path}")

# Step 3: Read the Parquet data for further processing
matches_bucketed = spark.read.parquet("/home/iceberg/data/matches.parquet")
match_details_bucketed = spark.read.parquet("/home/iceberg/data/match_details.parquet")
medals_matches_players_bucketed = spark.read.parquet("/home/iceberg/data/medals_matches_players.parquet")
medals_bucketed = spark.read.parquet("/home/iceberg/data/medals.parquet")
maps_bucketed = spark.read.parquet("/home/iceberg/data/maps.parquet")

# Write DataFrames to bucketed tables with sorting within partitions
matches_bucketed.write \
    .mode("overwrite") \
    .bucketBy(16, "match_id") \
    .sortWithinPartitions("mapid") \
    .saveAsTable("matches_bucketed")

maps_bucketed.write \
    .mode("overwrite") \
    .bucketBy(16, "mapid") \
    .sortWithinPartitions("name") \
    .saveAsTable("bootcamp.maps_bucketed")

medals_bucketed.write \
    .mode("overwrite") \
    .bucketBy(16, "medal_id") \
    .sortWithinPartitions("name") \
    .saveAsTable("bootcamp.medals_bucketed")

match_details_bucketed.write \
    .mode("overwrite") \
    .bucketBy(16, "match_id") \
    .sortWithinPartitions("player_gamertag") \
    .saveAsTable("bootcamp.match_details_bucketed")

medals_matches_players_bucketed.write \
    .mode("overwrite") \
    .bucketBy(16, "match_id") \
    .sortWithinPartitions("player_gamertag") \
    .saveAsTable("medal_matches_players_bucketed")



# Bucket Join 1: matches_bucketed and match_details_bucketed
bucket_join_1 = matches_bucketed.alias("matches").join(
    match_details_bucketed.alias("details"),
    on="match_id",  # Join on the bucketed column
    how="inner"
).select(
    col("matches.match_id"),
    col("matches.mapid"),
    col("details.player_gamertag"),
    col("details.player_total_kills")
)

# Show the result of the first bucket join
print("Bucket Join 1: Matches and Match Details")
bucket_join_1.show()

# Bucket Join 2: matches_bucketed and medals_matches_players_bucketed
bucket_join_2 = matches_bucketed.alias("matches").join(
    medals_matches_players_bucketed.alias("medals"),
    on="match_id",  # Join on the bucketed column
    how="inner"
).select(
    col("matches.match_id"),
    col("matches.mapid"),
    col("medals.player_gamertag"),
    col("medals.medal_id"),
    col("medals.count")
)

# Show the result of the second bucket join
print("Bucket Join 2: Matches and Medals Matches Players")
bucket_join_2.show()

# Bucket Join 3: match_details_bucketed and medals_matches_players_bucketed
bucket_join_3 = match_details_bucketed.alias("details").join(
    medals_matches_players_bucketed.alias("medals"),
    on="match_id",  # Join on the bucketed column
    how="inner"
).select(
    col("details.match_id"),
    col("details.player_gamertag"),
    col("details.player_total_kills"),
    col("medals.medal_id"),
    col("medals.count")
)

# Show the result of the third bucket join
print("Bucket Join 3: Match Details and Medals Matches Players")
bucket_join_3.show()


# Explicit broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1000000000000")

# Perform the broadcast join
explicit_broadcast_df = matches_bucketed.alias("matches").join(
    broadcast(maps_bucketed).alias("map"),  # Explicitly broadcast maps_bucketed
    col("matches.mapid") == col("map.mapid")  # Join condition
).select(
    col("matches.*"),  # Select all columns from matches_bucketed
    col("map.name")    # Select the 'name' column from maps_bucketed
)

# Show the result
explicit_broadcast_df.show()

# Query 4a: Which player averages the most kills per game?
player_avg_kills = match_details_bucketed.groupBy("player_gamertag") \
    .agg(avg("player_total_kills").alias("avg_kills_per_game"))

# Find the player with the highest average kills
player_with_most_kills = player_avg_kills.orderBy("avg_kills_per_game", ascending=False).first()
print(f"Player with the most kills: {player_with_most_kills}")

# Query 4b: Which playlist gets played the most?
if "playlist_id" in matches_bucketed.columns:
    playlist_counts = matches_bucketed.groupBy("playlist_id") \
        .agg(count("*").alias("match_count"))
    playlist_counts.show()
else:
    print("playlist_id column not found in matches_bucketed.")

# Query 4c: Which map gets played the most?
map_counts = matches_bucketed.groupBy("mapid") \
    .agg(count("*").alias("match_count"))
map_counts.show()

# Query 4d: Which map do players get the most Killing Spree medals on?
# Filter for Killing Spree == 2430242797
killing_spree_medals_df = medals_matches_players_bucketed.filter(col("medal_id") == "2430242797")

# Group by mapid and count Killing Spree medals
map_killing_spree_counts = killing_spree_medals_df.groupBy("mapid") \
    .agg(count("*").alias("killing_spree_count"))

# Find the map with the highest Killing Spree medal count
most_killing_spree_map = map_killing_spree_counts.orderBy("killing_spree_count", ascending=False).first()
print(f"Map with the most Killing Spree medals: {most_killing_spree_map}")

# Function to calculate and print table size
def calculate_table_size(table_name):
    table_location = spark.sql(f"DESCRIBE FORMATTED {table_name}") \
                          .filter(col("col_name") == "Location") \
                          .select("data_type") \
                          .collect()[0][0]
    table_size = spark.sql(f"SELECT SUM(size) AS total_size FROM (SELECT input_file_name(), length(*) AS size FROM {table_name})") \
                      .collect()[0][0]
    print(f"Size of {table_name}: {table_size / (1024 * 1024):.2f} MB")

# Experiment with different partitioning strategies for each dataset

# 1. matches_bucketed
# Version 1: Partitioned by mapid
matches_bucketed.write \
    .mode("overwrite") \
    .partitionBy("mapid") \
    .saveAsTable("matches_bucketed_v1")
calculate_table_size("matches_bucketed_v1")

# Version 2: Partitioned by mapid and sorted within partitions by match_id
matches_bucketed.sortWithinPartitions("match_id").write \
    .mode("overwrite") \
    .partitionBy("mapid") \
    .saveAsTable("matches_bucketed_v2")
calculate_table_size("matches_bucketed_v2")

# Version 3: Partitioned by mapid and match_id
matches_bucketed.write \
    .mode("overwrite") \
    .partitionBy("mapid", "match_id") \
    .saveAsTable("matches_bucketed_v3")
calculate_table_size("matches_bucketed_v3")

# 2. maps_bucketed
# Version 1: Partitioned by name
maps_bucketed.write \
    .mode("overwrite") \
    .partitionBy("name") \
    .saveAsTable("maps_bucketed_v1")
calculate_table_size("maps_bucketed_v1")

# Version 2: Partitioned by name and sorted within partitions by mapid
maps_bucketed.sortWithinPartitions("mapid").write \
    .mode("overwrite") \
    .partitionBy("name") \
    .saveAsTable("maps_bucketed_v2")
calculate_table_size("maps_bucketed_v2")

# Version 3: Partitioned by name and mapid
maps_bucketed.write \
    .mode("overwrite") \
    .partitionBy("name", "mapid") \
    .saveAsTable("maps_bucketed_v3")
calculate_table_size("maps_bucketed_v3")

# 3. match_details_bucketed
# Version 1: Partitioned by player_gamertag
match_details_bucketed.write \
    .mode("overwrite") \
    .partitionBy("player_gamertag") \
    .saveAsTable("match_details_bucketed_v1")
calculate_table_size("match_details_bucketed_v1")

# Version 2: Partitioned by player_gamertag and sorted within partitions by match_id
match_details_bucketed.sortWithinPartitions("match_id").write \
    .mode("overwrite") \
    .partitionBy("player_gamertag") \
    .saveAsTable("match_details_bucketed_v2")
calculate_table_size("match_details_bucketed_v2")

# Version 3: Partitioned by player_gamertag and match_id
match_details_bucketed.write \
    .mode("overwrite") \
    .partitionBy("player_gamertag", "match_id") \
    .saveAsTable("match_details_bucketed_v3")
calculate_table_size("match_details_bucketed_v3")

# 4. medals_bucketed
# Version 1: Partitioned by name
medals_bucketed.write \
    .mode("overwrite") \
    .partitionBy("name") \
    .saveAsTable("medals_bucketed_v1")
calculate_table_size("medals_bucketed_v1")

# Version 2: Partitioned by name and sorted within partitions by medal_id
medals_bucketed.sortWithinPartitions("medal_id").write \
    .mode("overwrite") \
    .partitionBy("name") \
    .saveAsTable("medals_bucketed_v2")
calculate_table_size("medals_bucketed_v2")

# Version 3: Partitioned by name and medal_id
medals_bucketed.write \
    .mode("overwrite") \
    .partitionBy("name", "medal_id") \
    .saveAsTable("medals_bucketed_v3")
calculate_table_size("medals_bucketed_v3")

# 5. medals_matches_players_bucketed
# Version 1: Partitioned by player_gamertag
medals_matches_players_bucketed.write \
    .mode("overwrite") \
    .partitionBy("player_gamertag") \
    .saveAsTable("medal_matches_players_bucketed_v1")
calculate_table_size("medal_matches_players_bucketed_v1")

# Version 2: Partitioned by player_gamertag and sorted within partitions by match_id
medals_matches_players_bucketed.sortWithinPartitions("match_id").write \
    .mode("overwrite") \
    .partitionBy("player_gamertag") \
    .saveAsTable("medal_matches_players_bucketed_v2")
calculate_table_size("medal_matches_players_bucketed_v2")

# Version 3: Partitioned by player_gamertag and match_id
medals_matches_players_bucketed.write \
    .mode("overwrite") \
    .partitionBy("player_gamertag", "match_id") \
    .saveAsTable("medal_matches_players_bucketed_v3")
calculate_table_size("medal_matches_players_bucketed_v3")