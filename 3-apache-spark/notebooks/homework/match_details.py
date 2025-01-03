from pyspark.sql import SparkSession
# from pyspark.sql.functions import ex

spark = SparkSession.builder.appName("Homework").getOrCreate()

df = spark.read.option("header", "true") \
    .csv("/home/iceberg/data/match_details.csv") \

df.show(5)

df.createOrReplaceTempView("df")

data = spark.sql("SELECT * FROM df")
data.show(5)

df.createOrReplaceTempView("filtered_df")

data = spark.sql("""
                    WITH deduped AS (
                        SELECT
                            player_gamertag, match_id, team_id,
                            ROW_NUMBER() OVER (PARTITION BY player_gamertag, match_id, team_id ORDER BY match_id) AS row_num
                        FROM filtered_df
                        )
                        SELECT * FROM deduped
                        WHERE row_num = 1
                    """)
data.show(truncate=False)
# count rows
data.count()