## for test 
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from ..jobs.employee_scd_job import do_employee_scd_transformation

# Define test data as plain dictionaries (easier to read)
source_data = [
    {"employee_id": 101, "year": 2020, "department": "Engineering"},
    {"employee_id": 101, "year": 2021, "department": "Engineering"},
    {"employee_id": 101, "year": 2022, "department": "Data Science"},
    {"employee_id": 102, "year": 2021, "department": "HR"},
    {"employee_id": 102, "year": 2022, "department": "HR"},
    {"employee_id": 103, "year": 2022, "department": "Marketing"}
]

expected_data = [
    {"employee_id": 101, "department": "Engineering", "start_year": 2020, "end_year": 2021},
    {"employee_id": 101, "department": "Data Science", "start_year": 2022, "end_year": 2022},
    {"employee_id": 102, "department": "HR", "start_year": 2021, "end_year": 2022},
    {"employee_id": 103, "department": "Marketing", "start_year": 2022, "end_year": 2022}
]

def test_employee_scd_generation(spark):
    # Create source DataFrame
    source_df = spark.createDataFrame(source_data)

    # Run the transformation
    actual_df = do_employee_scd_transformation(spark, source_df)

    # Create expected DataFrame
    expected_df = spark.createDataFrame(expected_data)

    # Compare DataFrames
    assert_df_equality(actual_df, expected_df)


## for job

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lag, sum as _sum, when, min as _min, max as _max

def do_employee_scd_transformation(spark, dataframe):
    # Define a window for lag and cumulative sum operations
    window = Window.partitionBy("employee_id").orderBy("year")

    # Step 1: Identify when a department changes
    df_with_change = dataframe.withColumn(
        "did_change",
        (lag("department", 1).over(window) != col("department")) |
        lag("department", 1).over(window).isNull()
    )

    # Step 2: Assign a streak identifier for each department
    df_with_streak = df_with_change.withColumn(
        "streak_identifier",
        _sum(when(col("did_change"), 1).otherwise(0)).over(window)
    )

    # Step 3: Aggregate to get start and end years for each streak
    result_df = df_with_streak.groupBy("employee_id", "department", "streak_identifier").agg(
        _min("year").alias("start_year"),
        _max("year").alias("end_year")
    ).select("employee_id", "department", "start_year", "end_year")

    return result_df

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("employee_scd") \
        .getOrCreate()

    # Example: Load source data (replace with actual source)
    source_data = [
        {"employee_id": 101, "year": 2020, "department": "Engineering"},
        {"employee_id": 101, "year": 2021, "department": "Engineering"},
        {"employee_id": 101, "year": 2022, "department": "Data Science"},
        {"employee_id": 102, "year": 2021, "department": "HR"},
        {"employee_id": 102, "year": 2022, "department": "HR"},
        {"employee_id": 103, "year": 2022, "department": "Marketing"}
    ]
    source_df = spark.createDataFrame(source_data)

    # Run the transformation
    output_df = do_employee_scd_transformation(spark, source_df)

    # Save or display the result
    output_df.show()