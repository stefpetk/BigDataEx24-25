from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year, count, sum as spark_sum, rank
from pyspark.sql.window import Window

username = "stefanospetkovits"
spark = SparkSession.builder.appName("DF Query 2 execution").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/DfQ2_{job_id}"

# Load parquet files
df1 = spark.read.parquet("hdfs://hdfs-namenode:9000/user/stefanospetkovits/data/parquet/LA_Crime_Data_2010_2019.parquet")
df2 = spark.read.parquet("hdfs://hdfs-namenode:9000/user/stefanospetkovits/data/parquet/LA_Crime_Data_2020_2025.parquet")

# Create a union the two DataFrames
df = df1.union(df2)

# Extract year and create solved/unsolved columns
df = df.withColumn("Year", year(col("Date Rptd"))) \
       .withColumn("Solved", when(col("Status").isin("IC", "CC"), 0).otherwise(1))

# Group by year and area, aggregate
result_df = df.groupBy("Year", "AREA NAME") \
    .agg(
        spark_sum("Solved").alias("Solved_Cases"),
        count("*").alias("Total_Cases")
    ) \
    .withColumn("Solved_Rate", col("Solved_Cases") / col("Total_Cases")) \
    .orderBy("Year", "AREA NAME")

# Add ranking column
window_spec = Window.partitionBy("Year").orderBy(col("Solved_Rate").desc())
result_df = result_df.withColumn("Rank", rank().over(window_spec))

# Select only the columns you want
result_df = result_df.select("Year", "AREA NAME", "Solved_Rate", "Rank") \
    .orderBy("Year", "Rank")

result_df.show(truncate=False)
result_df.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")
