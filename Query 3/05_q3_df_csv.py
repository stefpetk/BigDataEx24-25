from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

username = "stefanospetkovits"

spark = SparkSession.builder.appName("DataFrame Query 3 (with DF and csv)").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/DF_csv_Q3_{job_id}"


hsize_df = spark.read.option("header", "true").csv(
    "hdfs://hdfs-namenode:9000/user/root/data/2010_Census_Populations_by_Zip_Code.csv"
).select(
    col("Zip Code").cast("string").alias("zip"),
    col("Average Household Size").cast("double").alias("household_size")
)

income_df = spark.read.option("header", "true").csv(
    "hdfs://hdfs-namenode:9000/user/root/data/LA_income_2015.csv"
).select(
    col("Zip Code").cast("string").alias("zip"),
    regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("double").alias("median_income")
)

# 1-1
joined_df = hsize_df.join(income_df, on="zip", how="inner")
joined_df.explain(extended=True)


result_df = joined_df \
    .filter(col("household_size").isNotNull() & (col("household_size") != 0)) \
    .withColumn("income_per_person", col("median_income") / col("household_size")) \
    .select("zip", "income_per_person")

result_df.show(truncate=False)

result_df.coalesce(1) \
        .write.format("csv") \
        .option("header", "true") \
		.save(output_dir)
