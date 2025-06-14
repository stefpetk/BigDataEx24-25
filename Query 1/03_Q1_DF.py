from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

username = "stefanospetkovits"
spark = SparkSession \
    .builder \
    .appName("DF Query 1 Execution (DF without UDF)") \
    .getOrCreate()
sc = spark.sparkContext

# Set the log level to ERROR to minimize logging output
sc.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/DfQ1_{job_id}"

# Load the crime data 
df_cr2k19 = spark.read \
                 .parquet("hdfs://hdfs-namenode:9000/user/stefanospetkovits/data/parquet/LA_Crime_Data_2010_2019.parquet")
df_cr2k25 = spark.read \
                 .parquet("hdfs://hdfs-namenode:9000/user/stefanospetkovits/data/parquet/LA_Crime_Data_2020_2025.parquet")

# Create a union between the crime data tables (as they have the same columns structure)
un_crdf = df_cr2k19.union(df_cr2k25) \
    .orderBy("Vict Age", ascending=False)

# Filter the dataframe rows so that only rows with the "AGGRAVATED ASSAULT" classification are retained
filt_df = un_crdf.filter(col("Crm Cd Desc").contains("AGGRAVATED ASSAULT"))

# Assign individual ages to an age group
result_df = filt_df.withColumn(
    "AgeGroup",
    when(col("Vict Age") < 18, "Children < 18")
    .when((col("Vict Age") >= 18) & (col("Vict Age") <= 24), "Young Adults: 18-24")
    .when((col("Vict Age") >= 25) & (col("Vict Age") <= 64), "Adults: 25-64")
    .otherwise("Elderly > 65")
)

# Select only the relevant columns and aggregate the records by age group
result_df = result_df.select("Crm Cd Desc", "AgeGroup") \
                     .groupBy("Crm Cd Desc", "AgeGroup") \
                     .count() \
                     .withColumnRenamed("count", "Count") \
                     .withColumnRenamed("Crm Cd Desc", "Crime Description") \
                     .withColumnRenamed("AgeGroup", "Victim Age Group") \
					 .orderby("count", ascending=False)

# Show results for debugging and save to the HDFS
result_df.show(truncate=False)
result_df.coalesce(1) \
        .write.format("csv") \
        .option("header", "true") \
		.save(output_dir)
