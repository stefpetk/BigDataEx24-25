from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, udf

username = "stefanospetkovits"
spark = SparkSession \
    .builder \
    .appName("DF Query 1 Execution (DF with UDF)") \
    .getOrCreate()
sc = spark.sparkContext

# Set the log level to ERROR to minimize logging output
sc.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/DF_UDF_Q1_{job_id}"

# Load the crime data 
df_cr2k19 = spark.read.parquet
("hdfs://hdfs-namenode:9000/user/stefanospetkovits/data/parquet/LA_Crime_Data_2010_2019.parquet")
df_cr2k15 = spark.read.parquet
("hdfs://hdfs-namenode:9000/user/stefanospetkovits/data/parquet/LA_Crime_Data_2020_2025.parquet")

# Create a union between the crime data tables (as they have the same columns structure)
un_crdf = df_cr2k19.union(df_cr2k15) \
    .orderBy("Vict Age", ascending=False)

# Filter the dataframe rows so that only tuples with the "AGGRAVATED ASSAULT" classification
filt_df = un_crdf.filter(col("Crm Cd Desc").contains("AGGRAVATED ASSAULT"))

# Define a function to classify the age of the victims to age groups
def classify_age(vict_age):
    if vict_age < 18:
        return "Children < 18"
    elif 18 <= vict_age <= 24:
        return "Young Adults: 18-24"
    elif 25 <= vict_age <= 64:
        return "Adults: 25-64"
    else:
        return "Elderly > 65"

# Register the UDF
age_2_ageformat = udf(classify_age, StringType())

# Apply the UDF to the DataFrame
result_df = filt_df.withColumn("AgeGroup", age_2_ageformat(col("Vict Age")))

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