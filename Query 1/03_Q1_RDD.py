import pyspark as spark
from pyspark.sql import SparkSession

username = "stefanospetkovits"
sc = SparkSession \
        .builder \
        .appName("RDD Query 1 execution") \
        .getOrCreate()  # define the necessary arguments for the spark session

# minimize logging output
sc.sparkContext.setLogLevel("ERROR")

# Get job ID and define the output directory
job_id = sc.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/RddQ1_{job_id}"

# ==========
# SCHEMA DETAILS:
# Los Angeles Crime Data Columns: "Crm Cd Desc", "Vict Age"
#
# Corresponding positions for crime data:
#   Crm Cd Desc: t[0]
#   Vict Age: t[1]

# Construct the RDD’s for crime data
cr_rdd_2k19 = sc.read.parquet(
    "hdfs://hdfs-namenode:9000/user/stefanospetkovits/data/parquet/LA_Crime_Data_2010_2019.parquet"
).rdd.map(lambda row: (
    row["Crm Cd Desc"],
    int(row["Vict Age"]))) # crime data for 2010-2019

cr_rdd_2k25 = sc.read.parquet(
    "hdfs://hdfs-namenode:9000/user/stefanospetkovits/data/parquet/LA_Crime_Data_2020_2025.parquet"
).rdd.map(lambda row: (
    row["Crm Cd Desc"],
    int(row["Vict Age"])))  # crime data for 2020-2025

# Define a function to categorize age groups in the crime data RDD's
def age_group(age):
    if age < 18:
        return "Children < 18"
    elif 18 <= age <= 24:
        return "Young Adults: 18-24"
    elif 25 <= age <= 64:
        return "Adults: 25-64"
    else:
        return "Elderly > 65"

# Filter the crime data RDD's to contain rows/tuples that contain the "AGGRAVATED ASSUALT" classification,
# map the age groups and create a union of the two RDD's
agg_with_agegrp = cr_rdd_2k19.filter(lambda t: "AGGRAVATED ASSAULT" in t[0]) \
    .union(cr_rdd_2k25.filter(lambda t: "AGGRAVATED ASSAULT" in t[0])) \
    .sortBy(lambda t: t[1], ascending=False) \
    .map(lambda t: (age_group(t[1]), 1)) \
    .reduceByKey(lambda a, b: a + b)

# Print results (for debugging) and save to HDFS
for rec in agg_with_agegrp.collect():
    print(rec)

agg_with_agegrp.coalesce(1).saveAsTextFile(output_dir)
