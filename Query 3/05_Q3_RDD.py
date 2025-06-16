import pyspark as spark
from pyspark.sql import SparkSession

username = "stefanospetkovits"
sc = SparkSession \
        .builder \
        .appName("RDD Query 3 execution") \
        .getOrCreate()  # define the necessary arguments for the spark session

# minimize logging output
sc.sparkContext.setLogLevel("ERROR")

# Get job ID and define the output directory
job_id = sc.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/RddQ3_{job_id}"

# ==========
# SCHEMA DETAILS:
# 1) 2010 Census Populations by Zip Code 
# Corresponding positions for columns of interest:
#   Zip Code: t[0]
#   Average Household Size: t[1]
#
# 2) Median Household Income by Zip Code
# Corresponding positions for columns of interest:
#   Zip Code: t[0]
#   Estimated Median Household Income: t[1]

# Create the RDD’s for household size and income data
hsize_rdd = sc.read.parquet(
    "hdfs://hdfs-namenode:9000/user/stefanospetkovits/data/parquet/2010_Census_Populations_by_Zip_Code.parquet"
).rdd.map(lambda row: (
    str(row["Zip Code"]),
    (row["Average Household Size"]))) # household size data

income_rdd = sc.read.parquet(
    "hdfs://hdfs-namenode:9000/user/stefanospetkovits/data/parquet/LA_income_2015.parquet"
).rdd.map(lambda row: (
    str(row["Zip Code"]),
    (float(row["Estimated Median Income"].replace("$", "").replace(",", ""))))) # income data

# Join the two RDDs on Zip Code
joined_rdd = hsize_rdd.join(income_rdd)

# Calculate the mean income per person by dividing the median household 
# income by the average household size
mean_income_per_person_rdd = joined_rdd.map(lambda x: (x[0], x[1][1] / x[1][0]))

# Print results (for debugging) and save to HDFS
for rec in mean_income_per_person_rdd.collect():
    print(rec)

mean_income_per_person_rdd.coalesce(1).saveAsTextFile(output_dir)
