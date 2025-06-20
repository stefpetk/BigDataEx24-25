from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, lower, count, avg, row_number
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
from pyspark.sql.window import Window
import math

def haversine(lat1, lng1, lat2, lng2):
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


username = "stefanospetkovits"
spark = SparkSession.builder.appName("DataFrame Query 4").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/DF_Q4_{job_id}"


haversine_dist = udf(haversine, DoubleType())

df1 = spark.read.csv("hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2010_2019.csv", header=True, inferSchema=True)
df2 = spark.read.csv("hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2020_2025.csv", header=True, inferSchema=True)
df1 = df1.withColumnRenamed("AREA ", "AREA")

crimes_df = df1.unionByName(df2)

# Load stations and Keep their division and coordinates
stations_df = spark.read.csv("hdfs://hdfs-namenode:9000/user/root/data/LA_Police_Stations.csv", header=True, inferSchema=True).select(
    col("DIVISION").alias("division"),
    col("Y").alias("station_lat"),
    col("X").alias("station_lon")
)


# Split value column into code and description and keep only the codes with descriptions containing the keywords "gun" and "weapon"
mo_gun_df = spark.read.text("hdfs://hdfs-namenode:9000/user/root/data/MO_codes.txt") \
    .withColumn("code", split(col("value"), " ", 2)[0]) \
    .withColumn("description", lower(split(col("value"), " ", 2)[1])) \
    .filter((col("description").contains("gun")) | (col("description").contains("weapon"))) \
    .drop("value","description")

mo_codes_list = [row.code for row in mo_gun_df.collect()]
pattern = r"\b(" + "|".join([code.strip() for code in mo_codes_list if code.strip() != ""]) + r")\b"

# Retain crimes with locations other than Null Island, find crimes with gun MO codes that contain at least 
# one MO gun code and keep their IDs and coordinates
filtered_crimes_df = crimes_df \
    .filter(~((col("LAT") == 0) & (col("LON") == 0))) \
    .filter(col("Mocodes").rlike(pattern)) \
    .select("DR_NO", "LAT", "LON")


# Perform cartesian product between all crimes and stations and then calculate the distance for all combinations
crime_with_stations = filtered_crimes_df.crossJoin(stations_df) \
    .withColumn("distance", haversine_dist(
        col("LAT"), col("LON"), col("station_lat"), col("station_lon")
    ))

crime_with_stations.explain(extended=True)

# Using windows, ordering and ranking, keep the closest station for each crime
window_spec = Window.partitionBy("DR_NO").orderBy(col("distance").asc())
closest_stations = crime_with_stations.withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1)


# For each station calculate the number of incidents and the average crime distance
result_df = closest_stations \
    .select("DR_NO", "division", "distance") \
    .groupBy("division") \
    .agg(
        avg("distance").alias("average_crime_distance"),
        count("*").alias("incident_count")
    ).orderBy(col("incident_count").desc())

result_df.show(truncate=False)
result_df.coalesce(1) \
        .write.format("csv") \
        .option("header", "true") \
		.save(output_dir)
