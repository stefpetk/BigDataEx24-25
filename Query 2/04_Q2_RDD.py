import pyspark as spark
from pyspark.sql import SparkSession

username = "stefanospetkovits"
sc = SparkSession \
        .builder \
        .appName("RDD Query 2 execution") \
        .getOrCreate()  # define the necessary arguments for the spark session

# minimize logging output
sc.sparkContext.setLogLevel("ERROR")

# Get job ID and define the output directory
job_id = sc.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/RddQ2_{job_id}"

# ==========
# SCHEMA DETAILS:
# Los Angeles Crime Data: "Date Rptd", "AREA NAME", "Status"
#
# Corresponding positions for crime data:
#   Date Rptd: t[0]
#   AREA NAME: t[1]
#   Status: t[2]

# Load the RDD’s for crime data and MO codes
cr_rdd_2k19 = sc.read.parquet(
    "hdfs://hdfs-namenode:9000/user/stefanospetkovits/data/parquet/LA_Crime_Data_2010_2019.parquet"
).rdd.map(lambda row: (
    str(row["Date Rptd"]),
    row["AREA NAME"],
    row["Status"])) # crime data for 2010-2019

cr_rdd_2k25 = sc.read.parquet(
    "hdfs://hdfs-namenode:9000/user/stefanospetkovits/data/parquet/LA_Crime_Data_2020_2025.parquet"
).rdd.map(lambda row: (
    str(row["Date Rptd"]),
    row["AREA NAME"],
    row["Status"])) # crime data for 2020-2025

# Unite the two RDD's as the columns are the same 
un_rdd = cr_rdd_2k19.union(cr_rdd_2k25)

def solved_unsolved(row):
    # row: (date, area, status)
    status = row[2]
    solved = 0 if status in ("IC", "CC") else 1
    return ((int(row[0].split(" ")[0].split("/")[2]), row[1]), (solved, 1))

# Map to ((year, area), (solved, total))
mapped_rdd = un_rdd.map(solved_unsolved)

# Reduce by key to sum solved and total cases
reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Compute solved case rate
result_rdd = reduced_rdd.map(lambda x: (x[0][0], x[0][1], x[1][0], x[1][1], x[1][0] / x[1][1]))

# Keep only (year, area, solved_rate)
area_rate_rdd = result_rdd.map(lambda x: (x[0], (x[1], x[4])))

# Group by year
grouped_rdd = area_rate_rdd.groupByKey()

# For each year, sort areas by solved_rate descending and assign rank
def rank_areas(year_areas):
    year, areas = year_areas
    # areas: iterable of (area, solved_rate)
    sorted_areas = sorted(areas, key=lambda x: x[1], reverse=True)
    return [(year, area, solved_rate, rank+1) for rank, (area, solved_rate) in enumerate(sorted_areas)]

final_rdd = grouped_rdd.flatMap(rank_areas).sortBy(lambda x: (x[0], x[3]))  # sort by year, rank

# Print results (for debugging) and save to HDFS
for rec in final_rdd.collect():
    print(rec)

final_rdd.map(lambda x: (x[0], x[1], x[2], x[3])).coalesce(1).saveAsTextFile(output_dir)