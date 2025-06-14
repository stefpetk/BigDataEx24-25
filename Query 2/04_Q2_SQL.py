from pyspark.sql import SparkSession

username = "stefanospetkovits"
spark = SparkSession.builder.appName("SQL Query 2 execution").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/SqlQ2_{job_id}"

# Load parquet files
df1 = spark.read.parquet("hdfs://hdfs-namenode:9000/user/stefanospetkovits/data/parquet/LA_Crime_Data_2010_2019.parquet")
df2 = spark.read.parquet("hdfs://hdfs-namenode:9000/user/stefanospetkovits/data/parquet/LA_Crime_Data_2020_2025.parquet")

df = df1.unionByName(df2)
df.createOrReplaceTempView("crime")

query = """
SELECT Year, `AREA NAME`, Solved_Rate, Rank
FROM (
    SELECT
        CAST(YEAR(TO_TIMESTAMP(`Date Rptd`, 'MM/dd/yyyy hh:mm:ss a')) AS INT) AS Year,
        `AREA NAME`,
        SUM(CASE WHEN Status IN ('IC', 'CC') THEN 0 ELSE 1 END) * 1.0 / COUNT(*) AS Solved_Rate,
        RANK() OVER (
            PARTITION BY CAST(YEAR(TO_TIMESTAMP(`Date Rptd`, 'MM/dd/yyyy hh:mm:ss a')) AS INT)
            ORDER BY SUM(CASE WHEN Status IN ('IC', 'CC') THEN 0 ELSE 1 END) * 1.0 / COUNT(*) DESC
        ) AS Rank
    FROM crime
    GROUP BY Year, `AREA NAME`
)
ORDER BY Year, Rank
"""

result_df = spark.sql(query)
result_df.show(truncate=False)
result_df.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")