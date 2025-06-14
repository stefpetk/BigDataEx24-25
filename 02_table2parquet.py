import pyspark.sql import SparkSession
from pyspark.sql import StructField, StructType, IntegerType, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("CSV2Parquet").getOrCreate()

# Root directory for datasets in HDFS
root_dir = "hdfs://hdfs-namenode:9000/user/root/data"

# Set the output directory for parquet files
output_dir = "hdfs://hdfs-namenode:9000/user/stefanospetkovits/data/parquet"

# Dictionary of datasets and associated files
ds_dict = {"Los Angeles Crime Data (2010-2019)": "LA_Crime_Data_2010_2019.csv",
		   	"Los Angeles Crime Data (2020-)": "LA_Crime_Data_2020_2025.csv",
			"LA Police Stations": "LA_Police_Stations.csv",
			"Median Household Income By Zip Code": "LA_income_2015.csv",
			"2010 Census Populations by Zip Code": "2010_Census_Populations_by_Zip_Code.csv",
			"MO Codes in Numerical Order": "MO_codes.txt"}

# create a schema for the MO codes dataframe to be constructed
mo_schema = StructType([
	StructField("mo_code", IntegerType()),
	StructField("description", StringType())
])

for file_name in ds_dict.values():
    file_path = f"{root_dir}/{file_name}"
    out_path = f"{output_dir}/{file_name.split('.')[0]}.parquet"
    if file_name.endswith(".csv"):
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    else:  # txt file
        df = spark.read.option("delimiter", " ").schema(mo_schema).csv(file_path)
    df.write.mode("overwrite").parquet(out_path)