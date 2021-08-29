from pyspark.sql import SparkSession
import os
current_dir = os.path.dirname(__file__)
# determine data file path - modified
relative_path = "../../../../data/books.csv"
absolute_path = os.path.join(current_dir, relative_path)
#spark session
spark = SparkSession.builder.appName("ingestcsv").master("local").getOrCreate()
df = spark.read.format("csv")\
        .option("header", True)\
        .option("sep", ";")\
        .option("multiline", True)\
        .option("quote", "*")\
        .option("inferSchema", True)\
        .load(absolute_path)

df.show(10, 80)

