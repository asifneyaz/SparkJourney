from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("range").master("local[*]").getOrCreate()
myRange = spark.range(1, 1000)
myRange.show(10)