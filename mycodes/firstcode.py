from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("session").master("local[*]").getOrCreate()
myRange = spark.range(1000).toDF("number")
myRange.show(10)