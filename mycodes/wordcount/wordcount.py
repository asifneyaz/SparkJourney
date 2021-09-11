from pyspark.sql import SparkSession
from pyspark.sql import functions
spark = SparkSession.builder.appName("Word Count").master("local[*]").getOrCreate()
sc = spark.sparkContext
text = sc.textFile("PrideandPrejudice.txt")
flattened_text = text.flatMap(lambda line: line.split(" "))
flattened_text_counts = flattened_text.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x+y)
output = flattened_text_counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))






