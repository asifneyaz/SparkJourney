
#CSV ingestion in a dataframe with a Schema.

import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField,
                               IntegerType,DateType,
                               StringType)

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/books.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("Complex CSV with a schema to Dataframe") \
    .master("local[*]").getOrCreate()

# Creates the schema
schema = StructType([StructField('id', IntegerType(), False),
                     StructField('authorId', IntegerType(), False),
                     StructField('bookTitle', StringType(), False),
                     StructField('releaseDate', DateType(), False),
                     StructField('url', StringType(), False)])

# Reads a CSV file with header, called books.csv, stores it in a
# dataframe
df = spark.read.format("csv") \
    .option("header", True) \
    .option("multiline", True) \
    .option("sep", ";") \
    .option("dateFormat", "MM/dd/yyyy") \
    .option("quote", "*") \
    .schema(schema) \
    .load(absolute_file_path)


# Shows at most 20 rows from the dataframe

df.show(10, 80)
schema_as_json = df.schema.json()
print(schema_as_json)
parsedSchemaAsJson = json.loads(schema_as_json)
print(parsedSchemaAsJson)

print("Schema***{}".format((json.dumps(parsedSchemaAsJson, indent=4))))

spark.stop()
