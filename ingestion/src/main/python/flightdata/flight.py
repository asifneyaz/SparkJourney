from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("flight").master("local[*]").getOrCreate()
flightdata = spark.read.option("inferSchema", True).option("header", True).csv("../../../../../data/flight-data/csv/2015-summary.csv")
flightdata.show(10)
flightdata.createOrReplaceTempView("flight_data_view")
sqlway = spark.sql("""SELECT DEST_COUNTRY_NAME, COUNT(1) FROM flight_data_view GROUP BY DEST_COUNTRY_NAME """)
sqlway.show(10)

