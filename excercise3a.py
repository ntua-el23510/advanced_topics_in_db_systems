""" Task 2
    Create a DataFrame that contains the main data-set. Keep the original column names
but change the column types as instructed below:
• Date Rptd: date
• DATE OCC: date
• Vict Age: integer
• LAT: double
• LON: double
Print the total number of rows for the entire data-set and the data type of every column.
(5%)
"""

import os
os.environ['JAVA_HOME']= "D:\Erasmus\openlogic-openjdk-jre-8u382-b05-windows-32"
os.environ['SPARK_HOME']= "D:\Erasmus\spark-3.5.0-bin-hadoop3\spark-3.5.0-bin-hadoop3"
os.environ['PYSPARK_PYTHON'] = "python"
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, count, row_number, to_date
from pyspark.sql.window import Window


spark = SparkSession.builder.master("local[*]").appName("databases_project").getOrCreate()
dataframe = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)

dataframe = dataframe.withColumn("Date Rptd", to_date("Date Rptd", "MM/dd/yyyy hh:mm:ss a"))
dataframe = dataframe.withColumn("DATE OCC", to_date("DATE OCC", "MM/dd/yyyy hh:mm:ss a"))
dataframe = dataframe.withColumn("Vict Age", dataframe["Vict Age"].cast("int"))
dataframe = dataframe.withColumn("LAT", dataframe["LAT"].cast("float"))
dataframe = dataframe.withColumn("LON", dataframe["LON"].cast("float"))

#group by year and month and count the number of crimes
dataframe_grouped = dataframe.groupBy(year("DATE OCC").alias("Year"), month("DATE OCC").alias("Month")) \
               .agg(count("*").alias("crime_total"))

#using Window to group the data by Year and sort by crime total
window_spec = Window.partitionBy("Year").orderBy(dataframe_grouped["crime_total"].desc())
dataframe_ranked = dataframe_grouped.withColumn("#", row_number().over(window_spec))

#get top-3 months with the highest number of crimes for each year
dataframe_top3 = dataframe_ranked.filter(dataframe_ranked["#"] <= 3)

#sort the results in ascending order with respect to the year and descending order with respect to the number of crimes
dataframe_sorted = dataframe_top3.orderBy("Year", dataframe_top3["crime_total"].desc())

#print the month, year, number of criminal acts recorded, and ranking of the month within the respective year
dataframe_sorted.select("Month", "Year", "crime_total", "#").show()