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
os.environ['HADOOP_HOME'] = "D:\Erasmus\hadoop"
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, count, row_number, to_date, when, col
from pyspark.sql.window import Window


spark = SparkSession.builder.master("local[*]").appName("databases_project").getOrCreate()
dataframe = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)

dataframe = dataframe.withColumn("Date Rptd", to_date("Date Rptd", "MM/dd/yyyy hh:mm:ss a"))
dataframe = dataframe.withColumn("DATE OCC", to_date("DATE OCC", "MM/dd/yyyy hh:mm:ss a"))
dataframe = dataframe.withColumn("Vict Age", dataframe["Vict Age"].cast("int"))
dataframe = dataframe.withColumn("LAT", dataframe["LAT"].cast("float"))
dataframe = dataframe.withColumn("LON", dataframe["LON"].cast("float"))

#dividing the "Time OCC" column into 4 parts of the day
dataframe_day_parts = dataframe.withColumn("Day_Part",
                             when((col("Time OCC") >= 500) & (col("Time OCC") < 1200), "Morning")
                             .when((col("Time OCC") >= 1200) & (col("Time OCC") < 1700), "Afternoon")
                             .when((col("Time OCC") >= 1700) & (col("Time OCC") < 2100), "Evening")
                             .otherwise("Night"))

#group by the day parts and count the number of crimes
dataframe_sorted = dataframe_day_parts.filter(col("Premis Desc") == "STREET")\
        .groupBy(col("Day_Part"))\
        .agg(count("*").alias("crime_total"))\
        .orderBy(col("crime_total").desc())
dataframe_sorted.show()