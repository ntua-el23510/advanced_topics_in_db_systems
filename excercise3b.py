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
from pyspark.sql.functions import year, month, count, row_number, to_date
from pyspark.sql.window import Window


spark = SparkSession.builder.master("local[*]").appName("databases_project").getOrCreate()
dataframe = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)

dataframe = dataframe.withColumn("Date Rptd", to_date("Date Rptd", "MM/dd/yyyy hh:mm:ss a"))
dataframe = dataframe.withColumn("DATE OCC", to_date("DATE OCC", "MM/dd/yyyy hh:mm:ss a"))
dataframe = dataframe.withColumn("Vict Age", dataframe["Vict Age"].cast("int"))
dataframe = dataframe.withColumn("LAT", dataframe["LAT"].cast("float"))
dataframe = dataframe.withColumn("LON", dataframe["LON"].cast("float"))


dataframe.createOrReplaceTempView("dataframe")
spark.sql("""
SELECT Year, Month, crime_total, Rank
FROM (
    SELECT Year, Month, crime_total, ROW_NUMBER() OVER (PARTITION BY Year ORDER BY crime_total DESC) AS Rank
    FROM (
        SELECT YEAR(`DATE OCC`) AS Year, MONTH(`DATE OCC`) AS Month, COUNT(*) AS crime_total
        FROM dataframe
        GROUP BY Year, Month
    ) t
) t
WHERE Rank <= 3
ORDER BY Year ASC, crime_total DESC
""").show()