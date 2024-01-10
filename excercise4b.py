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
from pyspark.sql.functions import year, month, count, row_number, to_date, when, col
from pyspark.sql.window import Window


spark = SparkSession.builder.master("local[*]").appName("databases_project").getOrCreate()
dataframe = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)

dataframe = dataframe.withColumn("Date Rptd", to_date("Date Rptd", "MM/dd/yyyy hh:mm:ss a"))
dataframe = dataframe.withColumn("DATE OCC", to_date("DATE OCC", "MM/dd/yyyy hh:mm:ss a"))
dataframe = dataframe.withColumn("Vict Age", dataframe["Vict Age"].cast("int"))
dataframe = dataframe.withColumn("LAT", dataframe["LAT"].cast("float"))
dataframe = dataframe.withColumn("LON", dataframe["LON"].cast("float"))

#using rdd to filter crimes commited on the street
rdd_filtered = dataframe.rdd.filter(lambda row: row["Premis Desc"] == "STREET")

#using rdd to map each crime record to a day part
rdd_mapped = rdd_filtered.map(
    lambda row: (
        "Morning" if 500 <= row["TIME OCC"] < 1200 else
        "Afternoon" if 1200 <= row["TIME OCC"] < 1700 else
        "Evening" if 1700 <= row["TIME OCC"] < 2100 else
        "Night"
    )
)

#using rdd to map day parts to keys
rdd_mapped = rdd_mapped.map(lambda x: (x, 1))
#using rdd to calculate conts for each day part
rdd_reduced = rdd_mapped.reduceByKey(lambda x, y: x + y)

#printing the result
for record in rdd_reduced.collect():
    print(record)