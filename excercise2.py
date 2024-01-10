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
from pyspark.sql.functions import to_date

#spark connection
spark = SparkSession.builder.master("local[*]").appName("databases_project").getOrCreate()
#reading csv file from the disk
dataframe = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)

#setting column types
dataframe = dataframe.withColumn("Date Rptd", to_date("Date Rptd", "MM/dd/yyyy hh:mm:ss a"))
dataframe = dataframe.withColumn("Vict Age", dataframe["Vict Age"].cast("int"))
dataframe = dataframe.withColumn("DATE OCC", to_date("DATE OCC", "MM/dd/yyyy hh:mm:ss a"))
dataframe = dataframe.withColumn("LON", dataframe["LON"].cast("float"))
dataframe = dataframe.withColumn("LAT", dataframe["LAT"].cast("float"))
print("Row count: ",dataframe.count())
dataframe.printSchema()