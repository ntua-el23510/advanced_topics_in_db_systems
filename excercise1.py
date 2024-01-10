
###
# Install and configure correctly the infrastructure for storage and processing using Apache
# Spark and Hadoop. Spark should be executed on top of the Apache Hadoop resource
# manager, YARN. Format the Apache Hadoop Distributed File System and use it for input/output of all the processing tasks you will develop. The execution environment
# needs to be set up in a fully distributed configuration, with at least two working nodes.
# Finally, web interfaces for HDFS, YARN and Spark History Server need to be available
# and accessible. (10%)
### 
import os
os.environ['JAVA_HOME']= "D:\Erasmus\openlogic-openjdk-jre-8u382-b05-windows-32"

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, count, row_number, when, col, regexp_replace, substring, udf, to_date, avg
from pyspark.sql.functions import round as round_df
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType

import geopy.distance

spark = SparkSession.builder.master("local[*]").appName("databases_project").getOrCreate()
df = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)