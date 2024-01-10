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
from pyspark.sql.functions import year, month, count, row_number, when, col, regexp_replace, substring, udf, to_date, avg
from pyspark.sql.functions import round as round_dataframe
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType

spark = SparkSession.builder.master("local[*]").appName("databases_project").getOrCreate()
dataframe = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)

dataframe = dataframe.withColumn("Date Rptd", to_date("Date Rptd", "MM/dd/yyyy hh:mm:ss a"))
dataframe = dataframe.withColumn("DATE OCC", to_date("DATE OCC", "MM/dd/yyyy hh:mm:ss a"))
dataframe = dataframe.withColumn("Vict Age", dataframe["Vict Age"].cast("int"))
dataframe = dataframe.withColumn("LAT", dataframe["LAT"].cast("float"))
dataframe = dataframe.withColumn("LON", dataframe["LON"].cast("float"))

#reading income dataframe for 2015 year
income = spark.read.csv("income/LA_income_2015.csv", header=True, inferSchema=True)
#deleting "$" and "," from Estimated Median Income column and casting to int
income = income.withColumn("Estimated Median Income", regexp_replace("Estimated Median Income", "\\$", ""))
income = income.withColumn("Estimated Median Income", regexp_replace("Estimated Median Income", ",", "").cast("int"))

#sorting dataframe by descending order of income
income = income.sort(col("Estimated Median Income").desc())
#selecting top 3 and bottom 3 rows
highest_income = income.select("Zip Code").head(3)
lowest_income = income.select("Zip Code").tail(3)
#creating lists of zipcodes
highest_income_zipcodes = [row["Zip Code"] for row in highest_income]
lowest_income_zipcodes = [row["Zip Code"] for row in lowest_income]
#joining lists
zipcodes = highest_income_zipcodes + lowest_income_zipcodes

#reading dataframe for geocoding
geo = spark.read.csv("revgecoding.csv", header=True, inferSchema=True)
#casting ZIPcode column to int
geo = geo.withColumn("ZIPcode", substring(col("ZIPcode"), 0, 5).cast("int"))
#casting lat and lon columns to float
geo = geo.withColumn("LAT", geo["LAT"].cast("float"))
geo = geo.withColumn("LON", geo["LON"].cast("float"))

#filtering dataframe for 2015 year
dataframe_2015 = dataframe.filter(year("DATE OCC") == 2015)
#picking only crimes with victim age > 0
dataframe_2015 = dataframe_2015.filter(dataframe_2015["Vict Age"] > 0)
#joining dataframe with geocoding dataframe
dataframe_2015 = dataframe_2015.join(geo, on=["LAT", "LON"], how="left")
#mapping victim descent column to required format
dataframe_2015 = dataframe_2015.withColumn("Vict Descent", \
                            when(dataframe_2015["Vict Descent"] == "W", "White") \
                            .when(dataframe_2015["Vict Descent"] == "B", "Black") \
                            .when(dataframe_2015["Vict Descent"] == "H", "Hispanic/Latin/Mexican") \
                            .when(dataframe_2015["Vict Descent"] == "L", "Hispanic/Latin/Mexican") \
                            .when(dataframe_2015["Vict Descent"] == "M", "Hispanic/Latin/Mexican") \
                            .otherwise("Unknown")
)

#filtering dataframe for zipcodes
dataframe_2015_all_zipcodes = dataframe_2015.filter(dataframe_2015["ZIPCode"].isin(zipcodes))
dataframe_2015_highest_income = dataframe_2015.filter(dataframe_2015["ZIPCode"].isin(highest_income_zipcodes))
dataframe_2015_lowest_income = dataframe_2015.filter(dataframe_2015["ZIPCode"].isin(lowest_income_zipcodes))

#grouping by victim descent
dataframe_2015_all_zipcodes.groupBy("Vict Descent").agg(count("*").alias("#")).sort(col("#").desc()).show()
dataframe_2015_highest_income.groupBy("Vict Descent").agg(count("*").alias("#")).sort(col("#").desc()).show()
dataframe_2015_lowest_income.groupBy("Vict Descent").agg(count("*").alias("#")).sort(col("#").desc()).show()