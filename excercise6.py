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
from pyspark.sql.functions import round as round_df
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType
import geopy.distance

spark = SparkSession.builder.master("local[*]").appName("databases_project").getOrCreate()
dataframe = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)

dataframe = dataframe.withColumn("Date Rptd", to_date("Date Rptd", "MM/dd/yyyy hh:mm:ss a"))
dataframe = dataframe.withColumn("DATE OCC", to_date("DATE OCC", "MM/dd/yyyy hh:mm:ss a"))
dataframe = dataframe.withColumn("Vict Age", dataframe["Vict Age"].cast("int"))
dataframe = dataframe.withColumn("LAT", dataframe["LAT"].cast("float"))
dataframe = dataframe.withColumn("LON", dataframe["LON"].cast("float"))

# # Reading additional dataframe containing police stations
# police_df = spark.read.csv("LAPD_Police_Stations.csv", header=True, inferSchema=True)
# police_df = police_df.withColumn("Police_LON", police_df["X"].cast("float").alias("Police_LON"))
# police_df = police_df.withColumn("Police_LAT", police_df["Y"].cast("float").alias("Police_LAT"))
# police_df = police_df.drop("X", "Y")

# def get_distance (lat1 , lon1 , lat2 , lon2 ) :
#     return round(geopy.distance.distance((lat1, lon1), (lat2, lon2)).km, 3)

# # Defining a UDF to calculate distance between two points
# get_distance_udf = udf(get_distance, FloatType())

# # Filtering values with 0 latitude and longitude 
# df_null_island = dataframe.where((col("LAT") != 0) & (col("LON") != 0))
# # Filtering values with weapon code between 100 and 200 i.e. guns
# df_guns = df_null_island.where((col("Weapon Used Cd") >= 100) & (col("Weapon Used Cd") < 200))
# # Joining guns dataframe with police dataframe
# df_guns_police = df_guns.join(police_df, [df_guns["AREA "] == police_df["PREC"]], how="left")
# # Selecting useful columns
# df_crimes = df_guns_police.select("LAT", "LON", "Police_LAT", "Police_LON", "PREC", "DATE OCC", "DIVISION")

# # Calculating distance between crime location and police station that responded to the crime
# df_crimes = df_crimes.withColumn("Distance", get_distance_udf(df_crimes["LAT"], df_crimes["LON"], df_crimes["Police_LAT"], df_crimes["Police_LON"]))

# # Grouping by year and calculating average distance
# df_stats1 = df_crimes.groupBy(year("DATE OCC").alias("Year")).agg(count("*").alias("Number of Crimes"), avg("Distance").alias("Average Distance (km)")).orderBy("Year")
# # Rounding the average distance to 3 decimal places
# df_stats1 = df_stats1.withColumn("Average Distance (km)", round_df(df_stats1["Average Distance (km)"], 3))

# # Grouping by division and calculating average distance
# df_stats2 = df_crimes.groupBy("DIVISION").agg(count("*").alias("Number of Crimes"), avg("Distance").alias("Average Distance (km)")).orderBy(col("Number of Crimes").desc())
# # Rounding the average distance to 3 decimal places
# df_stats2 = df_stats2.withColumn("Average Distance (km)", round_df(df_stats2["Average Distance (km)"], 3))

# #6a
# df_stats1.show()
# df_stats2.show()

# # Repeating the same steps as above but instead the police station with the closest distance is selected
# police_cross_join = police_df.crossJoin(df_guns)
# # Calculating distance between crime location and closest police station
# police_cross_join = police_cross_join.withColumn("Distance", get_distance_udf(police_cross_join["LAT"], police_cross_join["LON"], police_cross_join["Police_LAT"], police_cross_join["Police_LON"]))
# window_spec = Window.partitionBy("DR_NO").orderBy(col("Distance").asc())
# closest_police_station_df = police_cross_join.withColumn("row_num", row_number().over(window_spec)).filter("row_num = 1").drop("row_num")

# # Calculating the same stats as above
# df_stats11 = closest_police_station_df.groupBy(year("DATE OCC").alias("Year")).agg(count("*").alias("Number of Crimes"), avg("Distance").alias("Average Distance (km)")).orderBy("Year")

# df_stats11 = df_stats11.withColumn("Average Distance (km)", round_df(df_stats11["Average Distance (km)"], 3))

# df_stats22 = closest_police_station_df.groupBy("DIVISION").agg(count("*").alias("Number of Crimes"), avg("Distance").alias("Average Distance (km)")).orderBy(col("Number of Crimes").desc())

# df_stats22 = df_stats22.withColumn("Average Distance (km)", round_df(df_stats22["Average Distance (km)"], 3))

# #6b
# df_stats11.show()
# df_stats22.show()

#reading dataframe for police stations
police = spark.read.csv("LAPD_Police_Stations.csv", header=True, inferSchema=True)
#casting X and Y columns to float
police = police.withColumn("X", police["X"].cast("float"))
police = police.withColumn("Y", police["Y"].cast("float"))
#changing names of columns
police = police.withColumnRenamed("X", "Police_X")
police = police.withColumnRenamed("Y", "Police_Y")

#defining distance function
def get_distance (lat1 , lon1 , lat2 , lon2 ) :
    return round(geopy.distance.distance((lat1, lon1), (lat2, lon2)).km, 3)

#changing function to udf
get_distance_udf = udf(get_distance, FloatType())

#deleting rows with 0 latitude and longitude
df_victims = dataframe.where((col("LAT") != 0) & (col("LON") != 0))
#picking only crimes with weapon code between 100 and 200
df_only_guns = df_victims.where((col("Weapon Used Cd") >= 100) & (col("Weapon Used Cd") < 200))
#joining dataframes
df_police = df_only_guns.join(police, [dataframe["AREA "] == police["PREC"]], how="left")
#selecting useful columns
df_police = df_police.select("LAT", "LON", "Police_X", "Police_Y", "PREC", "DATE OCC", "DIVISION")
#calculating distance between crime location and police station that was responsible for the crime
df_police = df_police.withColumn("Distance", get_distance_udf(df_police["LAT"], df_police["LON"], df_police["Police_Y"], df_police["Police_X"]))
#grouping by year and calculating average distance
df_police_stats = df_police.groupBy(year("DATE OCC").alias("Year")).agg(count("*").alias("Number of Crimes"), avg("Distance").alias("Average Distance (km)")).orderBy("Year")
#rounding the average distance to 3 decimal places
df_police_stats = df_police_stats.withColumn("Average Distance (km)", round_df(df_police_stats["Average Distance (km)"], 3))
#grouping by division and calculating average distance
df_police_stats2 = df_police.groupBy("DIVISION").agg(count("*").alias("Number of Crimes"), avg("Distance").alias("Average Distance (km)")).orderBy(col("Number of Crimes").desc())
#rounding the average distance to 3 decimal places
df_police_stats2 = df_police_stats2.withColumn("Average Distance (km)", round_df(df_police_stats2["Average Distance (km)"], 3))

#query 4 a
df_police_stats.show()
df_police_stats2.show()


#doing the same for query 4 b
police_cross_join = police.crossJoin(df_only_guns)
#calculating distance between crime location and closest police station
police_cross_join = police_cross_join.withColumn("Distance", get_distance_udf(police_cross_join["LAT"], police_cross_join["LON"], police_cross_join["Police_Y"], police_cross_join["Police_X"]))
#selecting closest police station using window
window_spec = Window.partitionBy("DR_NO").orderBy(col("Distance").asc())
closest_police_station_df = police_cross_join.withColumn("row_num", row_number().over(window_spec)).filter("row_num = 1").drop("row_num")
df_police_stats = closest_police_station_df.groupBy(year("DATE OCC").alias("Year")).agg(count("*").alias("Number of Crimes"), avg("Distance").alias("Average Distance (km)")).orderBy("Year")
df_police_stats = df_police_stats.withColumn("Average Distance (km)", round_df(df_police_stats["Average Distance (km)"], 3))
df_police_stats2 = closest_police_station_df.groupBy("DIVISION").agg(count("*").alias("Number of Crimes"), avg("Distance").alias("Average Distance (km)")).orderBy(col("Number of Crimes").desc())
df_police_stats2 = df_police_stats2.withColumn("Average Distance (km)", round_df(df_police_stats2["Average Distance (km)"], 3))

#query 4 b
df_police_stats.show()
df_police_stats2.show()