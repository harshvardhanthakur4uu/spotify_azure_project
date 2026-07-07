# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

import os 
import sys

project_pth = os.path.join(os.getcwd(),'..','..')
sys.path.append(project_pth)

from utils.transformations import reusable

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimUser**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **AutoLoader**

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation", "abfss://silver@spotifyprojectadbharsh.dfs.core.windows.net/DimUser/checkpoint")\
            .load("abfss://bronze@spotifyprojectadbharsh.dfs.core.windows.net/DimUser")

# COMMAND ----------

df = spark.read.format("parquet")\
        .load("abfss://bronze@spotifyprojectadbharsh.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.withColumn("user_name", upper(col("user_name")))
display(df)

# COMMAND ----------

df_user = df_user.withColumn("user_name", upper(col("user_name")))

# COMMAND ----------

df_user_obj = reusable()

df_user = df_user_obj.dropColumns(df_user, ['_rescued_data'])
df_user = df_user.dropDuplicates(['user_id'])

# COMMAND ----------

df_user.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", "abfss://silver@spotifyprojectadbharsh.dfs.core.windows.net/DimUser/checkpoint")\
        .trigger(once=True)\
        .option("path", "abfss://silver@spotifyprojectadbharsh.dfs.core.windows.net/DimUser/data")\
        .toTable("spotify_catalog.silver.DimUser")

# COMMAND ----------

dff = spark.read.table("spotify_catalog.silver.DimUser")
display(dff)   

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimArtist**

# COMMAND ----------

df_art = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation", "abfss://silver@spotifyprojectadbharsh.dfs.core.windows.net/DimArtist/checkpoint")\
            .load("abfss://bronze@spotifyprojectadbharsh.dfs.core.windows.net/DimArtist")

# COMMAND ----------

df2 = spark.read.format("parquet")\
        .load("abfss://bronze@spotifyprojectadbharsh.dfs.core.windows.net/DimArtist")
display(df2)

# COMMAND ----------

df_art_obj = reusable()

df_art = df_art_obj.dropColumns(df_art,['_rescued_data'])
df_art = df_art.dropDuplicates(['artist_id']) 

# COMMAND ----------

df_art.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", "abfss://silver@spotifyprojectadbharsh.dfs.core.windows.net/DimArtist/checkpoint")\
        .trigger(once=True)\
        .option("path", "abfss://silver@spotifyprojectadbharsh.dfs.core.windows.net/DimArtist/data")\
        .toTable("spotify_catalog.silver.DimArtist")

# COMMAND ----------

dff2 = spark.read.table("spotify_catalog.silver.DimArtist")
display(dff2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimTrack**

# COMMAND ----------

df_tra = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation", "abfss://silver@spotifyprojectadbharsh.dfs.core.windows.net/DimTrack/checkpoint")\
            .load("abfss://bronze@spotifyprojectadbharsh.dfs.core.windows.net/DimTrack")

# COMMAND ----------

df3 = spark.read.format("parquet")\
        .load("abfss://bronze@spotifyprojectadbharsh.dfs.core.windows.net/DimTrack")
display(df3)

# COMMAND ----------

df_tra = df_tra.withColumn("durationFlag",when(col('duration_sec')<150,"low")\
                                            .when(col('duration_sec')<300,"medium")\
                                            .otherwise("high"))

df_tra = df_tra.withColumn("track_name",regexp_replace(col('track_name'),'-',' '))

df_tra = reusable().dropColumns(df_tra,['_rescued_data'])

# COMMAND ----------

df_tra.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", "abfss://silver@spotifyprojectadbharsh.dfs.core.windows.net/DimTrack/checkpoint")\
        .trigger(once=True)\
        .option("path", "abfss://silver@spotifyprojectadbharsh.dfs.core.windows.net/DimTrack/data")\
        .toTable("spotify_catalog.silver.DimTrack")

# COMMAND ----------

dff3 = spark.read.table("spotify_catalog.silver.DimTrack")
display(dff3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimDate**

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation", "abfss://silver@spotifyprojectadbharsh.dfs.core.windows.net/DimDate/checkpoint")\
            .load("abfss://bronze@spotifyprojectadbharsh.dfs.core.windows.net/DimDate")

# COMMAND ----------

df4 = spark.read.format("parquet")\
        .load("abfss://bronze@spotifyprojectadbharsh.dfs.core.windows.net/DimDate")
display(df4)

# COMMAND ----------

df_date = reusable().dropColumns(df_date,['_rescued_data'])

# COMMAND ----------

df_date.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", "abfss://silver@spotifyprojectadbharsh.dfs.core.windows.net/DimDate/checkpoint")\
        .trigger(once=True)\
        .option("path", "abfss://silver@spotifyprojectadbharsh.dfs.core.windows.net/DimDate/data")\
        .toTable("spotify_catalog.silver.DimDate")

# COMMAND ----------

dff4 = spark.read.table("spotify_catalog.silver.DimDate")
display(dff4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **FactStream**

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation", "abfss://silver@spotifyprojectadbharsh.dfs.core.windows.net/FactStream/checkpoint")\
            .load("abfss://bronze@spotifyprojectadbharsh.dfs.core.windows.net/FactStream")

# COMMAND ----------

df5 = spark.read.format("parquet")\
        .load("abfss://bronze@spotifyprojectadbharsh.dfs.core.windows.net/FactStream")
display(df5)

# COMMAND ----------

df_fact = reusable().dropColumns(df_fact,['_rescued_data'])

# COMMAND ----------

df_fact.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", "abfss://silver@spotifyprojectadbharsh.dfs.core.windows.net/FactStream/checkpoint")\
        .trigger(once=True)\
        .option("path", "abfss://silver@spotifyprojectadbharsh.dfs.core.windows.net/FactStream/data")\
        .toTable("spotify_catalog.silver.FactStream")

# COMMAND ----------

dff5 = spark.read.table("spotify_catalog.silver.FactStream")
display(dff5)

# COMMAND ----------

