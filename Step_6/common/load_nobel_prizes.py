from pyspark.sql import functions as f
from pyspark.sql.window import Window

from common.start_stop_spark import spark_start, spark_stop


# create Nobel Prizes table in Data Warehouse
# data analysis shows that the logical key of the Nobel Prize is "Laureate ID" + "Prize"
def loadNobelPrizes():
    spark_start()
    
    df_nob_tmp = spark.sql("select * from akrasnova_tmp.nobel_laureates") # temp table Nobel Laureates with updated columns
    df_cat_dwh = spark.sql("select * from akrasnova_wrh.categories") # DWH table Categories
    df_pt_dwh = spark.sql("select * from akrasnova_wrh.prize_types").drop("upd_date") # DWH table Prize Types
    df_lt_dwh = spark.sql("select * from akrasnova_wrh.laureate_types") # DWH table Laureate Types
    
    # columns: Year, Category, Prize, Motivation, Prize Share, Laureate ID, Laureate Type
    df = df_nob_tmp.withColumnRenamed("Prize Share", "prize_share") \
                   .withColumnRenamed("Motivation", "motivation") \
                   .withColumnRenamed("Year", "year") \
                   .withColumnRenamed("Laureate ID", "laureate_id") \
                   .withColumnRenamed("Laureate Type", "laureate_type_tmp") \
                   .withColumnRenamed("Category", "category_tmp") \
                   .withColumnRenamed("Prize", "prize_tmp") \
                   .select("laureate_id", "year", "motivation", "prize_share", \
                           "category_tmp", "laureate_type_tmp", "prize_tmp", "upd_date") \
                   .distinct()
    
    # add Category ID
    join_expr = f.trim(df["category_tmp"]) == f.trim(df_cat_dwh["category"])
    df = df.join(df_cat_dwh, join_expr, "left_outer")
    
    # add Prize Type ID
    join_expr = f.trim(df["prize_tmp"]) == f.trim(df_pt_dwh["prize_type"])
    df = df.join(df_pt_dwh, join_expr, "left_outer")
    
    # add Laureate Type ID
    join_expr = f.trim(df["laureate_type_tmp"]) == f.trim(df_lt_dwh["laureate_type"])
    df = df.join(df_lt_dwh, join_expr, "left_outer")
    
    # add ID
    df = df.sort("year", "laureate_id") \
           .withColumn("nobel_prize_id", f.monotonically_increasing_id()) \
           .select("nobel_prize_id", "year", "category_id", "laureate_id", "laureate_type_id", \
                   "prize_type_id", "prize_share", "motivation", "upd_date")
    
    # save Nobel Prize table
    df.write.format("orc") \
      .partitionBy("category_id","laureate_type_id","upd_date")
      .mode("append") \
      .saveAsTable("akrasnova_wrh.nobel_prizes")
    
    spark_stop()
