from pyspark.sql import functions as f

from common.start_stop_spark import spark_start, spark_stop


# create simple reference tables in Data Warehouse from the Nobel Laureates dataset
def loadFromNobelDf(isUpdate=False):
    spark_start()
    
    df = spark.sql("select * from akrasnova_tmp.nobel_laureates") # temp table Nobel Laureates with updated columns
    
    # create Prize Types table
    df_tmp = df.select("Prize", "upd_date") \
               .distinct() \
               .withColumn("prize_type_id", f.monotonically_increasing_id()) \
               .withColumnRenamed("Prize", "prize_type") \
               .select("prize_type_id", "prize_type", "upd_date")
      
    # -- if update: start
    if isUpdate:
    
        df_prev = spark.sql("select * from akrasnova_wrh.prize_types") # previous records
        df_tmp = df_prev.union(df_tmp) # previous + current records
        
        # update dataset with up-to-date version of data: choose record with max value of upd_date for records with the same prize_type
        w = Window.partitionBy("prize_type").orderBy(f.col("upd_date").desc())
        df_tmp = df_tmp.withColumn("rank", f.rank().over(w)) \
                       .where("rank = 1") \
                       .drop("rank") \
                       .select("prize_type_id", "prize_type", "upd_date")
    # -- if update: end
    
    # save Prize Types table
    df_tmp.write.format("orc") \
          .partitionBy("upd_date")
          .mode("overwrite") \
          .saveAsTable("akrasnova_wrh.prize_types")
    
    # -- if create: start
    if not isUpdate:
    
        # create Laureate Types table
        df_tmp = df.select("Laureate Type") \
                   .distinct() \
                   .withColumn("laureate_type_id", f.monotonically_increasing_id()) \
                   .select("laureate_type_id", "Laureate Type") \
                   .withColumnRenamed("Laureate Type", "laureate_type")
                   
        # save Laureate Types table
        df_tmp.write.format("orc") \
              .mode("overwrite") \
              .saveAsTable("akrasnova_wrh.laureate_types")
              
        # create Categories table
        df_tmp = df.select("Category") \
                   .distinct() \
                   .withColumn("category_id", f.monotonically_increasing_id()) \
                   .select("category_id", "Category") \
                   .withColumnRenamed("Category", "category")
        
        # save Categories table
        df_tmp.write.format("orc") \
              .mode("overwrite") \
              .saveAsTable("akrasnova_wrh.categories")
        
        # create Genders table
        df_tmp = df.select("Sex") \
                   .where("Sex is not null") \
                   .distinct() \
                   .withColumn("gender_id", f.monotonically_increasing_id()) \
                   .select("gender_id", "Sex") \
                   .withColumnRenamed("Sex", "gender")
        
        # save Genders table
        df_tmp.write.format("orc") \
              .mode("overwrite") \
              .saveAsTable("akrasnova_wrh.genders")
    # -- if create: end
    
    spark_stop()

# create table Regions in Data Warehouse
def loadRegions():
    spark_start()
    
    df = spark.sql("select * from select * from akrasnova_tmp.countries") # temp table Countries
    
    # create Regions table
    df = df.select("Region") \
           .distinct() \
           .withColumn("region_id", f.monotonically_increasing_id()) \
           .select("region_id", "Region") \
           .withColumnRenamed("Region", "region_name")
    
    # save Regions table
    df.write.format("orc") \
      .mode("overwrite") \
      .saveAsTable("akrasnova_wrh.regions")
    
    spark_stop()

# create table Continents in Data Warehouse
def loadContinents():
    spark_start()
    
    df = spark.sql("select * from akrasnova_staging.country_continents")
    
    # create Continents table
    df = df.select("continent_code") \
           .distinct() \
           .withColumn("continent_id", f.monotonically_increasing_id()) \
           .withColumn("continent_name",  f.when(f.col("continent_code") == 'NA', 'NORTH AMERICA') \
                                           .when(f.col("continent_code") == 'SA', 'SOUTH AMERICA') \
                                           .when(f.col("continent_code") == 'AS', 'ASIA') \
                                           .when(f.col("continent_code") == 'AN', 'ANTARCTICA') \
                                           .when(f.col("continent_code") == 'OC', 'AUSTRALIA/OCEANIA') \
                                           .when(f.col("continent_code") == 'EU', 'EUROPE') \
                                           .when(f.col("continent_code") == 'AF', 'AFRICA') \
                                           .otherwise('')) \
           .select("continent_id", "continent_name", "continent_code")
    
    # save Continents table
    df.write.format("orc") \
      .mode("overwrite") \
      .saveAsTable("akrasnova_wrh.continents")
    
    spark_stop()

# create table Currencies in Data Warehouse
def loadCurrencyCodes():
    spark_start()
    
    df = spark.sql("select * from akrasnova_staging.country_currencies") # staging table Currency Codes
    
    # create Currencies table
    df = df.select("currency_code") \
           .where("currency_code != ''") \
           .distinct() \
           .withColumn("currency_code_id", f.monotonically_increasing_id()) \
           .select("currency_code_id", "currency_code")
    
    # save Currencies table
    df.write.format("orc") \
      .mode("overwrite") \
      .saveAsTable("akrasnova_wrh.currencies")
    
    spark_stop()