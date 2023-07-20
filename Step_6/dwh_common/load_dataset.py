from pyspark.sql import functions as f
from pyspark.sql import Column as c
from pyspark.sql.window import Window

from dwh_common.start_stop_spark import spark_start, spark_stop


# create Dataset table
# during an initial transformation with data analysis done before, use created reference DWH tables for Snowflake model
def loadDataset(isUpdate=False):
    spark_start()
    spark.conf.set("spark.sql.adaptive.enabled",True)
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled",True)
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",True)
    
    df_nob_tmp = spark.sql("select * from akrasnova_tmp.nobel_laureates") # temp table Nobel Laureates with updated columns
    df_laur_dwh = spark.sql("select * from akrasnova_wrh.laureate_persons").drop("upd_date") # DWH table Laureates Persons
    df_org_dwh = spark.sql("select * from akrasnova_wrh.organizations").drop("upd_date") # DWH table Organizations
    df_cont_dwh = spark.sql("select * from akrasnova_wrh.continents") # DWH table Continents
    df_reg_dwh = spark.sql("select * from akrasnova_wrh.regions") # DWH table Regions
    
    # if initial load, use DWH table Countries, else DWH up-to-date table Countries
    table_name = 'countries_uptodate' if isUpdate else 'countries'
    df_cnt_dwh = spark.sql("select * from akrasnova_wrh."+table_name).drop("upd_date") # DWH table Countries / up-to-date Countries
    
    # if initial load, use DWH table Countries, else DWH up-to-date table Cities
    table_name = 'cities_uptodate' if isUpdate else 'cities'
    df_city_dwh = spark.sql("select * from akrasnova_wrh."+table_name).drop("upd_date") # DWH table Countries / up-to-date Cities
    
    # use Nobel Prize dataframe as main
    # id is the internal column for grouping, remove it later
    df = df_nob_tmp.sort("Year", "Laureate ID") \
                   .withColumn("id", f.monotonically_increasing_id()) \
                   .withColumnRenamed("Year", "year") \
                   .withColumnRenamed("Category", "category") \
                   .withColumnRenamed("Prize", "prize") \
                   .withColumnRenamed("Motivation", "motivation") \
                   .withColumnRenamed("Prize Share", "prize_share") \
                   .withColumnRenamed("Laureate ID", "laureate_id") \
                   .withColumnRenamed("Laureate Type", "laureate_type") \
                   .withColumnRenamed("Full Name", "full_name") \
                   .withColumnRenamed("Birth Date", "birth_date") \
                   .withColumnRenamed("Birth City", "birth_city_name") \
                   .withColumnRenamed("Birth Country", "birth_country_name") \
                   .withColumnRenamed("Sex", "gender") \
                   .withColumnRenamed("Organization Name", "organization_name") \
                   .withColumnRenamed("Organization City", "organization_city_name") \
                   .withColumnRenamed("Organization Country", "organization_country_name") \
                   .withColumnRenamed("Death Date", "death_date") \
                   .withColumnRenamed("Death City", "death_city_name") \
                   .withColumnRenamed("Death Country", "death_country_name") \
                   .withColumn("laureate_person_name", \
                       f.when(f.col("laureate_type") == 'Individual', f.col("full_name")) \
                        .otherwise(f.lit(None))) \
                   .withColumn("society_name", \
                       f.when(f.col("laureate_type") == 'Organization', f.col("full_name")) \
                        .otherwise(f.lit(None))) \
                   .select("id", "laureate_id", "year", "category", "prize", "prize_share", "laureate_type", "motivation", \
                           "laureate_person_name", "gender", "society_name", 
                           "birth_date", "birth_city_name", "birth_country_name", \
                           "death_date", "death_city_name", "death_country_name", \
                           "organization_name", "organization_city_name", "organization_country_name", "upd_date")
    
    # add Birth/Death Country Id and City Id
    df_tmp = df_laur_dwh.select("laureate_id", "birth_city_id", "birth_country_id", "death_city_id", "death_country_id")
                        .withColumnRenamed("laureate_id", "laur_id_tmp") \
    join_expr = f.trim(df["laureate_id"]) == f.trim(df_tmp["laur_id_tmp"])
    df = df.join(df_tmp, join_expr, "left_outer") \
           .drop("laur_id_tmp")
    
    # add Organization Country Id and City Id
    df_tmp = df_org_dwh.select("organization_id", "organization_name", "organization_country_id", "organization_city_id")
                       .withColumnRenamed("organization_name", "org_name_tmp") \
    join_expr = f.trim(df["organization_name"]) == f.trim(df_tmp["org_name_tmp"])
    df = df.join(df_tmp, join_expr, "left_outer") \
           .drop("org_name_tmp")
    
    # add Parent Country Name: parent country name added for historical names of country (or if specified a name of country part)
    # if country has a modern name, parent name = country name
    
    # create temp dataframe with Country Id/Name and Parent Country Id/Name
    df_cnt_ch = df_cnt_dwh.select("country_id", "country_name", "parent_country_id") \
                          .withColumnRenamed("country_id", "cnt_id_tmp") \
                          .withColumnRenamed("country_name", "cnt_name_tmp") \
                          .withColumnRenamed("parent_country_id", "par_cnt_id_tmp") 
    df_cnt_par = df_cnt_dwh.select("country_id", "country_name") \
                           .withColumnRenamed("country_id", "par_cnt_id_tmp2") \
                           .withColumnRenamed("country_name", "par_cnt_name_tmp")
    join_expr = f.trim(df_cnt_ch["par_cnt_id_tmp"]) == f.trim(df_cnt_par["par_cnt_id_tmp2"])
    df_cnt_ch = df_cnt_ch.join(df_cnt_par, join_expr, "left_outer") \
                         .select("cnt_id_tmp", "cnt_name_tmp", "par_cnt_id_tmp", "par_cnt_name_tmp")
    
    # join Birth Country Parent Name
    join_expr = f.trim(df["birth_country_id"]) == f.trim(df_cnt_ch["cnt_id_tmp"])
    df = df.join(df_mis_cntr, join_expr, "left_outer") \
           .withColumnRenamed("cnt_id_tmp", "birth_country_parent_id") 
           .withColumn("birth_country_parent_name", \
                    f.when(c.isNull(f.col("par_cnt_name_tmp")), f.col("birth_country_name")) \
                     .otherwise(f.col("par_cnt_name_tmp"))) \
           .drop("cnt_name_tmp", "par_cnt_id_tmp", "par_cnt_name_tmp")
    
    # join Death Country Parent Name
    join_expr = f.trim(df["death_country_id"]) == f.trim(df_cnt_ch["cnt_id_tmp"])
    df = df.join(df_mis_cntr, join_expr, "left_outer") \
           .withColumnRenamed("cnt_id_tmp", "death_country_parent_id") 
           .withColumn("death_country_parent_name", \
                    f.when(c.isNull(f.col("par_cnt_name_tmp")), f.col("death_country_name")) \
                     .otherwise(f.col("par_cnt_name_tmp"))) \
           .drop("cnt_name_tmp", "par_cnt_id_tmp", "par_cnt_name_tmp")
    
    # join Organization Country Parent Name
    join_expr = f.trim(df["organization_country_id"]) == f.trim(df_cnt_ch["cnt_id_tmp"])
    df = df.join(df_mis_cntr, join_expr, "left_outer") \
           .withColumnRenamed("cnt_id_tmp", "organization_country_parent_id") 
           .withColumn("organization_country_parent_name", \
                    f.when(c.isNull(f.col("par_cnt_name_tmp")), f.col("organization_country_name")) \
                     .otherwise(f.col("par_cnt_name_tmp"))) \
           .drop("cnt_name_tmp", "par_cnt_id_tmp", "par_cnt_name_tmp")
           
    # add Parent Organization name: parent organization name added for child organizations
    # if there is no Parent Organization, parent organization = Organization name
    # create temp dataframe with Org Id/Name and Parent Org Id/Name
    df_org_ch = df_org_dwh.select("organization_id", "organization_name", "parent_organization_id") \
                          .withColumnRenamed("organization_id", "org_id_tmp") \
                          .withColumnRenamed("organization_name", "org_name_tmp") \
                          .withColumnRenamed("parent_organization_id", "par_org_id_tmp")
    df_org_par = df_org_dwh.select("organization_id", "organization_name") \
                           .withColumnRenamed("organization_id", "par_org_id_tmp2") \
                           .withColumnRenamed("organization_name", "par_org_name")
    join_expr = f.trim(df_org_ch["par_org_id_tmp"]) == f.trim(df_org_par["par_org_id_tmp2"])
    df_org_ch = df_org_ch.join(df_org_par, join_expr, "left_outer") \
                         .select("org_id_tmp", "org_name_tmp", "par_org_id_tmp", "par_org_name")
    
    # join Organization Parent Name
    join_expr = f.trim(df["organization_id"]) == f.trim(df_org_ch["org_id_tmp"])
    df = df.join(df_org_ch, join_expr, "left_outer") \
           .withColumn("organization_parent_name", \
                    f.when(c.isNull(f.col("par_org_name")), f.col("organization_name")) \
                     .otherwise(f.col("par_org_name"))) \
           .drop("org_id_tmp", "org_name_tmp", "par_org_id_tmp", "par_org_name")
    
    # add Country information
    # join Country information by Parent Country Name columns that contain modern country names
    # if the country has modern name, Parent Country Name = Country Name. if not, join the information of modern name to history country
    
    # join Continent Name and Region Name to Country dataframe
    join_expr = f.trim(df_cnt_dwh["continent_id"]) == f.trim(df_cont_dwh["continent_id"])
    join_expr1 = f.trim(df_cnt_dwh["region_id"]) == f.trim(df_reg_dwh["region_id"])
    df_cnt_tmp = df_cnt_dwh.join(df_cont_dwh, join_expr, "left_outer") \
                           .join(df_reg_dwh, join_expr1, "left_outer") \
                           .select("country_id", "country_name", "region_name", "continent_name", \
                                   "population", "area_sq_miles", "pop_dencity_per_sq_mile", "coastline", \
                                   "net_migration", "infant_mortality_per_1000", "gdb_dollar_per_capita", "percent_literacy", \
                                   "phones_per_1000", "percent_arable", "percent_crops", "percent_other", "climate", \
                                   "birthrate", "deathrate", "agriculture", "industry", "service")
    
    # Birth Country: add all information about birth place
    join_expr = f.trim(df["birth_country_parent_id"]) == f.trim(df_cnt_tmp["country_id"])
    df = df.join(df_cnt_tmp, join_expr, "left_outer") \
           .withColumnRenamed("continent_name", "birth_continent_name") \
           .withColumnRenamed("region_name", "birth_region_name") \
           .withColumnRenamed("population", "birth_country_population") \
           .withColumnRenamed("area_sq_miles", "birth_country_area_sq_miles") \
           .withColumnRenamed("pop_dencity_per_sq_mile", "birth_country_pop_dencity_per_sq_mile") \
           .withColumnRenamed("coastline", "birth_country_coastline") \
           .withColumnRenamed("net_migration", "birth_country_net_migration") \
           .withColumnRenamed("infant_mortality_per_1000", "birth_country_infant_mortality_per_1000") \
           .withColumnRenamed("gdb_dollar_per_capita", "birth_country_gdb") \
           .withColumnRenamed("percent_literacy", "birth_country_percent_literacy") \
           .withColumnRenamed("phones_per_1000", "birth_country_phones_per_1000") \
           .withColumnRenamed("percent_arable", "birth_country_percent_arable") \
           .withColumnRenamed("percent_crops", "birth_country_percent_crops") \
           .withColumnRenamed("percent_other", "birth_country_percent_other") \
           .withColumnRenamed("climate", "birth_country_climate") \
           .withColumnRenamed("birthrate", "birth_country_birthrate") \
           .withColumnRenamed("deathrate", "birth_country_deathrate") \
           .withColumnRenamed("agriculture", "birth_country_agriculture") \
           .withColumnRenamed("industry", "birth_country_industry") \
           .withColumnRenamed("service", "birth_country_service") \
           .drop("country_id", "country_name")
    
    # Death Country: add only main and geographical information about death place
    join_expr = f.trim(df["death_country_parent_id"]) == f.trim(df_cnt_tmp["country_id"])
    df = df.join(df_cnt_tmp, join_expr, "left_outer") \
           .withColumnRenamed("continent_name", "death_continent_name") \
           .withColumnRenamed("region_name", "death_region_name") \
           .withColumnRenamed("population", "death_country_population") \
           .withColumnRenamed("coastline", "death_country_coastline") \
           .withColumnRenamed("gdb_dollar_per_capita", "death_country_gdb") \
           .withColumnRenamed("climate", "death_country_climate") \
           .drop("country_id", "country_name", "area_sq_miles", "pop_dencity_per_sq_mile", "net_migration", \ 
                 "infant_mortality_per_1000", "percent_literacy", "phones_per_1000", "percent_arable", \
                 "percent_crops", "percent_other", "birthrate", "deathrate", "agriculture", "industry", "service")
    
    # Organization Country - add only main information about organization place
    join_expr = f.trim(df["organization_country_parent_id"]) == f.trim(df_cnt_tmp["country_id"])
    df = df.join(df_cnt_tmp, join_expr, "left_outer") \
           .withColumnRenamed("continent_name", "organization_continent_name") \
           .withColumnRenamed("region_name", "organization_region_name") \
           .withColumnRenamed("population", "organization_continent_name") \
           .withColumnRenamed("gdb_dollar_per_capita", "organization_country_gdb") \
           .drop("country_id", "country_name", "area_sq_miles", "pop_dencity_per_sq_mile", "coastline", "net_migration", \
                 "infant_mortality_per_1000", "percent_literacy", "phones_per_1000", "percent_arable", "percent_crops", \
                 "percent_other", "climate", "birthrate", "deathrate", "agriculture", "industry", "service")
    
    # add City information
    
    # Birth City
    join_expr = f.trim(df["birth_city_id"]) == f.trim(df_city_dwh["city_id"])
    df = df.join(df_city_dwh, join_expr, "left_outer") \
           .withColumnRenamed("city_region", "birth_city_region") \
           .withColumnRenamed("population", "birth_city_population") \
           .withColumnRenamed("latitude", "birth_city_latitude") \
           .withColumnRenamed("longitude", "birth_city_longitude") \
           .drop("city_id", "city_name", "accent_city_name")
    
    # Death City
    join_expr = f.trim(df["death_city_id"]) == f.trim(df_city_dwh["city_id"])
    df = df.join(df_city_dwh, join_expr, "left_outer") \
           .withColumnRenamed("city_region", "death_city_region") \
           .withColumnRenamed("population", "death_city_population") \
           .withColumnRenamed("latitude", "death_city_latitude") \
           .withColumnRenamed("longitude", "death_city_longitude") \
           .drop("city_id", "city_name", "accent_city_name")
    
    # Organization City
    join_expr = f.trim(df["organization_city_id"]) == f.trim(df_city_dwh["city_id"])
    df = df.join(df_city_dwh, join_expr, "left_outer") \
           .withColumnRenamed("city_region", "organization_city_region") \
           .withColumnRenamed("population", "organization_city_population") \
           .withColumnRenamed("latitude", "organization_city_latitude") \
           .withColumnRenamed("longitude", "organization_city_longitude") \
           .drop("city_id", "city_name", "accent_city_name")
    
    # save to temp table if the new records is loaded into existing dataset
    df.write.format("orc") \
      .mode("append") \
      .saveAsTable("akrasnova_tmp.dataset")
    
    # Organization columns
    
    # take all dataset records, not delta only
    df = spark.sql("select * from akrasnova_tmp.dataset")
    
    # 1 laureate can work in more than 1 organization; create 3 sets of organization columns
    w = Window.partitionBy("laureate_id").orderBy(f.col("laureate_id"))
    df = df.withColumn("row_org", f.row_number().over(w))
    
    # create 3 sets of organization's columns.
    df = df.withColumn("organization_name1", \
                f.when(f.col("row_org") == 1, f.col("organization_name")).otherwise(f.lit(None))) \
           .withColumn("organization_name2", \
                f.when(f.col("row_org") == 2, f.col("organization_name")).otherwise(f.lit(None))) \
           .withColumn("organization_name3", \
                f.when(f.col("row_org") == 3, f.col("organization_name")).otherwise(f.lit(None))) \
           .withColumn("organization_city_name1", \
                f.when(f.col("row_org") == 1, f.col("organization_city_name")).otherwise(f.lit(None))) \
           .withColumn("organization_city_name2", \
                f.when(f.col("row_org") == 2, f.col("organization_city_name")).otherwise(f.lit(None))) \
           .withColumn("organization_city_name3", \
                f.when(f.col("row_org") == 3, f.col("organization_city_name")).otherwise(f.lit(None))) \
           .withColumn("organization_country_name1", \
                f.when(f.col("row_org") == 1, f.col("organization_country_name")).otherwise(f.lit(None))) \
           .withColumn("organization_country_name2", \
                f.when(f.col("row_org") == 2, f.col("organization_country_name")).otherwise(f.lit(None))) \
           .withColumn("organization_country_name3", \
                f.when(f.col("row_org") == 3, f.col("organization_country_name")).otherwise(f.lit(None))) \
           .withColumn("organization_country_parent_name1", \
                f.when(f.col("row_org") == 1, f.col("organization_country_parent_name")).otherwise(f.lit(None))) \
           .withColumn("organization_country_parent_name2", \
                f.when(f.col("row_org") == 2, f.col("organization_country_parent_name")).otherwise(f.lit(None))) \
           .withColumn("organization_country_parent_name3", \
                f.when(f.col("row_org") == 3, f.col("organization_country_parent_name")).otherwise(f.lit(None))) \
           .withColumn("organization_parent_name1", \
                f.when(f.col("row_org") == 1, f.col("organization_parent_name")).otherwise(f.lit(None))) \
           .withColumn("organization_parent_name2", \
                f.when(f.col("row_org") == 2, f.col("organization_parent_name")).otherwise(f.lit(None))) \
           .withColumn("organization_parent_name3", \
                f.when(f.col("row_org") == 3, f.col("organization_parent_name")).otherwise(f.lit(None))) \
           .withColumn("organization_region_name1", \
                f.when(f.col("row_org") == 1, f.col("organization_region_name")).otherwise(f.lit(None))) \
           .withColumn("organization_region_name2", \
                f.when(f.col("row_org") == 2, f.col("organization_region_name")).otherwise(f.lit(None))) \
           .withColumn("organization_region_name3", \
                f.when(f.col("row_org") == 3, f.col("organization_region_name")).otherwise(f.lit(None))) \
           .withColumn("organization_country_population1", \
                f.when(f.col("row_org") == 1, f.col("organization_country_population")).otherwise(f.lit(None))) \
           .withColumn("organization_country_population2", \
                f.when(f.col("row_org") == 2, f.col("organization_country_population")).otherwise(f.lit(None))) \
           .withColumn("organization_country_population3", \
                f.when(f.col("row_org") == 3, f.col("organization_country_population")).otherwise(f.lit(None))) \
           .withColumn("organization_country_gdb1", \
                f.when(f.col("row_org") == 1, f.col("organization_country_gdb")).otherwise(f.lit(None))) \
           .withColumn("organization_country_gdb2", \
                f.when(f.col("row_org") == 2, f.col("organization_country_gdb")).otherwise(f.lit(None))) \
           .withColumn("organization_country_gdb3", \
                f.when(f.col("row_org") == 3, f.col("organization_country_gdb")).otherwise(f.lit(None))) \
           .withColumn("organization_continent_name1", \
                f.when(f.col("row_org") == 1, f.col("organization_continent_name")).otherwise(f.lit(None))) \
           .withColumn("organization_continent_name2", \
                f.when(f.col("row_org") == 2, f.col("organization_continent_name")).otherwise(f.lit(None))) \
           .withColumn("organization_continent_name3", \
                f.when(f.col("row_org") == 3, f.col("organization_continent_name")).otherwise(f.lit(None))) \
           .withColumn("organization_city_region1", \
                f.when(f.col("row_org") == 1, f.col("organization_city_region")).otherwise(f.lit(None))) \
           .withColumn("organization_city_region2", \
                f.when(f.col("row_org") == 2, f.col("organization_city_region")).otherwise(f.lit(None))) \
           .withColumn("organization_city_region3", \
                f.when(f.col("row_org") == 3, f.col("organization_city_region")).otherwise(f.lit(None))) \
           .withColumn("organization_city_population1", \
                f.when(f.col("row_org") == 1, f.col("organization_city_population")).otherwise(f.lit(None))) \
           .withColumn("organization_city_population2", \
                f.when(f.col("row_org") == 2, f.col("organization_city_population")).otherwise(f.lit(None))) \
           .withColumn("organization_city_population3", \
                f.when(f.col("row_org") == 3, f.col("organization_city_population")).otherwise(f.lit(None))) \
           .withColumn("organization_city_latitude1", \
                f.when(f.col("row_org") == 1, f.col("organization_city_latitude")).otherwise(f.lit(None))) \
           .withColumn("organization_city_latitude2", \
                f.when(f.col("row_org") == 2, f.col("organization_city_latitude")).otherwise(f.lit(None))) \
           .withColumn("organization_city_latitude3", \
                f.when(f.col("row_org") == 3, f.col("organization_city_latitude")).otherwise(f.lit(None))) \
           .withColumn("organization_city_longitude1", \
                f.when(f.col("row_org") == 1, f.col("organization_city_longitude")).otherwise(f.lit(None))) \
           .withColumn("organization_city_longitude2", \
                f.when(f.col("row_org") == 2, f.col("organization_city_longitude")).otherwise(f.lit(None))) \
           .withColumn("organization_city_longitude3", \
                f.when(f.col("row_org") == 3, f.col("organization_city_longitude")).otherwise(f.lit(None))) \
           .drop("organization_name", "organization_city_name", "organization_country_name", "organization_country_parent_name", \
             "organization_parent_name", "organization_region_name", "organization_country_population", \
             "organization_country_gdb", "organization_continent_name", "organization_city_region", \
             "organization_city_population", "organization_city_latitude", "organization_city_longitude", "id", "row_org")  
    
        # group values by laureate_id. 
        # if organization 1 column is not null, take values of 1 set of organization columns
        # if organization 2 column is not null, take values of 2 set of organization columns
        # else take values of 3 set of organization columns
        df = df.groupBy("laureate_id").agg(
                f.min('year').alias('year'), \
                f.min('category').alias('category'), \
                f.min('prize').alias('prize'), \
                f.min('prize_share').alias('prize_share'), \
                f.min('laureate_type').alias('laureate_type'), \
                f.min('motivation').alias('motivation'), \
                f.min('laureate_person_name').alias('laureate_person_name'), \
                f.min('gender').alias('gender'), \
                f.min('society_name').alias('society_name'), \
                f.min('birth_date').alias('birth_date'), \
                f.min('birth_city_name').alias('birth_city_name'), \
                f.min('birth_city_region').alias('birth_city_region'), \
                f.min('birth_city_population').alias('birth_city_population'), \
                f.min('birth_city_latitude').alias('birth_city_latitude'), \
                f.min('birth_city_longitude').alias('birth_city_longitude'), \
                f.min('birth_country_name').alias('birth_country_name'), \
                f.min('birth_country_parent_name').alias('birth_country_parent_name'), \
                f.min('birth_region_name').alias('birth_region_name'), \
                f.min('birth_continent_name').alias('birth_continent_name'), \
                f.min('birth_country_population').alias('birth_country_population'), \
                f.min('birth_country_area_sq_miles').alias('birth_country_area_sq_miles'), \
                f.min('birth_country_pop_dencity_per_sq_mile').alias('birth_country_pop_dencity_per_sq_mile'), \
                f.min('birth_country_coastline').alias('birth_country_coastline'), \
                f.min('birth_country_net_migration').alias('birth_country_net_migration'), \
                f.min('birth_country_infant_mortality_per_1000').alias('birth_country_infant_mortality_per_1000'), \
                f.min('birth_country_gdb').alias('birth_country_gdb'), \
                f.min('birth_country_percent_literacy').alias('birth_country_percent_literacy'), \
                f.min('birth_country_phones_per_1000').alias('birth_country_phones_per_1000'), \
                f.min('birth_country_percent_arable').alias('birth_country_percent_arable'), \
                f.min('birth_country_percent_crops').alias('birth_country_percent_crops'), \
                f.min('birth_country_percent_other').alias('birth_country_percent_other'), \
                f.min('birth_country_climate').alias('birth_country_climate'), \
                f.min('birth_country_birthrate').alias('birth_country_birthrate'), \
                f.min('birth_country_deathrate').alias('birth_country_deathrate'), \
                f.min('birth_country_agriculture').alias('birth_country_agriculture'), \
                f.min('birth_country_industry').alias('birth_country_industry'), \
                f.min('birth_country_service').alias('birth_country_service'), \
                f.min('death_date').alias('death_date'), \
                f.min('death_city_name').alias('death_city_name'), \
                f.min('death_city_region').alias('death_city_region'), \
                f.min('death_city_population').alias('death_city_population'), \
                f.min('death_city_latitude').alias('death_city_latitude'), \
                f.min('death_city_longitude').alias('death_city_longitude'), \
                f.min('death_country_name').alias('death_country_name'), \
                f.min('death_country_parent_name').alias('death_country_parent_name'), \
                f.min('death_region_name').alias('death_region_name'), \
                f.min('death_continent_name').alias('death_continent_name'), \
                f.min('death_country_population').alias('death_country_population'), \
                f.min('death_country_gdb').alias('death_country_gdb'), \
                f.min('death_country_coastline').alias('death_country_coastline'), \
                f.min('death_country_climate').alias('death_country_climate'), \
                f.min('organization_name1').alias('organization_name1'), \
                f.min('organization_parent_name1').alias('organization_parent_name1'), \
                f.min('organization_city_name1').alias('organization_city_name1'), \
                f.min('organization_city_region1').alias('organization_city_region1'), \
                f.min('organization_city_population1').alias('organization_city_population1'), \
                f.min('organization_city_latitude1').alias('organization_city_latitude1'), \
                f.min('organization_city_longitude1').alias('organization_city_longitude1'), \
                f.min('organization_country_name1').alias('organization_country_name1'), \
                f.min('organization_country_parent_name1').alias('organization_country_parent_name1'), \
                f.min('organization_region_name1').alias('organization_region_name1'), \
                f.min('organization_continent_name1').alias('organization_continent_name1'), \
                f.min('organization_country_population1').alias('organization_country_population1'), \
                f.min('organization_country_gdb1').alias('organization_country_gdb1'), \
                f.min('organization_name2').alias('organization_name2'), \
                f.min('organization_parent_name2').alias('organization_parent_name2'), \
                f.min('organization_city_name2').alias('organization_city_name2'), \
                f.min('organization_city_region2').alias('organization_city_region2'), \
                f.min('organization_city_population2').alias('organization_city_population2'), \
                f.min('organization_city_latitude2').alias('organization_city_latitude2'), \
                f.min('organization_city_longitude2').alias('organization_city_longitude2'), \
                f.min('organization_country_name2').alias('organization_country_name2'), \
                f.min('organization_country_parent_name2').alias('organization_country_parent_name2'), \
                f.min('organization_region_name2').alias('organization_region_name2'), \
                f.min('organization_continent_name2').alias('organization_continent_name2'), \
                f.min('organization_country_population2').alias('organization_country_population2'), \
                f.min('organization_country_gdb2').alias('organization_country_gdb2'), \
                f.min('organization_name3').alias('organization_name3'), \
                f.min('organization_parent_name3').alias('organization_parent_name3'), \
                f.min('organization_city_name3').alias('organization_city_name3'), \
                f.min('organization_city_region3').alias('organization_city_region3'), \
                f.min('organization_city_population3').alias('organization_city_population3'), \
                f.min('organization_city_latitude3').alias('organization_city_latitude3'), \
                f.min('organization_city_longitude3').alias('organization_city_longitude3'), \
                f.min('organization_country_name3').alias('organization_country_name3'), \
                f.min('organization_country_parent_name3').alias('organization_country_parent_name3'), \
                f.min('organization_region_name3').alias('organization_region_name3'), \
                f.min('organization_continent_name3').alias('organization_continent_name3'), \
                f.min('organization_country_population3').alias('organization_country_population3'), \
                f.min('organization_country_gdb3').alias('organization_country_gdb3'), \
                f.max('upd_date').alias('upd_date'))
    
    # save Dataset table
    df.write.format("orc") \
      .partitionBy("year","category","laureate_type","upd_date")
      .mode("overwrite") \
      .saveAsTable("akrasnova_wrh.dataset")
    
    spark_stop()
 