from pyspark.sql import functions as f
from pyspark.sql.window import Window

from common_dwh.start_stop_spark import spark_start, spark_stop


# create table Countries in Data Warehouse
# information will be loaded from the tables:
#   table countries_delta: the original Countries dataset;
# + initial load:
#   table missing_coutries: data that does not exist in the Countries dataset and exists in the ISO2 codes names.json dataset;
#   table missing_hist_coutries: data with historical names of countries that do not exist today (with added parent_country_id).
def loadCountriesDWH(isUpdate=False):
    spark_start()
    
    df_cnt = spark.sql("select * from select * from akrasnova_tmp.countries") # temp table Countries
    df_reg_dwh = spark.sql("select * from akrasnova_wrh.regions") # DWH table list of Regions
    df_cont_dwh = spark.sql("select * from akrasnova_wrh.continents") # DWH table list of Continents
    df_cont_stg = spark.sql("select * from akrasnova_staging.country_continents") # staging table Country Codes + Continents
    df_cpt_stg = spark.sql("select * from akrasnova_staging.country_capital") # staging table Country Codes + Capitals
    df_names_stg = spark.sql("select * from akrasnova_staging.country_names") # staging table ISO2 country codes + country names
    
    # -- if create: start --
    if not isUpdate:
        df_mis_cnt = spark.sql("select * from akrasnova_tmp.missing_coutries") # temp table Missing Country Names
        df_hist_cnt = spark.sql("select * from akrasnova_tmp.missing_hist_coutries") # temp table History Countries
    
        # dataframe df_mis_cnt with missing country names - create the same structure
        df_mis_cnt = df_mis_cnt.select("country_name") \
            .withColumn("1", f.lit(None)).withColumn("2", f.lit(None)).withColumn("3", f.lit(None)).withColumn("4", f.lit(None)) \
            .withColumn("5", f.lit(None)).withColumn("6", f.lit(None)).withColumn("7", f.lit(None)).withColumn("8", f.lit(None)) \
            .withColumn("9", f.lit(None)).withColumn("10", f.lit(None)).withColumn("11", f.lit(None)).withColumn("12", f.lit(None)) \
            .withColumn("13", f.lit(None)).withColumn("14", f.lit(None)).withColumn("15", f.lit(None)).withColumn("16", f.lit(None)) \
            .withColumn("17", f.lit(None)).withColumn("18", f.lit(None)).withColumn("19", f.lit(None)).withColumn("20", f.lit(None))

        # union dataframes
        df = df_cnt.union(df_mis_cnt)
    # -- if create: end --
    
    # add an ID column to the union dataframes 1 and 2, add null column Parent Country ID
    w = Window.orderBy("Country")
    df = df.withColumn("country_id", f.row_number().over(w) - 1) \
           .withColumn("parent_country_id", f.lit(None).cast('integer'))
    
    # -- if create: start --
    if not isUpdate:
        # check the max value of country_id
        max_count_id = df.select(f.max("country_id")).collect()[0][0]
    # -- if create: end --
    
    # rename columns, update column order
    df = df.withColumnRenamed("Country", "country_name") \
           .withColumnRenamed("Population", "population") \
           .withColumnRenamed("Region", "region") \
           .withColumnRenamed("Area (sq. mi.)", "area_sq_miles") \
           .withColumnRenamed("Pop. Density (per sq. mi.)", "pop_dencity_per_sq_mile") \
           .withColumnRenamed("Coastline (coast/area ratio)", "coastline") \
           .withColumnRenamed("Net migration", "net_migration") \
           .withColumnRenamed("Infant mortality (per 1000 births)", "infant_mortality_per_1000") \
           .withColumnRenamed("GDP ($ per capita)", "gdb_dollar_per_capita") \
           .withColumnRenamed("Literacy (%)", "percent_literacy") \
           .withColumnRenamed("Phones (per 1000)", "phones_per_1000") \
           .withColumnRenamed("Arable (%)", "percent_arable") \
           .withColumnRenamed("Crops (%)", "percent_crops") \
           .withColumnRenamed("Other (%)", "percent_other") \
           .withColumnRenamed("Climate", "climate") \
           .withColumnRenamed("Birthrate", "birthrate") \
           .withColumnRenamed("Deathrate", "deathrate") \
           .withColumnRenamed("Agriculture", "agriculture") \
           .withColumnRenamed("Industry", "industry") \
           .withColumnRenamed("Service", "service") \
           .select("country_id", "parent_country_id", "country_name", "population", "region", "area_sq_miles", \
                   "pop_dencity_per_sq_mile", "coastline", "net_migration", "infant_mortality_per_1000", \
                   "gdb_dollar_per_capita", "percent_literacy", "phones_per_1000", "percent_arable", \
                   "percent_crops", "percent_other", "climate", "birthrate", "deathrate", "agriculture", \
                   "industry", "service", "upd_date")
    
    # -- if create: start --
    if not isUpdate:
        # dataframe df_hist_cnt with historical records
        # add Country ID - starts from the next country_id
        w = Window.orderBy("country_name")
        df_hist_cnt = df_hist_cnt.withColumn("country_id", f.row_number().over(w) + max_count_id)

        # add Parent Country ID = Country ID from the dataframe df created below
        df.createOrReplaceTempView("ORIG_CNT")
        df_hist_cnt.createOrReplaceTempView("HIST_CNT")
        df_tmp = sp.sql("select country_id, \
               case \
               when country_name like '%(Poland)' then (select country_id from ORIG_CNT where trim(country_name) = 'Poland') \
               when country_name like '%(Austria)' then (select country_id from ORIG_CNT where trim(country_name) = 'Austria') \
               when country_name like '%(Croatia)' then (select country_id from ORIG_CNT where trim(country_name) = 'Croatia') \
               when country_name like '%(Czech Republic)' or country_name like '%Czechoslovakia%' \
                    then (select country_id from ORIG_CNT where trim(country_name) = 'Czech Republic') \
               when country_name like '%(Hungary)' then (select country_id from ORIG_CNT where trim(country_name) = 'Hungary') \
               when country_name like '%(Slovenia)' then (select country_id from ORIG_CNT where trim(country_name) = 'Slovenia') \
               when country_name like '%(Ukraine)' then (select country_id from ORIG_CNT where trim(country_name) = 'Ukraine') \
               when country_name like '%(Italy)' then (select country_id from ORIG_CNT where trim(country_name) = 'Italy') \
               when country_name like '%(Germany)' or trim(country_name) in ('East Germany', 'Federal Republic of Germany')\
                    then (select country_id from ORIG_CNT where trim(country_name) = 'Germany') \
               when country_name like '%(Israel)' then (select country_id from ORIG_CNT where trim(country_name) = 'Israel') \
               when country_name like '%(Bangladesh)' then (select country_id from ORIG_CNT where trim(country_name) = 'Bangladesh') \
               when country_name like '%(India)' then (select country_id from ORIG_CNT where trim(country_name) = 'India') \
               when country_name like '%(Saint Lucia)' then (select country_id from ORIG_CNT where trim(country_name) = 'Saint Lucia') \
               when country_name like '%(Greece)' then (select country_id from ORIG_CNT where trim(country_name) = 'Greece') \
               when country_name like '%(Denmark)' then (select country_id from ORIG_CNT where trim(country_name) = 'Denmark') \
               when country_name like '%(Algeria)' then (select country_id from ORIG_CNT where trim(country_name) = 'Algeria') \
               when country_name like '%(Ghana)' then (select country_id from ORIG_CNT where trim(country_name) = 'Ghana') \
               when country_name like '%(Slovakia)' then (select country_id from ORIG_CNT where trim(country_name) = 'Slovakia') \
               when country_name like '%(Turkey)' then (select country_id from ORIG_CNT where trim(country_name) = 'Turkey') \
               when country_name like '%(Zambia)' then (select country_id from ORIG_CNT where trim(country_name) = 'Zambia') \
               when country_name like '%(Iran)' then (select country_id from ORIG_CNT where trim(country_name) = 'Iran') \
               when country_name like '%(Belarus)' then (select country_id from ORIG_CNT where trim(country_name) = 'Belarus') \
               when country_name like '%(Russia)' or country_name like '%Union of Soviet Socialist Republics%' \
                    then (select country_id from ORIG_CNT where trim(country_name) = 'Russia') \
               when country_name like '%(Lithuania)' then (select country_id from ORIG_CNT where trim(country_name) = 'Lithuania') \
               when country_name like '%(Azerbaijan)' then (select country_id from ORIG_CNT where trim(country_name) = 'Azerbaijan') \
               when country_name like '%(Finland)' then (select country_id from ORIG_CNT where trim(country_name) = 'Finland') \
               when country_name like '%(Latvia)' then (select country_id from ORIG_CNT where trim(country_name) = 'Latvia') \
               when country_name like '%France)' then (select country_id from ORIG_CNT where trim(country_name) = 'France') \
               when country_name like '%(Zimbabwe)' then (select country_id from ORIG_CNT where trim(country_name) = 'Zimbabwe') \
               when country_name like '%(Pakistan)' then (select country_id from ORIG_CNT where trim(country_name) = 'Pakistan') \
               when country_name like '%China)' then (select country_id from ORIG_CNT where trim(country_name) = 'China') \
               when country_name like '%(Indonesia)' then (select country_id from ORIG_CNT where trim(country_name) = 'Indonesia') \
               when country_name like '%(Serbia)' then (select country_id from ORIG_CNT where trim(country_name) = 'Serbia') \
               when country_name like '%(Republic of Macedonia)' \
                    then (select country_id from ORIG_CNT where trim(country_name) = 'Macedonia') \
               when country_name like '%(Bosnia%' \
                    then (select country_id from ORIG_CNT where trim(country_name) = 'Bosnia and Herzegovina') \
               when trim(country_name) in ('Northern Ireland', 'Scotland') \
                    then (select country_id from ORIG_CNT where trim(country_name) = 'United Kingdom') \
               when country_name like '%(South Korea)' then (select country_id from ORIG_CNT where trim(country_name) = 'South Korea') \
               when trim(country_name) = 'Trinidad' \
                    then (select country_id from ORIG_CNT where trim(country_name) = 'Trinidad and Tobago') \
               when country_name like '%(Myanmar)' then (select country_id from ORIG_CNT where trim(country_name) = 'Myanmar') \
               else null end as parent_country_id, \
               country_name \
               from HIST_CNT")
        
        # create the same structure of dataframe 3 with historical names
        df_tmp = df_tmp.withColumn("1", f.lit(None)).withColumn("2", f.lit(None)).withColumn("3", f.lit(None)) \
            .withColumn("4", f.lit(None)).withColumn("5", f.lit(None)).withColumn("6", f.lit(None)).withColumn("7", f.lit(None)) \
            .withColumn("8", f.lit(None)).withColumn("9", f.lit(None)).withColumn("10", f.lit(None)).withColumn("11", f.lit(None)) \
            .withColumn("12", f.lit(None)).withColumn("13", f.lit(None)).withColumn("14", f.lit(None)).withColumn("15", f.lit(None)) \
            .withColumn("16", f.lit(None)).withColumn("17", f.lit(None)).withColumn("18", f.lit(None)).withColumn("19", f.lit(None)) \
            .withColumn("20", f.lit(None))

        # union dataframes
        df = df.union(df_tmp)
    # -- if create: end --
    
    # add region_id - link to the Regions table
    join_expr = f.trim(df["region"]) == f.trim(df_reg_dwh["region_name"])
    df = df.join(df_reg_dwh, join_expr, "left_outer").drop("region", "region_name")
    
    # add continent_id - link to the Continents table
    # join dataframes Countries, Continents, ISO2 Code
    join_expr = f.trim(df_names_stg["country_code"]) == f.trim(df_cont_stg["country_code"])
    df_tmp = df_names_stg.join(df_cont_stg, join_expr, "left_outer") \
                            .select("country_name", "continent_code") \
                            .withColumnRenamed("continent_code", "c_code") \
                            .withColumnRenamed("country_name", "c_name")

    join_expr = f.trim(df_tmp["c_code"]) == f.trim(df_cont_dwh["continent_code"])
    df_tmp = df_tmp.join(df_cont_dwh, join_expr, "left_outer").select("c_name", "continent_id")

    join_expr = f.trim(df_tmp["c_name"]) == f.trim(df["country_name"])
    df = df.join(df_tmp, join_expr, "left_outer")
    
    # update continent_id value for the countries that are not in Continent staging table:
    # Gaza Strip and West Bank have the ISO2 code = 'PS' and continent = 'ASIA'
    # Netherlands Antilles have 3 ISO2 codes, but 1 continent = 'NORTH AMERICA'
    df_cont_dwh.createOrReplaceTempView("CONTINENTS")
    asia_id = sp.sql("select continent_id from CONTINENTS where continent_name = 'ASIA'").collect()[0][0]
    na_id = sp.sql("select continent_id from CONTINENTS where continent_name = 'NORTH AMERICA'").collect()[0][0]

    df = df.withColumn("continent_id", \
                f.when(f.trim(f.col("country_name")) == 'Gaza Strip', asia_id) \
                 .when(f.trim(f.col("country_name")) == 'Netherlands Antilles', na_id) \
                 .when(f.trim(f.col("country_name")) == 'West Bank', asia_id) \
                 .otherwise(df_tmp["continent_id"]))
    
    # add Capitals
    # join dataframes Countries, ISO2 Code, Capitals
    join_expr = f.trim(df_names_stg["country_code"]) == f.trim(df_cpt_stg["country_code"])
    df_tmp = df_names_stg.join(df_cpt_stg, join_expr, "left_outer") \
                         .select("country_name", "capital_name") \
                         .withColumnRenamed("country_name", "c_name")

    # + add missing capitals
    join_expr = f.trim(df_tmp["c_name"]) == f.trim(df["country_name"])
    df = df.join(df_tmp, join_expr, "left_outer") \
           .withColumn("capital_name", \
                            f.when(f.col("capital_name") == '', f.lit(None)) \
                             .when(f.trim(f.col("country_name")) == 'Scotland', 'Edinburgh') \
                             .when(f.trim(f.col("country_name")) == 'Northern Ireland', 'Belfast') \
                             .when(f.trim(f.col("country_name")) == 'Netherlands Antilles', 'Willemstad') \
                             .when(f.trim(f.col("country_name")) == 'West Bank', 'East Jerusalem') \
                             .when(f.trim(f.col("country_name")) == 'Gaza Strip', 'East Jerusalem') \
                             .otherwise(f.col("capital_name")))
    
    # update Countries dataset
    df = df.select("country_id", "country_name", "region_id", "continent_id", "parent_country_id", \
                   "capital_name", "population", "area_sq_miles", "pop_dencity_per_sq_mile", "coastline", \
                   "net_migration", "infant_mortality_per_1000", "gdb_dollar_per_capita", "percent_literacy", \
                   "phones_per_1000", "percent_arable", "percent_crops", "percent_other", "climate", \
                   "birthrate", "deathrate", "agriculture", "industry", "service", "upd_date") \
           .sort("country_id")
    
    # save Countries table
    df.write.format("orc") \
      .partitionBy("upd_date")
      .mode("append") \
      .saveAsTable("akrasnova_wrh.countries")
    
    # -- if update: start
    if isUpdate:
    
        # create dataset with up-to-date version of data: choose record with max value of upd_date for records with the same country_name
        # this dataset will be used in other data transformation functions
        df = spark.sql("select * from select * from akrasnova_wrh.countries")
        
        w = Window.partitionBy("country_name").orderBy(f.col("upd_date").desc())
        df = df.withColumn("rank", f.rank().over(w)) \
               .where("rank = 1") \
               .drop("rank")
         
        # save the latest version of data
        df.write.format("orc") \
          .partitionBy("upd_date")
          .mode("overwrite") \
          .saveAsTable("akrasnova_wrh.countries_uptodate")
    # -- if update: end
    
    spark_stop()

# create table Country Codes in Data Warehouse
# contains a list of all possible Country Codes: ISO2, ISO3, country code names, phone codes, link to currency codes
# in most cases, country code names are the same as country names in the Countries dataset.
def loadCountryCodesDWH():
    spark_start()
    
    df_names_stg = spark.sql("select * from akrasnova_staging.country_names") # staging table ISO2 country codes + country names
    df_iso3_stg = spark.sql("select * from akrasnova_staging.country_iso3") # staging table ISO3 + ISO2 country codes
    df_curr_stg = spark.sql("select * from akrasnova_staging.country_currencies") # staging table Currency Codes
    df_curr_dwh = spark.sql("select * from akrasnova_wrh.currencies") # DWH table Currency Codes
    df_phones_stg = spark.sql("select * from akrasnova_staging.country_phones") # staging table Phone Codes
    
    # join ISO3 Codes
    df = df_names_stg.withColumnRenamed("country_code", "country_code1")
    join_expr = f.trim(df["country_code1"]) == f.trim(df_iso3_stg["country_code"])
    df = df.join(df_iso3_stg, join_expr, "left_outer")

    # join Curency Codes
    df = df.withColumnRenamed("country_code", "country_code2")
    join_expr = f.trim(df["country_code1"]) == f.trim(df_curr_stg["country_code"])
    df = df.join(df_curr_stg, join_expr, "left_outer")

    # join Currency ID
    df = df.withColumnRenamed("currency_code", "currency_code1")
    join_expr = f.trim(df["currency_code1"]) == f.trim(df_curr_dwh["currency_code"])
    df = df.join(df_curr_dwh, join_expr, "left_outer")

    # join Phone Codes
    df = df.withColumnRenamed("country_code", "country_code3")
    join_expr = f.trim(df["country_code3"]) == f.trim(df_phones_stg["country_code"])
    df = df.join(df_phones_stg, join_expr, "left_outer")

    # add ID, select and rename columns
    df = df.sort("country_name") \
           .withColumn("country_code_id", f.monotonically_increasing_id()) \
           .withColumnRenamed("country_name", "code_name") \
           .withColumnRenamed("country_code", "iso2_code") \
           .select("country_code_id", "code_name", "iso2_code", "iso3_code", \
                   "currency_code_id", "phone_code")
    
    # save Country Code table
    df.write.format("orc") \
      .mode("overwrite") \
      .saveAsTable("akrasnova_wrh.country_codes")
    
    spark_stop()

# create Codes in Countries table in Data Warehouse
# contains matches between countries and country codes (splitting an 'M-to-N' relationship)
def loadCodesInCntDWH(isUpdate=False):
    spark_start()
    
    # if initial load, use DWH table Countries, else DWH up-to-date table Countries
    table_name = 'countries_uptodate' if isUpdate else 'countries'
    df_cnt = spark.sql("select * from akrasnova_wrh."+table_name).drop("upd_date") # DWH table Countries / up-to-date Countries
    
    df_cc_dwh = spark.sql("select * from akrasnova_wrh.country_codes") # DWH table Country Codes
    
    # join dataframes Country Codes with Countries (not historical records), add matches
    join_expr = f.trim(df_cc_dwh["code_name"]) == f.trim(df_cnt["country_name"])
    df = df_cc_dwh.join(df_cnt.where("parent_country_id is null"), join_expr, "outer") \
                  .withColumn("iso2_code_tmp", \
                                    f.when(f.trim(f.col("country_name")) == 'Gaza Strip', 'PS') \
                                     .when(f.trim(f.col("country_name")) == 'West Bank', 'PS') \
                                     .otherwise(f.col("iso2_code"))) \
                  .withColumn("country_name_tmp", \
                                    f.when(f.trim(f.col("iso2_code")) == 'BQ', 'Netherlands Antilles') \
                                     .when(f.trim(f.col("iso2_code")) == 'CW', 'Netherlands Antilles') \
                                     .when(f.trim(f.col("iso2_code")) == 'SX', 'Netherlands Antilles') \
                                     .otherwise(f.col("country_name"))) \
                  .select("iso2_code_tmp", "country_name_tmp") \
                  .where("country_name_tmp is not null") \
                  .where("iso2_code_tmp is not null")
    
    # add ID, Country ID, Country Code ID
    join_expr = f.trim(df["iso2_code_tmp"]) == f.trim(df_cc_dwh["iso2_code"])
    join_expr1 = f.trim(df["country_name_tmp"]) == f.trim(df_cnt["country_name"])

    # dataframe with ids, iso2 code, country name
    df = df.join(df_cc_dwh, join_expr, "inner") \
           .join(df_cnt, join_expr1, "inner")
    
    # create version of dataframe with iso2_code, country_name, country_id only
    df_tmp = df.select("iso2_code", "country_name", "country_id")
    
    # this version of dataframe will be used in the section Cities, save it to the temp table
    df_tmp.write.format("orc") \
          .mode("overwrite") \
          .saveAsTable("akrasnova_tmp.codes_in_countries")

    # select ids only
    df = df.sort("country_code_id") \
           .withColumn("code_in_country_id", f.monotonically_increasing_id()) \
           .select("code_in_country_id", "country_code_id", "country_id")
    
    # save Codes in Countries table - overwrite data
    df.write.format("orc") \
      .mode("overwrite") \
      .saveAsTable("akrasnova_wrh.codes_in_countries")
    
    spark_stop()

# create Cities table in Data Warehouse
# contains a list of all cities. Created from the original Cities dataset. 
def loadCitiesDWH(isUpdate=False):
    spark_start()
    
    df_cities_tmp = spark.sql("select * from akrasnova_tmp.cities") # temp table ISO2 country codes + country names
    df_cc_tmp = spark.sql("select * from akrasnova_tmp.codes_in_countries") # temp table Codes in Countries
    
    # create Cities dataset, add Country ID
    # use the temp dataset Code in Countries because the list of countries in the Cities dataset is very similar to ISO2 codes
    # temp dataframe with codes in countries to join
    df_cc_tmp = df_cc_tmp.withColumn("iso2_code", f.lower(f.col("iso2_code"))) \
                         .select("iso2_code", "country_name", "country_id")
    
    # exclude records with country = 'zr' from the Cities dataframe as duplicates for the code 'an'
    df = df_cities_tmp.where("Country != 'zr'")
    
    # update the temp Code in the Country dataset - change the codes 'bq', 'cw', 'sx' to 'an', group it into one record
    df_cc_tmp = df_cc_tmp.withColumn("iso2_code", f.when(f.col("iso2_code") == 'bq', 'an') \
                                                   .when(f.col("iso2_code") == 'cw', 'an') \
                                                   .when(f.col("iso2_code") == 'sx', 'an') \
                                                   .otherwise(f.col("iso2_code"))) \
                         .distinct()
    
    # country 'ps' has 2 records in the Countries dataframe: 'West Bank' and 'Gaza Strip'
    # separate the codes in the temp Codes in Countries dataframe
    df_cc_tmp = df_cc_tmp.withColumn("iso2_code", f.when(f.trim(f.col("country_name")) == 'West Bank', 'ps_wb') \
                                                   .when(f.trim(f.col("country_name")) == 'Gaza Strip', 'ps_gs') \
                                                   .otherwise(f.col("iso2_code")))
    
    # divide the codes in the original Cities dataframe by longitude
    df = df.withColumn("CountryUpd", 
                                f.when((f.col("Country") == 'ps') & (f.col("Longitude") <= 34.8), 'ps_gs') \
                                 .when((f.col("Country") == 'ps') & (f.col("Longitude") > 34.8), 'ps_wb') \
                                 .otherwise(f.col("Country")))
    
    # join dataframes - add a Country ID to the Cities dataframe
    join_expr = f.trim(df["CountryUpd"]) == f.trim(df_cc_tmp["iso2_code"])
    df = df_cities.join(df_cc_tmp, join_expr, "left_outer")
    
    # add ID, rename or delete columns
    df = df.sort("country_id", "AccentCity") \
           .withColumn("city_id", f.monotonically_increasing_id()) \
           .withColumnRenamed("City", "city_name") \
           .withColumnRenamed("AccentCity", "accent_city_name") \
           .withColumnRenamed("Region", "city_region") \
           .withColumnRenamed("Population", "population") \
           .withColumnRenamed("Latitude", "latitude") \
           .withColumnRenamed("Longitude", "longitude") \
           .select("city_id", "city_name", "accent_city_name", "country_id", "city_region", "population", \
                   "latitude", "longitude", "CityUpd", "Country", "upd_date")
    
    # the CityUpd and Country columns are required for future joining the Nobel Laureates data frame.
    # save Cities temp dataframe with columns CityUpd and Country to the temp table
    df.write.format("orc") \
      .mode("append") \
      .saveAsTable("akrasnova_tmp.cities2")
    
    # remove CityUpd and Country columns
    df = df.select("city_id", "city_name", "accent_city_name", "country_id", "city_region", "population", \
                   "latitude", "longitude", "upd_date")
    
    # save Cities table
    df.write.format("orc") \
      .partitionBy("country_id","upd_date")
      .mode("append") \
      .saveAsTable("akrasnova_wrh.cities")
    
    # -- if update: start -- 
    if isUpdate:
    
        # create dataset with up-to-date version of data: choose record with max value of upd_date for records with the same city_name
        # this dataset will be used in other data transformation functions
        df_tmp = spark.sql("select * from akrasnova_tmp.cities2")
        
        w = Window.partitionBy("city_name").orderBy(f.col("upd_date").desc())
        df_tmp = df_tmp.withColumn("rank", f.rank().over(w)) \
                       .where("rank = 1") \
                       .drop("rank")
         
        # save the up-to-date version of data with Country and CityUpd columns (will be used later)
        df_tmp.write.format("orc") \
              .partitionBy("upd_date")
              .mode("overwrite") \
              .saveAsTable("akrasnova_tmp.cities2_uptodate")
              
        # remove CityUpd and Country columns
        df_tmp = df_tmp.select("city_id", "city_name", "accent_city_name", "country_id", "city_region", "population", \
                               "latitude", "longitude", "upd_date")
        
        # create and overwrite the up-to-date version of Cities table in DWH
        df_tmp.write.format("orc") \
              .partitionBy("country_id")
              .mode("append") \
              .saveAsTable("akrasnova_wrh.cities_uptodate")
        
    # -- if update: end -- 
    
    # -- if create: start --
    if not isUpdate:
    
        # add city_id to the missing_cities table (mismatches in Cities and Nobel Laureates datasets)
        df_mis_cities = spark.sql("select * from akrasnova_tmp.missing_cities") # temp table with mismatches in City/Nobel Laureates
        
        join_expr = (f.trim(df_mis_cities["orig_city_name"]) == f.trim(df["accent_city_name"])) \
                  & (f.trim(df_mis_cities["orig_region"]) == f.trim(df["city_region"])) \
                  & (f.trim(df_mis_cities["orig_country"]) == f.trim(df["Country"]))

        df_mis_cities = df_mis_cities.join(df, join_expr, "left_outer") \
                                     .select("city_id", "mis_city_name", "orig_city_name", "orig_region", "orig_country") \
                                     .sort("city_id")
        
        # save df_mis_cities to temp table
        df_mis_cities.write.format("orc") \
                     .mode("overwrite") \
                     .saveAsTable("akrasnova_tmp.missing_cities")
    # -- if create: end --
    
    spark_stop()
