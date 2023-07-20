from pyspark.sql import functions as f
from pyspark.sql.window import Window

from dwh_common.start_stop_spark import spark_start, spark_stop


# create Societies and Laureate Person tables in Data Warehouse
# the original ID will be preserved for the Societies and Laureate Persons tables.
def loadLaureates(isUpdate=False):
    spark_start()
    
    df_nobel_tmp = spark.sql("select * from akrasnova_tmp.nobel_laureates") # temp table Nobel Laureates with updated columns
    df_gend_dwh = spark.sql("select * from akrasnova_wrh.genders") # DWH table Genders
    
    # if initial load, use DWH table Countries, else DWH up-to-date table Countries
    table_name = 'countries_uptodate' if isUpdate else 'countries'
    df_cnt_dwh = spark.sql("select * from akrasnova_wrh."+table_name).drop("upd_date") # DWH table Countries / up-to-date Countries
    
    # if initial load, use temp table Cities, else temp up-to-date table Cities
    table_name = 'cities2_uptodate' if isUpdate else 'cities2'
    df_cities_tmp = spark.sql("select * from akrasnova_tmp."+table_name).drop("upd_date") # temp table Cities / up-to-date Countries
    
    if not isUpdate:
        df_mis_cities = spark.sql("select * from akrasnova_tmp.missing_cities") # temp table Missing Cities
    
    # create Societies table: laureates with laureate type = "Organization"
    df = df_nobel_tmp.withColumnRenamed("Laureate Type", "laureate_type") \
                     .withColumnRenamed("Laureate ID", "laureate_id") \
                     .withColumnRenamed("Full Name", "society_name") \
                     .where("laureate_type = 'Organization'") \
                     .select("laureate_id", "society_name", "upd_date") \
                     .distinct() \
                     .sort("laureate_id")
    
    # -- if update: start
    if isUpdate:
    
        df_prev = spark.sql("select * from akrasnova_wrh.societies") # previous records
        df = df_prev.union(df) # previous + current records
        
        # update dataset with up-to-date version of data: choose record with max value of upd_date for records with the same society_name
        w = Window.partitionBy("society_name").orderBy(f.col("upd_date").desc())
        df = df.withColumn("rank", f.rank().over(w)) \
               .where("rank = 1") \
               .drop("rank") \
               .select("laureate_id", "society_name", "upd_date") \
               .sort("laureate_id")
    # -- if update: end
    
    # save Societies table
    df.write.format("orc") \
      .partitionBy("upd_date")
      .mode("overwrite") \
      .saveAsTable("akrasnova_wrh.societies")
    
    # create Laureate Persons table: laureates with laureate type = "Individual"
    df = df_nobel_tmp.withColumnRenamed("Laureate Type", "laureate_type") \
                     .withColumnRenamed("Laureate ID", "laureate_id") \
                     .withColumnRenamed("Full Name", "full_name") \
                     .withColumnRenamed("Birth Date", "birth_date") \
                     .withColumnRenamed("Birth City", "birth_city") \
                     .withColumnRenamed("Birth Country", "birth_country") \
                     .withColumnRenamed("Death Date", "death_date") \
                     .withColumnRenamed("Death City", "death_city") \
                     .withColumnRenamed("Death Country", "death_country") \
                     .where("laureate_type = 'Individual'") \
                     .select("laureate_id", "full_name", "Sex", "birth_date", "birth_city", \
                             "birth_country", "death_date", "death_city", "death_country", "upd_date") \
                     .distinct()
    
    # add Gender ID
    join_expr = f.trim(df["Sex"]) == f.trim(df_gend_dwh["gender"])
    df = df.join(df_gend_dwh, join_expr, "left_outer") \
           .select("laureate_id", "full_name", "gender_id", "birth_date", "birth_city", \
                   "birth_country", "death_date", "death_city", "death_country", "upd_date")
    
    # add Birth Country ID
    join_expr = f.trim(df["birth_country"]) == f.trim(df_cnt_dwh["country_name"])
    df = df.join(df_cnt_dwh, join_expr, "left_outer") \
           .withColumnRenamed("country_id", "birth_country_id") \
           .withColumnRenamed("parent_country_id", "birth_parent_country_id") \
           .select("laureate_id", "full_name", "gender_id", "birth_date", "birth_city", \
                   "birth_country_id", "death_date", "death_city", "death_country", \
                   "birth_country", "birth_parent_country_id", "upd_date")
    
    # add Death Country ID
    join_expr = f.trim(df["death_country"]) == f.trim(df_cnt_dwh["country_name"])
    df = df.join(df_cnt_dwh, join_expr, "left_outer") \
           .withColumnRenamed("country_id", "death_country_id") \
           .withColumnRenamed("parent_country_id", "death_parent_country_id") \
           .select("laureate_id", "full_name", "gender_id", "birth_date", "birth_city", \
                   "birth_country_id", "death_date", "death_city", "death_country_id", \
                   "birth_parent_country_id", "death_parent_country_id", "death_country", "upd_date")
    
    # add Birth City ID
    
    # add existing in the Cities dataframe names - current countries
    # join Cities dataframe by city name + country ID
    join_expr = (f.trim(df["birth_city"]) == f.trim(df_cities_tmp["CityUpd"])) \
              & (df["birth_country_id"] == df_cities_tmp["country_id"])
    df = df.join(df_cities_tmp, join_expr, "left_outer") \
           .withColumnRenamed("city_id", "birth_city_id") \
           .drop("city_name", "accent_city_name", "country_id", "city_region", "population", \
                 "latitude", "longitude", "CityUpd", "Country")

    # some cities are not unique by "City Name" + "Country". Remove duplicates from dataframe, use first City ID number
    w = Window.partitionBy("laureate_id").orderBy(f.col("laureate_id"))
    df = df.withColumn("row", f.row_number().over(w)) \
           .filter(f.col("row") == 1) \
           .drop("row")
    
    # add existing in the Cities dataframe names - historical countries
    # join Cities dataframe by city name + parent country ID
    join_expr = (f.trim(df["birth_city"]) == f.trim(df_cities_tmp["CityUpd"])) \
              & (df["birth_parent_country_id"] == df_cities_tmp["country_id"])
    df = df.join(df_cities_tmp, join_expr, "left_outer") \
           .withColumn("birth_city_id", \
                        f.when(f.col("birth_city_id") >= 0, f.col("birth_city_id")) \
                         .otherwise(f.col("city_id"))) \
           .drop("city_name", "accent_city_name", "country_id", "city_region", "population", \
                 "latitude", "longitude", "CityUpd", "Country", "city_id")

    # Some cities are not unique by "City Name" + "Country". Remove duplicates from dataframe, use first City ID number
    w = Window.partitionBy("laureate_id").orderBy(f.col("laureate_id"))
    df = df.withColumn("row", f.row_number().over(w)) \
           .filter(f.col("row") == 1) \
           .drop("row")
    
    # -- if create: start
    if not isUpdate:
        # add missing in the Cities dataframe names
        # join missing city names to the Laureates dataframe - use the table akrasnova_tmp.missing_cities created before
        join_expr = f.trim(df["birth_city"]) == f.trim(df_mis_cities["mis_city_name"])
        df = df.join(df_mis_cities, join_expr, "left_outer") \
               .withColumn("birth_city_id", \
                            f.when(f.col("birth_city_id") >= 0, f.col("birth_city_id")) \
                             .otherwise(f.col("city_id"))) \
               .select("laureate_id", "full_name", "gender_id", "birth_date", "birth_city_id", \
                       "birth_country_id", "death_date", "death_city", "death_country_id", \
                       "birth_parent_country_id", "death_parent_country_id", "birth_city", "upd_date")
    # -- if create: end
    
    # add Death City ID
    
    # add existing in the Cities dataframe names - current countries
    # join Cities dataframe by city name + country ID
    join_expr = (f.trim(df["death_city"]) == f.trim(df_cities_tmp["CityUpd"])) \
              & (df["death_country_id"] == df_cities_tmp["country_id"])
    df = df.join(df_cities_tmp, join_expr, "left_outer") \
           .withColumnRenamed("city_id", "death_city_id") \
           .drop("city_name", "accent_city_name", "country_id", "city_region", "population", \
                 "latitude", "longitude", "CityUpd", "Country")

    # Some cities are not unique by "City Name" + "Country". Remove duplicates from dataframe, use first City ID number
    w = Window.partitionBy("laureate_id").orderBy(f.col("laureate_id"))
    df = df.withColumn("row", f.row_number().over(w)) \
           .filter(f.col("row") == 1) \
           .drop("row")
    
    # add existing in the Cities dataframe names - historical countries
    # join Cities dataframe by city name + parent country ID
    join_expr = (f.trim(df["death_city"]) == f.trim(df_cities_tmp["CityUpd"])) \
              & (df["death_parent_country_id"] == df_cities_tmp["country_id"])
    df = df.join(df_cities_tmp, join_expr, "left_outer") \
           .withColumn("death_city_id", \
                        f.when(f.col("death_city_id") >= 0, f.col("death_city_id")) \
                         .otherwise(f.col("city_id"))) \
           .drop("city_name", "accent_city_name", "country_id", "city_region", "population", \
                 "latitude", "longitude", "CityUpd", "Country", "city_id")

    # some cities are not unique by "City Name" + "Country". Remove duplicates from dataframe, use first City ID number
    w = Window.partitionBy("laureate_id").orderBy(f.col("laureate_id"))
    df = df.withColumn("row", f.row_number().over(w)) \
           .filter(f.col("row") == 1) \
           .drop("row")
    
    # -- if create: start
    if not isUpdate:
        # add missing in the Cities dataframe names
        # join missing city names to the Laureates dataframe - use the table akrasnova_tmp.missing_cities created before
        join_expr = f.trim(df["death_city"]) == f.trim(df_mis_cities["mis_city_name"])
        df = df.join(df_mis_cities, join_expr, "left_outer") \
               .withColumn("death_city_id", \
                            f.when(f.col("death_city_id") >= 0, f.col("death_city_id")) \
                             .otherwise(f.col("city_id"))) \
               .select("laureate_id", "full_name", "gender_id", "birth_date", "birth_city_id", \
                       "birth_country_id", "death_date", "death_city_id", "death_country_id", \
                       "birth_parent_country_id", "death_parent_country_id", "death_city", "upd_date")
    # -- if create: end
    
    # delete columns, sorting records
    df = df.select("laureate_id", "full_name", "gender_id", "birth_date", "birth_city_id", \
                   "birth_country_id", "death_date", "death_city_id", "death_country_id", "upd_date") \
           .sort("laureate_id")
    
    # -- if update: start
    if isUpdate:
    
        df_prev = spark.sql("select * from akrasnova_wrh.laureate_persons") # previous records
        df = df_prev.union(df) # previous + current records
        
        # update dataset with up-to-date version of data: choose record with max value of upd_date for records with the same laureate name + date of birth
        w = Window.partitionBy("full_name","birth_date").orderBy(f.col("upd_date").desc())
        df = df.withColumn("rank", f.rank().over(w)) \
               .where("rank = 1") \
               .drop("rank") \
               .select("laureate_id", "full_name", "gender_id", "birth_date", "birth_city_id", \
                       "birth_country_id", "death_date", "death_city_id", "death_country_id", "upd_date") \
               .sort("laureate_id")
    # -- if update: end
    
    # save Laureate Persons table
    df.write.format("orc") \
      .partitionBy("gender_id","upd_date")
      .mode("overwrite") \
      .saveAsTable("akrasnova_wrh.laureate_persons")
    
    spark_stop()

# create Organizations table in Data Warehouse
# contains a list of organizations where the laureates ("individual") worked
def loadOrganizations(isUpdate=False):
    spark_start()
    
    df_nobel_tmp = spark.sql("select * from akrasnova_tmp.nobel_laureates") # temp table Nobel Laureates with updated columns
    
    # if initial load, use DWH table Countries, else DWH up-to-date table Countries
    table_name = 'countries_uptodate' if isUpdate else 'countries'
    df_cnt_dwh = spark.sql("select * from akrasnova_wrh."+table_name).drop("upd_date") # DWH table Countries / up-to-date Countries
    
    # if initial load, use temp table Cities, else temp up-to-date table Cities
    table_name = 'cities2_uptodate' if isUpdate else 'cities2'
    df_cities_tmp = spark.sql("select * from akrasnova_tmp."+table_name).drop("upd_date") # temp table Cities / up-to-date Countries
    
    if not isUpdate:
        df_mis_orgs = spark.sql("select * from akrasnova_tmp.missing_orgs") # temp table missing parents organization
        df_mis_cities = spark.sql("select * from akrasnova_tmp.missing_cities") # temp table Missing Cities
    
    # Ogranizations dataframe
    df = df.withColumnRenamed("Laureate Type", "laureate_type") \
           .withColumnRenamed("Organization Name", "organization_name") \
           .withColumnRenamed("Organization Country", "organization_country") \
           .withColumnRenamed("Organization City", "organization_city") \
           .select("organization_name", "organization_country", "organization_city", "upd_date") \
           .where("laureate_type = 'Individual' and organization_name is not null") \
           .distinct() \
           .sort("organization_name")

    
    # add Organization ID
    
    # -- if update: start
    if isUpdate:
        df = df.withColumn("organization_id", f.monotonically_increasing_id()) \
               .select("organization_id", "organization_name", "organization_country", "organization_city", "upd_date")
    # -- if update: end
    
    # -- if create: start
    else:
        # add Organization ID
        w = Window.orderBy("organization_name")
        df = df.withColumn("organization_id", f.row_number().over(w) - 1) \
           .select("organization_id", "organization_name", "organization_country", "organization_city", "upd_date")
        
        # add parent organizations that are not in the list of organizations
        
        # check the max value of organization_id
        max_count_id = df.select(f.max("organization_id")).collect()[0][0]

        # add organization_id - starts from the next organization_id
        w = Window.orderBy("org_name")
        df_mis_orgs = df_mis_orgs.withColumn("org_id", f.row_number().over(w) + max_count_id) \
                                 .select("org_id", "org_name", "org_country", "org_city")
    
        # union dataframes
        df = df.union(df_tmp_mis_org)
    
        # add Parent Organization ID
        df.createOrReplaceTempView("ORG")
        df = sp.sql("select organization_id, organization_name, organization_city, organization_country, \
           case \
           when trim(organization_name) = 'Australian National University' and trim(organization_city) = 'Weston Creek' \
                then (select organization_id from ORG where trim(organization_name) = 'Australian National University' \
                and trim(organization_city) = 'Canberra') \
            when (trim(organization_name) = 'Bell Laboratories' and trim(organization_city) = 'Holmdel, NJ') \
            or trim(organization_name) = 'Bell Telephone Laboratories' \
                then (select organization_id from ORG where trim(organization_name) = 'Bell Laboratories' \
                and trim(organization_city) = 'Murray Hill, NJ') \
            when organization_name like 'Columbia University Division%'  \
                then (select organization_id from ORG where trim(organization_name) = 'Columbia University') \
            when organization_name like 'Duke University %'  \
                then (select organization_id from ORG where trim(organization_name) = 'Duke University') \
            when organization_name like '%Fritz-Haber-Institut%' or organization_name like '%Max-Planck%' \
                or organization_name like '%Max Planck Institute%' \
                then (select organization_id from ORG where trim(organization_name) = 'Max Planck Society') \
            when organization_name like 'Harvard University,%'  \
                then (select organization_id from ORG where trim(organization_name) = 'Harvard University') \
            when trim(organization_name) = 'I.G. Farbenindustrie A.G.' and trim(organization_city) = 'Heidelberg' \
                then (select organization_id from ORG where trim(organization_name) = 'I.G. Farbenindustrie A.G.' \
                and trim(organization_city) = 'Mannheim-Rheinau') \
            when trim(organization_name) = 'Imperial Cancer Research Fund Laboratory' \
                then (select organization_id from ORG where trim(organization_name) = 'Imperial Cancer Research Fund') \
            when trim(organization_name) = 'Johns Hopkins University School of Medicine' \
                then (select organization_id from ORG where trim(organization_name) = 'Johns Hopkins University') \
            when trim(organization_name) = 'Karolinska Institutet, Nobel Medical Institute' \
                then (select organization_id from ORG where trim(organization_name) = 'Karolinska Institutet') \
            when trim(organization_name) = 'Kiel University' and trim(organization_country) = 'Federal Republic of Germany' \
                then (select organization_id from ORG where trim(organization_name) = 'Kiel University' \
                and trim(organization_country) = 'Germany') \
            when organization_name like 'London University,%' \
                then (select organization_id from ORG where trim(organization_name) = 'London University') \
            when organization_name like 'Massachusetts Institute of Technology (MIT),%' \
                then (select organization_id from ORG \
                where trim(organization_name) = 'Massachusetts Institute of Technology (MIT)') \
            when trim(organization_name) = 'New York University, College of Medicine' \
                then (select organization_id from ORG where trim(organization_name) = 'New York University') \
            when trim(organization_name) = 'P.N. Lebedev Physical Institute' \
            and trim(organization_country) = 'Union of Soviet Socialist Republics' \
                then (select organization_id from ORG where trim(organization_name) = 'P.N. Lebedev Physical Institute' \
                and trim(organization_country) = 'Russia') \
            when trim(organization_name) = 'Rockefeller Institute for Medical Research' \
                then (select organization_id from ORG where trim(organization_name) = 'Rockefeller University') \
            when trim(organization_name) = 'Sorbonne University, Institut Henri Poincaré' \
                then (select organization_id from ORG where trim(organization_name) = 'Sorbonne University') \
            when trim(organization_name) = 'Stanford University School of Medicine' \
                then (select organization_id from ORG where trim(organization_name) = 'Stanford University') \
            when organization_name like 'University of California%' and trim(organization_city) != 'Berkeley, CA' \
                then (select organization_id from ORG where trim(organization_name) = 'University of California' \
                and trim(organization_city) = 'Berkeley, CA') \
            when trim(organization_name) = 'University of Chicago, Ben May Laboratory for Cancer Research' \
                then (select organization_id from ORG where trim(organization_name) = 'University of Chicago') \
            when trim(organization_name) = 'University of Colorado, JILA' \
                then (select organization_id from ORG where trim(organization_name) = 'University of Colorado') \
            when trim(organization_name) = 'University of Freiburg' \
                then (select organization_id from ORG where trim(organization_name) = 'University of Freiburg im Breisgau') \
            when trim(organization_name) = 'University of Heidelberg' and trim(organization_country) = 'Federal Republic of Germany' \
                then (select organization_id from ORG where trim(organization_name) = 'University of Heidelberg' \
                and trim(organization_country) = 'Germany') \
            when trim(organization_name) = 'University of Oxford, Royal Society' \
                then (select organization_id from ORG where trim(organization_name) = 'University of Oxford') \
            when organization_name like 'University of Texas %' \
                then (select organization_id from ORG where trim(organization_name) = 'University of Texas') \
            when trim(organization_name) = 'University of Tokyo' and trim(organization_city) = 'Kashiwa' \
                then (select organization_id from ORG where trim(organization_name) = 'University of Tokyo' \
                and trim(organization_city) = 'Tokyo') \
            when trim(organization_name) = 'University of Zurich, Institute of Experimental Immunology' \
                then (select organization_id from ORG where trim(organization_name) = 'University of Zurich') \
            when trim(organization_name) = 'Vanderbilt University School of Medicine' \
                then (select organization_id from ORG where trim(organization_name) = 'Vanderbilt Universitye') \
            when trim(organization_name) = 'Veterans Administration Hospital' and trim(organization_city) = 'New Orleans, LA' \
                then (select organization_id from ORG where trim(organization_name) = 'Veterans Administration Hospital' \
                and trim(organization_city) = 'Bronx, NY') \
            when trim(organization_name) = 'Yale University, School of Medicine' \
                then (select organization_id from ORG where trim(organization_name) = 'Yale University') \
           else null end as parent_organization_id \
           from ORG")
    # -- if create: end
    
    # add Organization Country ID
    join_expr = f.trim(df["organization_country"]) == f.trim(df_cnt_dwh["country_name"])
    df = df.join(df_cnt_dwh, join_expr, "left_outer") \
           .withColumnRenamed("country_id", "organization_country_id") \
           .withColumnRenamed("parent_country_id", "organization_parent_country_id") \
           .select("organization_id", "organization_name", "organization_country", "organization_country_id", \
                   "organization_city", "organization_parent_country_id", "parent_organization_id", "upd_date")
    
    # add Organization City ID
    
    # add existing in the Cities dataframe names - current countries
    # join Cities dataframe by city name + country ID
    join_expr = (f.trim(df["organization_city"]) == f.trim(df_cities_tmp["CityUpd"])) \
              & (df["organization_country_id"] == df_cities_tmp["country_id"])
    df = df.join(df_cities_tmp, join_expr, "left_outer") \
           .withColumnRenamed("city_id", "organization_city_id") \
           .drop("city_name", "accent_city_name", "country_id", "city_region", "population", \
                 "latitude", "longitude", "CityUpd", "Country")

    # some cities are not unique by "City Name" + "Country". Remove duplicates from dataframe, use first City ID number
    w = Window.partitionBy("organization_id").orderBy(f.col("organization_id"))
    df = df.withColumn("row", f.row_number().over(w)) \
           .filter(f.col("row") == 1)
           .drop("row")
    
    # add existing in the Cities dataframe names - historical countries
    # join Cities dataframe by city name + parent country ID
    join_expr = (f.trim(df["organization_city"]) == f.trim(df_cities_tmp["CityUpd"])) \
              & (df["organization_parent_country_id"] == df_cities_tmp["country_id"])
    df = df.join(df_cities_tmp, join_expr, "left_outer") \
           .withColumn("organization_city_id", \
                        f.when(f.col("organization_city_id") >= 0, f.col("organization_city_id")) \
                         .otherwise(f.col("city_id"))) \
           .drop("city_name", "accent_city_name", "country_id", "city_region", "population", \
                 "latitude", "longitude", "CityUpd", "Country", "city_id")

    # some cities are not unique by "City Name" + "Country". Remove duplicates from dataframe, use first City ID number
    w = Window.partitionBy("organization_id").orderBy(f.col("organization_id"))
    df = df.withColumn("row", f.row_number().over(w)) \
           .filter(f.col("row") == 1)
           .drop("row")
    
    # -- if create: start
    if not isUpdate:
        # add missing in the Cities dataframe names
        # join missing city names to the Laureates dataframe - use the dataframe df_mis_cities created above
        join_expr = f.trim(df["organization_city"]) == f.trim(df_mis_cities["mis_city_name"])
        df = df.join(df_mis_cities, join_expr, "left_outer") \
               .withColumn("organization_city_id", \
                                    f.when(f.col("organization_city_id") >= 0, f.col("organization_city_id")) \
                                     .otherwise(f.col("city_id"))) \
               .select("organization_id", "organization_name", "organization_country_id", \
                       "organization_city_id", "parent_organization_id", \
                       "organization_country", "organization_city", "upd_date") \
               .sort("organization_id")
    # -- if create: end
    
    # the datafame with Organization Country and Organization City columns will be used in Persons in Orgs dataset. 
    # create up-to-date version for Update and save it to the temp table
    
    # -- if update: start
    if isUpdate:
    
        df_prev = spark.sql("select * from akrasnova_tmp.organizations") # previous records
        df = df_prev.union(df) # previous + current records
        
        # update dataset with up-to-date version of data: choose record with max value of upd_date for records with the same organization_name
        w = Window.partitionBy("organization_name").orderBy(f.col("upd_date").desc())
        df = df.withColumn("rank", f.rank().over(w)) \
               .where("rank = 1") \
               .drop("rank") \
               .select("organization_id", "organization_name", "organization_country_id", \
                       "organization_city_id", "parent_organization_id", \
                       "organization_country", "organization_city", "upd_date") \
               .sort("organization_id")
    # -- if update: end

    # save it to the temp table
    df.write.format("orc") \
      .mode("overwrite") \
      .saveAsTable("akrasnova_tmp.organizations")
    
    # remove columns Organization Country and Organization City
    df = df.select("organization_id", "organization_name", "organization_country_id", \
                   "organization_city_id", "parent_organization_id", "upd_date")
    
    # save Organizations table
    df.write.format("orc") \
      .partitionBy("upd_date")
      .mode("overwrite") \
      .saveAsTable("akrasnova_wrh.organizations")
    
    spark_stop()

# create Persons in Organizations table
# contains the matches of the Laureate Persons and Organizations in which the laureates worked.
def loadPersonsInOrgs(isUpdate=False):
    spark_start()
    
    df_nob_tmp = spark.sql("select * from akrasnova_tmp.nobel_laureates").drop("upd_date") # temp table Nobel Laureates with updated columns
    df_org_tmp = spark.sql("select * from akrasnova_tmp.organizations").drop("upd_date") # temp table Organizations
    
    # use the original Nobel Prizes dataset
    df = df_nob_tmp.withColumnRenamed("Laureate Type", "laureate_type") \
                   .withColumnRenamed("Laureate ID", "laureate_id") \
                   .withColumnRenamed("Organization Name", "org_name") \
                   .withColumnRenamed("Organization Country", "org_country") \
                   .withColumnRenamed("Organization City", "org_city") \
                   .where("laureate_type = 'Individual' and org_name is not null") \
                   .select("laureate_id", "org_name", "org_country", "org_city") \
                   .distinct() \
                   .sort("laureate_id", "org_name") \
                   .withColumn("per_org_id", f.monotonically_increasing_id())
    
    # add Organization Id
    join_expr = (f.trim(df_org_tmp["organization_name"]) == f.trim(df["org_name"])) \
              & (f.trim(df_org_tmp["organization_country"]) == f.trim(df["org_country"])) \
              & (f.trim(df_org_tmp["organization_city"]) == f.trim(df["org_city"]))
    df = df.join(df_org_tmp, join_expr, "inner") \
           .select("per_org_id", "laureate_id", "organization_id", "organization_name", "upd_date") \
           .sort("per_org_id")
    
    # -- if update: start
    if isUpdate:
    
        df_prev = spark.sql("select * from akrasnova_wrh.persons_in_orgs") # previous records
        df = df_prev.union(df) # previous + current records
        
        # update dataset with up-to-date version of data: choose record with max value of upd_date for records with the same laur_id+org_name
        w = Window.partitionBy("laureate_id","organization_name").orderBy(f.col("upd_date").desc())
        df = df.withColumn("rank", f.rank().over(w)) \
               .where("rank = 1") \
               .drop("rank") \
               .select("per_org_id", "laureate_id", "organization_id", "organization_name", "upd_date") \
               .sort("per_org_id")
    # -- if update: end
    
    # save Persons in Organizations table
    df.write.format("orc") \
      .mode("overwrite") \
      .saveAsTable("akrasnova_wrh.persons_in_orgs")
    
    spark_stop()
