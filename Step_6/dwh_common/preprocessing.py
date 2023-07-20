from pyspark.sql import functions as f

from dwh_common.start_stop_spark import spark_start, spark_stop


# preproceccing - Countries dataset. Update original Countries dataset, create temp tables
def createTmpCountriesTbl(isUpdate=False):
    spark_start()

    df_cnt = spark.sql("select * from akrasnova_tmp.countries_delta") # staging table (delta) Countries
    
    # update the Country Names from the Countries dataset with the country names from the ISO2 dataset.
    # for example, if Country Name = 'Korea, South', the name from the ISO2 dataframe = 'South Korea'
    df_cnt = df_cnt.withColumn("Country", \
                        f.when(f.trim(f.col("Country")) == 'Congo, Repub. of the', 'Republic of the Congo') \
                         .when(f.trim(f.col("Country")) == 'Korea, North', 'North Korea') \
                         .when(f.trim(f.col("Country")) == 'Congo, Dem. Rep.', 'Democratic Republic of the Congo') \
                         .when(f.trim(f.col("Country")) == 'Burma', 'Myanmar') \
                         .when(f.trim(f.col("Country")) == 'Central African Rep.', 'Central African Republic') \
                         .when(f.trim(f.col("Country")) == 'Macau', 'Macao') \
                         .when(f.trim(f.col("Country")) == 'Micronesia, Fed. St.', 'Micronesia') \
                         .when(f.trim(f.col("Country")) == 'Virgin Islands', 'U.S. Virgin Islands') \
                         .when(f.trim(f.col("Country")) == 'N. Mariana Islands', 'Northern Mariana Islands') \
                         .when(f.trim(f.col("Country")) == 'Gambia, The', 'Gambia') \
                         .when(f.trim(f.col("Country")) == 'Cote d\'Ivoire', 'Ivory Coast') \
                         .when(f.trim(f.col("Country")) == 'St Pierre & Miquelon', 'Saint Pierre and Miquelon') \
                         .when(f.trim(f.col("Country")) == 'British Virgin Is.', 'British Virgin Islands') \
                         .when(f.trim(f.col("Country")) == 'Korea, South', 'South Korea') \
                         .when(f.trim(f.col("Country")) == 'Bahamas, The', 'Bahamas') \
                         .when(f.trim(f.col("Country")) == 'Trinidad & Tobago', 'Trinidad and Tobago') \
                         .when(f.trim(f.col("Country")) == 'Bosnia & Herzegovina', 'Bosnia and Herzegovina') \
                         .when(f.trim(f.col("Country")) == 'Antigua & Barbuda', 'Antigua and Barbuda') \
                         .when(f.trim(f.col("Country")) == 'Sao Tome & Principe', 'Sao Tome and Principe') \
                         .when(f.trim(f.col("Country")) == 'Saint Kitts & Nevis', 'Saint Kitts and Nevis') \
                         .when(f.trim(f.col("Country")) == 'Turks & Caicos Is', 'Turks and Caicos Islands') \
                         .otherwise(df_countries_orig["Country"]))
    
    # save updated dataframe to the temp table
    df_cnt.write.format("orc") \
          .mode("overwrite") \
          .saveAsTable("akrasnova_tmp.countries")
    
    # -- if create: start
    if not isUpdate:
    
        df_names = spark.sql("select * from akrasnova_staging.country_names") # staging table ISO2 Codes + Country Names
    
        # records in the Country Codes dataset that are not included in the Countries dataset.
        join_expr = f.trim(df_cnt["Country"]) == f.trim(df_names["country_name"])
        df = df_names.join(df_cnt, join_expr, "left_anti") \
                     .select("country_name", "country_code") \
                     .where("country_code not in ('BQ','CW','SX','PS')")
                     
        # save additional records to the temp table to join these records to the Countires table later
        df.write.format("orc") \
          .mode("overwrite") \
          .saveAsTable("akrasnova_tmp.missing_coutries")

    spark_stop() 
    # -- if create: end    

# preproceccing - history countries
# history names of countries from the Nobel Laureates dataset (Birth, Death, Organization Countries) that are not in the Countries and ISO2 datasets.
CREATE_TMP_HIST_COUNTRIES_TBL = """
/usr/bin/beeline -u jdbc:hive2://localhost:10000/default -n hive -p 123 <<END_SQL
drop table if exists akrasnova_tmp.missing_hist_coutries purge;
create table akrasnova_tmp.missing_hist_coutries (
    country_name  varchar(50)
) stored as orc;
insert into table akrasnova_tmp.missing_hist_coutries
  values 
    ('British Protectorate of Palestine (Israel)'), ('Württemberg (Germany)'), ('Czechoslovakia (Czech Republic)'),
    ('Russian Empire (Latvia)'), ('Trinidad'), ('Austria-Hungary (Poland)'), ('Schleswig (Germany)'),
    ('Russian Empire (Poland)'), ('British India (India)'), ('British West Indies (Saint Lucia)'), ('Tuscany (Italy)'),
    ('Bosnia (Bosnia and Herzegovina)'), ('Austria-Hungary (Ukraine)'), ('Russian Empire (Lithuania)'),
    ('Austria-Hungary (Slovenia)'), ('Austria-Hungary (Hungary)'), ('German-occupied Poland (Poland)'),
    ('Austria-Hungary (Bosnia and Herzegovina)'), ('Faroe Islands (Denmark)'), ('Austrian Empire (Czech Republic)'),
    ('Korea (South Korea)'), ('French Algeria (Algeria)'), ('Ottoman Empire (Turkey)'), ('West Germany (Germany)'),
    ('Austrian Empire (Italy)'), ('Prussia (Germany)'), ('Russian Empire (Ukraine)'), ('British Mandate of Palestine (Israel)'),
    ('Mecklenburg (Germany)'), ('Persia (Iran)'), ('Crete (Greece)'), ('Gold Coast (Ghana)'),
    ('Tibet (People's Republic of China)'), ('Austria-Hungary (Czech Republic)'), ('Prussia (Poland)'),
    ('Russian Empire (Belarus)'), ('Russian Empire (Azerbaijan)'), ('Union of Soviet Socialist Republics (Belarus)'),
    ('Southern Rhodesia (Zimbabwe)'), ('Burma (Myanmar)'), ('Russian Empire (Russia)'), ('Hesse-Kassel (Germany)'),
    ('Austria-Hungary (Croatia)'), ('Free City of Danzig (Poland)'), ('Prussia (Russia)'), ('British India (Bangladesh)'),
    ('Ottoman Empire (Republic of Macedonia)'), ('Austria-Hungary (Austria)'), ('East Friesland (Germany)'),
    ('Union of Soviet Socialist Republics (Russia)'), ('Java, Dutch East Indies (Indonesia)'), ('Russian Empire (Finland)'),
    ('Scotland'), ('Northern Ireland'), ('Bavaria (Germany)'), ('Austrian Empire (Austria)'),
    ('Union of Soviet Socialist Republics'), ('Northern Rhodesia (Zambia)'), ('Czechoslovakia'),
    ('Yugoslavia (Serbia)'), ('East Germany'), ('Federal Republic of Germany'), ('Alsace (then Germany, now France)');
END_SQL
"""

# preproceccing - update original Nobel Laureates dataset: columns Birth Country, Death Country, Organization Country
# common function for create and update airflow's graphs
def createTmpLaureatesTbl():
    spark_start()
    
    df = spark.sql("select * from akrasnova_tmp.nobel_laureates_delta") # staging table (delta) Nobel Laureates
    
    # update countries from the Nobel Laureates dataset by values from the Countries and ISO2 datasets.
    df = df.withColumn("Birth Country",  
                 f.when(f.col("Birth Country") == 'United States of America', 'United States') \
                  .when(f.col("Birth Country") == 'Guadeloupe Island', 'Guadeloupe') \
                  .when((f.col("Birth Country") == 'Germany (Poland)') | (f.col("Birth Country") == 'Germany (France)') \
                        | (f.col("Birth Country") == 'Germany (Russia)'), 'Germany') \
                  .when((f.col("Birth Country") == 'Poland (Ukraine)') | (f.col("Birth Country") == 'Poland (Lithuania)') \
                        | (f.col("Birth Country") == 'Poland (Belarus)'), 'Poland') \
                  .when(f.col("Birth Country") == 'Hungary (Slovakia)', 'Hungary') \
                  .when(f.col("Birth Country") == 'India (Pakistan)', 'India') \
                  .when(f.col("Birth Country") == 'W&uuml;rttemberg (Germany)', \
                        'Württemberg (Germany)') \
                  .otherwise(df["Birth Country"])) \
           .withColumn("Death Country",  
                 f.when(f.col("Death Country") == 'United States of America', 'United States') \
                  .when(f.col("Death Country") == 'Guadeloupe Island', 'Guadeloupe') \
                  .when((f.col("Death Country") == 'Germany (Poland)') | (f.col("Death Country") == 'Germany (France)') \
                        | (f.col("Death Country") == 'Germany (Russia)'), 'Germany') \
                  .when((f.col("Death Country") == 'Poland (Ukraine)') | (f.col("Death Country") == 'Poland (Lithuania)') \
                        | (f.col("Death Country") == 'Poland (Belarus)'), 'Poland') \
                  .when(f.col("Death Country") == 'Hungary (Slovakia)', 'Hungary') \
                  .when(f.col("Death Country") == 'India (Pakistan)', 'India') \
                          .otherwise(df["Death Country"])) \
           .withColumn("Organization Country",  
                 f.when(f.col("Organization Country") == 'United States of America', 'United States') \
                  .when(f.col("Organization Country") == 'Guadeloupe Island', 'Guadeloupe') \
                  .when((f.col("Organization Country") == 'Germany (Poland)') | (f.col("Organization Country") == 'Germany (France)') \
                        | (f.col("Organization Country") == 'Germany (Russia)'), 'Germany') \
                  .when((f.col("Organization Country") == 'Poland (Ukraine)') | (f.col("Organization Country") == 'Poland (Lithuania)') \
                        | (f.col("Organization Country") == 'Poland (Belarus)'), 'Poland') \
                  .when(f.col("Organization Country") == 'Hungary (Slovakia)', 'Hungary') \
                  .when(f.col("Organization Country") == 'India (Pakistan)', 'India') \
                  .otherwise(df["Organization Country"]))
    
    # update null City ID and Country ID for organizations
    df = df.withColumn("Organization City", \
                 f.when(f.trim(f.col("Organization Name")) == 'Howard Hughes Medical Institute', 'Durham, NC') \
                  .when(f.trim(f.col("Organization Name")) == 'University of Delaware', 'Newark, DE') \
                  .otherwise(f.col("Organization City"))) \
           .withColumn("Organization Country", \
                 f.when(f.trim(f.col("Organization Name")) == 'Howard Hughes Medical Institute', 'United States') \
                  .when((f.trim(f.col("Organization Name")) == 'Institut Pasteur') \
                      & (f.trim(f.col("Organization City")) == 'Tunis'), 'Tunisia') \
           .otherwise(f.col("Organization Country")))
    
    # save updated dataframe to the temp table
    df.write.format("orc") \
      .mode("overwrite") \
      .saveAsTable("akrasnova_tmp.nobel_laureates")
    
    spark_stop()  
    
# preproceccing - Cities dataset. Update original Cities dataset, create temp tables
def createTmpCitiesTbl():
    spark_start()

    df = spark.sql("select * from akrasnova_tmp.cities_delta") # staging table (delta) Cities
    
    # the Nobel dataset has the following format for US cities: "city, STATE". 
    # the Cities dataset contains only the City name in the AccentCity column and the state name in the Region column.
    # create a new column in the format "AccentCity, Region" for US cities and "AccentCity" for other cities in the Cities dataset.
    df = df.withColumn("CityUpd", \
                        f.when(f.col("Country") == 'us', f.concat_ws(", ","AccentCity",'Region')) \
                         .otherwise(f.col("AccentCity")))
    
    # save updated dataframe to the temp table
    df.write.format("orc") \
      .mode("overwrite") \
      .saveAsTable("akrasnova_tmp.cities")

    spark_stop()

# preproceccing - creating a temp table with mismatches in city names to join the dataframes of laureates persons and organizations
CREATE_TMP_MIS_CITIES_TBL = """
/usr/bin/beeline -u jdbc:hive2://localhost:10000/default -n hive -p 123 <<END_SQL
drop table if exists akrasnova_tmp.missing_cities purge;
create table akrasnova_tmp.missing_cities (
    mis_city_name varchar(50), 
    orig_city_name varchar(50),
    orig_region varchar(5),
    orig_country varchar(2)
) stored as orc;
insert into table akrasnova_tmp.missing_cities
  values 
    ('Casteldàwson', 'Castledawson', 'S7', 'gb'), ('Grand Valley, CO', 'Green Valley Acres', 'CO', 'us'),
    ('Yamanashi Prefecture', 'Yamanashi', '37', 'jp'), ('Amherst, NS', 'Amherst', '07', 'ca'),
    ('Champaign-Urbana, IL', 'Champaign', 'IL', 'us'), ('Danzig (Gdansk)', 'Danzig', '82', 'pl'),
    ('Fleräng', 'Uppsala', '21', 'se'), ('Kattowitz (Katowice)', 'Kattowitz', '83', 'pl'),
    ('Hobart, Tasmania', 'Hobart', '06', 'au'), ('Leningrad (Saint Petersburg)', 'Leningrad', '66', 'ru'),
    ('Gaffken (Parusnoye)', 'Gaffken', '23', 'ru'), ('Rufford, near Chesterfield', 'Rufford', 'H2', 'gb'),
    ('Goldschmieden, near Breslau', 'Breslau', '72', 'pl'), ('Kibbutz Sde-Nahum', 'Sede Nahum', '03', 'il'),
    ('Hansdorf (Lawice)', 'Hansdorf', '85', 'pl'), ('Toyama City', 'Toyama', '08', 'jp'),
    ('St. Petersburg', 'Sankt-Peterburg', '66', 'ru'), ('Aldea Chimel', 'Chimel', '01', 'gt'),
    ('Vicuña', 'Vicuna', '07', 'cl'), ('Dippenhall', 'Farnham', 'E4', 'gb'),
    ('Königshütte (Chorzów)', 'Königshütte', '14', 'de'), ('Skedsmo', 'Skedsmokorset', '01', 'no'),
    ('Kingston, ON', 'Kingston', '08', 'ca'), ('Leggiuno-Sangiano', 'Varese', '09', 'it'),
    ('Taktser', 'Qinghaihu', '06', 'cn'), ('Gränichen', 'Granichen', '01', 'ch'),
    ('Neisse (Nysa)', 'Neisse', '79', 'pl'), ('St. Columb Minor', 'Saint Columb Minor', 'C6', 'gb'),
    ('Zelvas', 'Zelva', '60', 'lt'), ('Olympus, TN', 'Byrdstown', 'TN', 'us'),
    ('Sorau (Zory)', 'Sorau in Nieder Lausitz', '76', 'pl'), ('Gjesdal', 'Ålgård', '14', 'no'),
    ('Viipuri (Vyborg)', 'Viipuri', '42', 'ru'), ('Kvikne', 'Kviknepladsen', '06', 'no'),
    ('Nuoro, Sardinia', 'Nuoro', '14', 'it'), ('Priluka (Nova Pryluka)', 'Priluka Novaya', '23', 'ua'),
    ('Laibach (Ljubljana)', 'Laibach', '04', 'si'), ('Smyrna (Izmir)', 'Smyrna', '35', 'tr'),
    ('Mexico City', 'Mexico', '09', 'mx'), ('Timmins, ON', 'Timmins', '08', 'ca'),
    ('San José, CA', 'San Jose', 'CA', 'us'), ('Jamaica Plain, MA (Boston)', 'Jamaica Plain', 'MA', 'us'),
    ('Nitzkydorf, Banat', 'Nitzkydorf', '36', 'ro'), ('Waltersdorf (Niegoslawice)', 'Waltersdorf', '76', 'pl'),
    ('Agrigento, Sicily', 'Agrigento', '15', 'it'), ('Medicine Hat, Alberta', 'Medicine Hat', '01', 'ca'),
    ('&#346;eteniai', 'Kedainiai', '60', 'lt'), ('Kharkov (Kharkiv)', 'Kharkov', '07', 'ua'),
    ('Wailacama', 'Dukuh Kupang Timur', '08', 'id'), ('Pressburg (Bratislava)', 'Pressburg', '02', 'sk'),
    ('Lennep (Remscheid)', 'Lennep', '07', 'de'), ('Higashimatsuyama', 'Higashi-Matsuyama', '34', 'jp'),
    ('Nam Ha province', 'Phu Ly', '80', 'vn'), ('Cheetham Hill, near Manchester  ', 'Manchester', 'I2', 'gb'),
    ('Kingston Hill', 'London', 'H9', 'gb'), ('Hofei, Anhwei', 'Hefei', '01', 'cn'),
    ('St. Louis, MO', 'Saint Louis', 'MO', 'us'), ('Dabrovica', 'Dabrowica', '77', 'pl'),
    ('Mount Verno, NY', 'Mount Vernon', 'NY', 'us'), ('Ta'izz', 'Taiz', '11', 'ye'),
    ('Mit Abu al-Kawm', 'Mit Abu al Kawm', '09', 'eg'), ('Chidambaram, Tamil Nadu', 'Chidambaram', '25', 'in'),
    ('Tananarive (Antananarivo)', 'Tananarive', '05', 'mg'), ('Frankfurt-on-the-Main', 'Frankfurt am Main', '05', 'de'),
    ('Hoechst', 'Frankfurt am Main', '05', 'de'), ('Jhang Maghi&#257;na', 'Jhang', '12', 'in'),
    ('Zhejiang Ningbo', 'Ningbo', '14', 'cn'), ('Wilno (Vilnius)', 'Wilno', '65', 'lt'),
    ('Langford Grove, Maldon, Essex', 'Langford', 'E4  ', 'gb'), ('Clausthal (Clausthal-Zellerfeld)', 'Clausthal', '06', 'de'),
    ('Strassburg (Strasbourg)', 'Strassburg', 'C1  ', 'fr'), ('Rangoon (Yangon)', 'Rangoon', '17', 'mm'),
    ('Strehlen (Strzelin)', 'Strehlen', '72', 'pl'), ('Buczacz (Buchach)', 'Buczacz', '22', 'ua'),
    ('Kalgoorlie', 'Boulder', '08', 'au'), ('Nizhny Tagil', 'Nizhnii Tagil', '71', 'ru'),
    ('Bremerhaven-Lehe', 'Bremerhaven', '03', 'de'), ('Corteno', 'Brescia', '09', 'it'),
    ('Windsor, ON', 'Windsor', '08', 'ca'), ('Bnin (Kórnik)', 'Bnin', '86', 'pl'),
    ('Iria Flavia', 'Iria', '58', 'es'), ('Breslau (Wroclaw)', 'Breslau', '72', 'pl'),
    ('Uskup (Skopje)', 'Üsküp', '39', 'tr'), ('Koenigsberg (Kaliningrad)', 'Königsberg', '23', 'ru'),
    ('Reykjavik', 'Reykjavík', '10', 'is'), ('Petilla de Aragó', 'Petilla de Aragón', '32', 'es'),
    (''s Graveland', ''s-Graveland', '07', 'nl'), ('Zloczov', 'Zloczow', '15', 'ua'),
    ('Vishneva', 'Wisniowa', '77', 'pl'), ('St. Paul, MN', 'Saint Paul', 'MN', 'us'),
    ('Kristiania (Oslo)', 'Kristiania', '12', 'no'), ('Rendcombe', 'Cirencester', 'E6', 'gb'),
    ('Vitebsk, Belorussia', 'Vitebsk', '07', 'by'), ('Mürzzuschlag', 'Murzzuschlag', '06', 'at'),
    ('Val di Castello', 'Pietrasanta', '16', 'it'), ('Glencorse', 'Midlothian', 'V5', 'gb'),
    ('Strelno (Strzelno)', 'Strelno', '73', 'pl'), ('Lochfield', 'Lichfield', 'N1', 'gb'),
    ('the Hague', 'The Hague', '11', 'nl'), ('Ivano-Frankivsk', 'IvanoFrankivsk', '06', 'ua'),
    ('Victoria, BC', 'Victoria', '02', 'ca'), ('Lethbridge, Alberta', 'Lethbridge', '01', 'ca'),
    ('Wickenberg, AZ', 'Wickenburg', 'AZ', 'us'), ('Grantchester', 'Cambridgeshire', 'C3', 'gb'),
    ('Wakulla Springs State Park, FL  ', 'Wakulla Springs', 'FL', 'us'), ('Presqu'île-de-Giens', 'Giens', 'B8', 'fr'),
    ('Newfoundland', 'Saint John's', '05', 'ca'), ('New Jersey, NJ', 'Jersey City', 'NJ', 'us'),
    ('Penrhyndeudraeth', 'Penrhyndeudreath', 'Y2', 'gb'), ('Perranarworthal', 'County of Cornwall', 'C6', 'gb'),
    ('Llangarron', 'Ross on Wye', 'F7', 'gb'), ('Kraków', 'Krakow', '77', 'pl'),
    ('Hamilton, Ontario', 'Hamilton', '08', 'ca'), ('Putney Heath', 'London', 'H9', 'gb'),
    ('Shipston-on-Stour', 'Shipston on Stour', 'P3', 'gb'), ('Brive-Corrèze', 'Brive-la-Gaillarde', 'B1', 'fr'),
    ('Övralid', 'Motala', '16', 'se'), ('Bornheim-Merten', 'Merten', '07', 'de'),
    ('Truro, NS', 'Truro', '07', 'ca'), ('Waterford, CT', 'New London', 'CT', 'us'),
    ('West Berlin', 'Berlin', '16', 'de'), ('Palma, Majorca', 'Palma de Mallorca', '07', 'es'),
    ('Ayot St. Lawrence', 'Ayot Saint Lawrence', 'F8', 'gb'), ('Moffett Field, CA', 'Santa Clara', 'CA', 'us'),
    ('Lewes, East Sussex', 'Lewes', 'E2', 'gb'), ('Agnetendorf (Jagniatków)', 'Jelenia Gora', '72', 'pl'),
    ('Research Triangle Park, NC', 'Raleigh', 'NC', 'us'), ('Bucksburn (Scotland)', 'Bucksburn', 'T5', 'gb'),
    ('Lidingö-Stockholm', 'Lidingö', '26', 'se'), ('Breisgau', 'Freiburg im Breisgau', '01', 'de'),
    ('J&uuml;lich', 'Jülich', '07', 'de'), ('Mannheim-Rheinau', 'Rheinau', '01', 'de'),
    ('Sapporo', 'Sapporo-shi', '12', 'jp'), ('Berlin-Dahlem', 'Dahlem', '16', 'de'),
    ('Long Island, New York, NY', 'Long Island City', 'NY', 'us'), ('Harwell, Berkshire', 'Harwell', 'K2', 'gb'),
    ('Argonne, IL', 'Lemont', 'IL', 'us'), ('Altenberg; Grünau im Almtal', 'Altenberg', '04', 'at'),
    ('Massachusetts, MA', 'Boston', 'MA', 'us'), ('Mülheim/Ruhr', 'Mülheim an der Ruhr', '07', 'de'),
    ('Rüschlikon', 'Zurich', '25', 'ch'), ('Kyoto', 'Kyoto-shi', '22', 'jp'),
    ('Guebwiller', 'Guebwiller', 'C1', 'fr'), ('Lahore', 'Lahore', '02', 'pk'),
    ('Thorshavn', 'Thorshavn', '00', 'fo'), ('Cluny', 'Offord Cluny', 'C3', 'gb'),
    ('Lagow', 'Lagow', '72', 'pl'), ('Neuchâtel', 'Neuchâtel', 'A6', 'fr'),
    ('Kaysersberg', 'Kaysersberg', 'C1', 'fr'), ('Dili', 'Dukuh Kupang Timur', '08', 'id'),
    ('Bad Salzbrunn', 'Bad Salzbrunn', '72', 'pl'), ('San Juan', 'San Juan', 'PR', 'us'),
    ('Lanzarote', 'Arrecife', '53', 'es'), ('Hong Kong', 'Hong Kong', '00', 'hk');
END_SQL
"""

# preproceccing - parent organizations that are not in the list of organizations
CREATE_TMP_MIS_ORGS_TBL = """
/usr/bin/beeline -u jdbc:hive2://localhost:10000/default -n hive -p 123 <<END_SQL
drop table if exists akrasnova_tmp.missing_orgs purge;
create table akrasnova_tmp.missing_orgs (
    org_name  varchar(50),
    org_country  varchar(50),
    org_city  varchar(50)
) stored as orc;
insert into table akrasnova_tmp.missing_orgs
  values 
    ('Duke University', 'United States', 'Durham, NC'),
    ('Max Planck Societ', 'Germany', 'Munich'),
    ('The University of California', 'United States', 'Berkeley, CA');
END_SQL
"""