create external table if not exists db_staging.countries_of_the_world_ext (
    country varchar(50),
    region varchar(50),
    population int,
    area_sq_mi int,
    pop_density_per_sq_mi decimal(8,2),
    coastline decimal(7,3),
    net_migration decimal(6,3),
    infant_mortality_per_1000_births decimal(7,3),
    gdp_dol_per_capita int,
    literacy_percent decimal(6,3),
    phones_per_1000 decimal(7,3),
    arable_percent decimal(6,3),
    crops_percent decimal(6,3),
    other_percent decimal(6,3),
    climate decimal(4,2),
    birthrate decimal(6,3),
    deathrate decimal(6,3),
    agriculture decimal(5,4),
    industry decimal(5,4),
    service decimal(5,4)
)
comment 'Countries table - for data in CSV file countries_of_the_world.csv'
--row format delimited
--fields terminated by ','
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\"")
stored as textfile
location '/staging/countries_of_the_world'
tblproperties ('skip.header.line.count'='1');

create table db_staging.countries_of_the_world stored as orc as select * from db_staging.countries_of_the_world_ext;

drop table db_staging.countries_of_the_world_ext;