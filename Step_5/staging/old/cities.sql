create external table if not exists db_staging.cities_ext (
    country string,
    city string,
    accentcity string,
    region int,
    population int,
    latitude decimal(20,17),
    longitude decimal(20,17)
)
comment 'Cities table - for data in CSV file worldcitiespop.csv'
--row format delimited
--fields terminated by ','
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\"")
stored as textfile
location '/staging/cities'
tblproperties ('skip.header.line.count'='1');

create table db_staging.cities stored as orc as select * from db_staging.cities_ext;

drop table db_staging.cities_ext;