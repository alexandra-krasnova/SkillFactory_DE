create external table db_staging.country_names_ext (
    country string,
    name string
)
comment 'names table - for data in JSON file names.json'
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile
location '/staging/country_names';

create table db_staging.country_names stored as orc as select * from db_staging.country_names_ext;

drop table db_staging.country_names_ext;