create external table db_staging.country_iso3_ext (
    country string,
    iso3 string
)
comment 'iso3 table - for data in JSON file iso3.json'
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile
location '/staging/country_iso3';

create table db_staging.country_iso3 stored as orc as select * from db_staging.country_iso3_ext;

drop table db_staging.country_iso3_ext;