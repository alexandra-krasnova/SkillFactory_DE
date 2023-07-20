create external table akrasnova_staging.country_iso3_ext (
    country string,
    iso3 string
)
comment 'iso3 table - for data in JSON file iso3.json'
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile
location '/akrasnova/country_iso3';

create table if not exists akrasnova_staging.country_iso3 stored as orc as select * from akrasnova_staging.country_iso3_ext;

drop table akrasnova_staging.country_iso3_ext;