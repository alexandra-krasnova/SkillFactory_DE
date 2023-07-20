create external table akrasnova_staging.country_names_ext (
    country string,
    name string
)
comment 'names table - for data in JSON file names.json'
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile
location '/akrasnova/country_names';

create table if not exists akrasnova_staging.country_names stored as orc as select * from akrasnova_staging.country_names_ext;

drop table akrasnova_staging.country_names_ext;