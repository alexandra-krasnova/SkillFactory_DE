create external table akrasnova_staging.country_continents_ext (
    country string,
    continent string
)
comment 'continent table - for data in JSON file continent.json'
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile
location '/akrasnova/country_continents';

create table if not exists akrasnova_staging.country_continents stored as orc as select * from akrasnova_staging.country_continents_ext;

drop table akrasnova_staging.country_continents_ext;