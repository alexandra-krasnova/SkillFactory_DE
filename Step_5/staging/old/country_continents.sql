create external table db_staging.country_continents_ext (
    country string,
    continent string
)
comment 'continent table - for data in JSON file continent.json'
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile
location '/staging/country_continents';

create table db_staging.country_continents stored as orc as select * from db_staging.country_continents_ext;

drop table db_staging.country_continents_ext;