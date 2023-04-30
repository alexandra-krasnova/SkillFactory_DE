create external table db_staging.country_capital_ext (
    country string,
    capital string
)
comment 'capital table - for data in JSON file capital.json'
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile
location '/staging/country_capital';

create table db_staging.country_capital stored as orc as select * from db_staging.country_capital_ext;

drop table db_staging.country_capital_ext;