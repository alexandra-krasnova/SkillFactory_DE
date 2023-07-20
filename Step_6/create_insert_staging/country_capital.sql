create external table akrasnova_staging.country_capital_ext (
    country string,
    capital string
)
comment 'capital table - for data in JSON file capital.json'
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile
location '/akrasnova/country_capital';

create table if not exists akrasnova_staging.country_capital stored as orc as select * from akrasnova_staging.country_capital_ext;

drop table akrasnova_staging.country_capital_ext;