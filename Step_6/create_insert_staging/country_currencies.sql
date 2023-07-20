create external table akrasnova_staging.country_currencies_ext (
    country string,
    currency string
)
comment 'currency table - for data in JSON file currency.json'
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile
location '/akrasnova/country_currencies';

create table if not exists akrasnova_staging.country_currencies stored as orc as select * from akrasnova_staging.country_currencies_ext;

drop table akrasnova_staging.country_currencies_ext;