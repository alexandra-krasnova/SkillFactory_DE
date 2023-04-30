create external table db_staging.country_currencies_ext (
    country string,
    currency string
)
comment 'currency table - for data in JSON file currency.json'
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile
location '/staging/country_currencies';

create table db_staging.country_currencies stored as orc as select * from db_staging.country_currencies_ext;

drop table db_staging.country_currencies_ext;