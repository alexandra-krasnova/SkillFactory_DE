create external table akrasnova_staging.country_phones_ext (
    country string,
    phone string
)
comment 'phone table - for data in JSON file phone.json'
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile
location '/akrasnova/country_phones';

create table if not exists akrasnova_staging.country_phones stored as orc as select * from akrasnova_staging.country_phones_ext;

drop table akrasnova_staging.country_phones_ext;