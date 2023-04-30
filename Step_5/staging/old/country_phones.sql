create external table db_staging.country_phones_ext (
    country string,
    phone string
)
comment 'phone table - for data in JSON file phone.json'
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile
location '/staging/country_phones';

create table db_staging.country_phones stored as orc as select * from db_staging.country_phones_ext;

drop table db_staging.country_phones_ext;