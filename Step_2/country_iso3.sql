-- Таблица для данных по кодам iso3, данные хранятся в файле /home/deng/Data/iso3.json
create /*external*/ table if not exists melaer_staging.country_iso3 (
    country_code string,
    iso3_code string
)
comment 'ISO3 table - for data in JSON file iso3.json'