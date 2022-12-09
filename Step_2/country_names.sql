-- Таблица для данных по именам стран, данные хранятся в файле /home/deng/Data/names.json
create /*external*/ table if not exists melaer_staging.country_names (
    country_code string,
    country_name string
)
comment 'Names table - for data in JSON file names.json'