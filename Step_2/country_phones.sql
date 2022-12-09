-- Таблица для данных по телефонным кодам стран, данные хранятся в файле /home/deng/Data/phone.json
create /*external*/ table if not exists melaer_staging.country_phones (
    country_code string,
    country_phone_code string
)
comment 'Phone table - for data in JSON file phone.json'