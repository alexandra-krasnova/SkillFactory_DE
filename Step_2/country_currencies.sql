-- Таблица для данных по валютам стран, данные хранятся в файле /home/deng/Data/currency.json
create /*external*/ table if not exists melaer_staging.country_currencies (
    country_code string,
    currency_code string
)
comment 'Currency table - for data in JSON file currency.json'