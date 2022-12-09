-- Таблица для данных по континентам, на которых располагаются страны, данные хранятся в файле /home/deng/Data/continent.json
create /*external*/ table if not exists melaer_staging.country_continents (
    country_code string,
    continent_code string
)
comment 'Continent table - for data in JSON file continent.json'