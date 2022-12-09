-- Таблица для данных по нобелевским лауреатам, данные хранятся в файле /home/deng/Data/nobel-laureates.csv
create /*external*/ table if not exists melaer_staging.nobel_laureates (
    year int,
    category string,
    prize string,
    motivation string,
    prize_share string, -- вероятно, можно поставить тип float
    laureate_id int,
    laureate_type string,
    full_name string,
    birth_date string, -- вероятно, можно поставить тип timestamp
    birth_city string,
    birth_country string,
    sex string,
    organization_name string,
    organization_city string,
    organization
	country string,
    deathdate string, -- вероятно, можно поставить тип timestamp
    death_city string,
    death_country string
)
comment 'Nobel Laureate table - for data in CSV file nobel-laureates.csv'