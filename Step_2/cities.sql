-- Таблица для данных по городам, данные хранятся в MySQL таблице skillfactory.cities
create table if not exists melaer_staging.sities (
    country string,
    city string,
    accentcity string,
    region int,
    population int,
    latitude decimal(20,17),
    longitude decimal(20,17)
)
comment 'City table - for data in MySQL table skillfactory.cities'