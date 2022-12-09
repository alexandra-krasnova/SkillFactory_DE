-- Таблица для данных по странам, данные хранятся в файле /home/deng/Data/countries_of_the_world.csv
create /*external*/ table if not exists melaer_staging.countries (
    country string,
    region string,
    population int,
    area_sq_mi int,
    pop_density_per_sq_mi float,
    coastline float,
    net_migration float,
    infant_mortality_per_1000_births float,
    gdp_dol_per_capita int,
    literacy_percent float,
    phones_per_1000 float,
    arable_percent float,
    crops_percent float,
    other_percent float,
    climate float,
    birthrate float,
    deathrate float,
    agriculture float,
    industry float,
    service float
)
comment 'Countries table - for data in CSV file countries_of_the_world.csv'