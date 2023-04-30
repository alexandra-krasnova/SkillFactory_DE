-- COUNTRIES - DATA LAKE

-- данные обновляются ежеквартально, каждую новую версию данных сложим в отдельную партицию и разделим данные на бакеты по названию страны
-- партиции по дате загрузки, кластеризации нет, т.к. количество стран небольшое
-- дельта значений отсутствует, но новые страны должны загружаться крайне редко, в основном информация по странам обновляется
-- данные лежат в директорию '/staging/countries_of_the_world'

-- создаем external таблицу для новых данных
create external table if not exists akrasnova_staging.countries_of_the_world_ext (
    country varchar(50),
    region varchar(50),
    population int,
    area_sq_mi int,
    pop_density_per_sq_mi decimal(8,2),
    coastline decimal(7,3),
    net_migration decimal(6,3),
    infant_mortality_per_1000_births decimal(7,3),
    gdp_dol_per_capita int,
    literacy_percent decimal(6,3),
    phones_per_1000 decimal(7,3),
    arable_percent decimal(6,3),
    crops_percent decimal(6,3),
    other_percent decimal(6,3),
    climate decimal(4,2),
    birthrate decimal(6,3),
    deathrate decimal(6,3),
    agriculture decimal(5,4),
    industry decimal(5,4),
    service decimal(5,4)
)
comment 'Countries table - for data in CSV file countries_of_the_world.csv'
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\"")
stored as textfile
location '/staging/countries_of_the_world'
tblproperties ('skip.header.line.count'='1');

-- создадим временную managed таблицу нужной структуры, в которую будут падать и перезаписываться новые данные
create table akrasnova_tmp.countries_stg (
    country varchar(50),
    region varchar(50),
    population int,
    area_sq_mi int,
    pop_density_per_sq_mi decimal(8,2),
    coastline decimal(7,3),
    net_migration decimal(6,3),
    infant_mortality_per_1000_births decimal(7,3),
    gdp_dol_per_capita int,
    literacy_percent decimal(6,3),
    phones_per_1000 decimal(7,3),
    arable_percent decimal(6,3),
    crops_percent decimal(6,3),
    other_percent decimal(6,3),
    climate decimal(4,2),
    birthrate decimal(6,3),
    deathrate decimal(6,3),
    agriculture decimal(5,4),
    industry decimal(5,4),
    service decimal(5,4),
	upd_date date -- новое поле, дата загрузки
);

-- insert overwrite данных из внешней таблицы во временную
insert overwrite table akrasnova_tmp.countries_stg select * from 
(
	(select * from akrasnova_staging.countries_of_the_world_ext), 
	current_date
);

-- создаем managed таблицу
create table akrasnova_staging.countries_of_the_world (
    country varchar(50),
    region varchar(50),
    population int,
    area_sq_mi int,
    pop_density_per_sq_mi decimal(8,2),
    coastline decimal(7,3),
    net_migration decimal(6,3),
    infant_mortality_per_1000_births decimal(7,3),
    gdp_dol_per_capita int,
    literacy_percent decimal(6,3),
    phones_per_1000 decimal(7,3),
    arable_percent decimal(6,3),
    crops_percent decimal(6,3),
    other_percent decimal(6,3),
    climate decimal(4,2),
    birthrate decimal(6,3),
    deathrate decimal(6,3),
    agriculture decimal(5,4),
    industry decimal(5,4),
    service decimal(5,4)
)
partitioned by (
	upd_date date
)
stored as orc;

-- указываем, что партиционирование динамическое
set hive.exec.dynamic.partition.mode=nonstrict
-- инсертим данные из временной таблицы
insert into akrasnova_staging.countries_of_the_world partition (upd_date) select * from akrasnova_tmp.countries_stg;

-- удаляем внешнюю таблицу
drop table akrasnova_staging.countries_of_the_world_ext;

