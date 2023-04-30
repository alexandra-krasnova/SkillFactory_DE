-- CITIES - DATA LAKE

-- предположим, что данные по городам меняюся довольно оперативно, например, указывается актуальное количество людей каждый месяц. 
-- следовательно, в каждую загрузку можно ожидать до нескольких миллионов записей.
-- дельта значений отсутствует, но новые города должны загружаться достаточно редко, в основном информация по городам обновляется
-- партиции по дате загрузки + стране, кластеризация по региону в стране + названию города, разделим на 16 бакетов.
-- данные лежат в директорию '/staging/cities'

-- создаем external таблицу для новых данных
create external table if not exists akrasnova_staging.cities_ext (
	country string,
    city string,
    accentcity string,
    region int,
    population int,
    latitude decimal(20,17),
    longitude decimal(20,17)
)
comment 'Cities table - for data in CSV file worldcitiespop.csv'
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\"")
stored as textfile
location '/staging/cities'
tblproperties ('skip.header.line.count'='1');

-- создадим временную managed таблицу нужной структуры, в которую будут падать и перезаписываться новые данные
create table akrasnova_tmp.cities_stg (
    city string,
    accentcity string,
    region int,
    population int,
    latitude decimal(20,17),
    longitude decimal(20,17),
	country string,
	upd_date date -- новое поле, дата загрузки
);

-- insert overwrite из внешней таблицы во временную
insert overwrite table akrasnova_tmp.cities_stg select * from 
(
	(select city, accentcity, region, population, latitude, longitude, country from akrasnova_staging.cities_ext), 
	current_date
);

-- создаем managed таблицу
create table akrasnova_staging.cities (
    city string,
    accentcity string,
    region int,
    population int,
    latitude decimal(20,17),
    longitude decimal(20,17)
)
partitioned by (
	country string,
	upd_date date
)
clustered by (region,accentcity) into 16 buckets
stored as orc;

-- указываем, что партиционирование динамическое
set hive.exec.dynamic.partition.mode=nonstrict
-- инсертим данные из временной таблицы
insert into akrasnova_staging.cities partition (country,upd_date) select * from akrasnova_tmp.cities_stg;

-- удаляем внешнюю таблицу
drop table akrasnova_staging.cities_ext;