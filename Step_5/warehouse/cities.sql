-- CITIES - DATA WAREHOUSE

-- предположим, что данные по городам меняюся довольно оперативно, например, указывается актуальное количество людей каждый месяц. 
-- следовательно, в каждую загрузку можно ожидать до нескольких миллионов записей.
-- дельта значений отсутствует, но новые города должны загружаться достаточно редко, в основном информация по городам обновляется
-- партиции по дате загрузки + стране, кластеризация по региону в стране + названию города, разделим на 16 бакетов.
-- данные уже существуют в созданной ранее таблице akrasnovadb.cities, 
-- переложим их в таблицу в новой db akrasnova_wrh.cities с партициями и бакетами

-- создадим временную managed таблицу нужной структуры
create table akrasnova_tmp.cities_wrh (
    city_id int,
	city_name string,
	accent_city_name string,
	city_region string,
	population int,
	latitude decimal(20,17),
    longitude decimal(20,17),
	country_id int,
	upd_date date -- новое поле, дата загрузки
);

-- insert из таблицы, созданной в процессе преобразования Spark, во временную таблицу
insert overwrite table akrasnova_tmp.cities_wrh select * from 
(
	(select city_id, city_name, accent_city_name, city_region, population, latitude, longitude, country_id from akrasnovadb.cities), 
	current_date);

-- создаем managed таблицу
create table akrasnova_wrh.cities (
    city_id int,
	city_name string,
	accent_city_name string,
	city_region string,
	population int,
	latitude decimal(20,17),
    longitude decimal(20,17)
)
partitioned by (
	country_id int,
	upd_date date
)
clustered by (city_region,city_id) into 16 buckets
stored as orc;

-- указываем, что партиционирование динамическое
set hive.exec.dynamic.partition.mode=nonstrict
-- инсертим данные из временной таблицы
insert into akrasnova_wrh.cities partition (country_id,upd_date) select * from akrasnova_tmp.cities_wrh;