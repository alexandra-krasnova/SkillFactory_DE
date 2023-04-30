-- COUNTRIES - DATA WAREHOUSE

-- данные обновляются ежеквартально, каждую новую версию данных сложим в отдельную партицию и разделим данные на бакеты по названию страны
-- дельта значений отсутствует, но новые страны должны загружаться крайне редко, в основном информация по странам обновляется
-- партиции по дате загрузки, кластеризации нет, т.к. количество стран небольшое
-- данные уже существуют в созданной ранее таблице akrasnovadb.countries, 
-- переложим их в таблицу в новой db akrasnova_wrh.countries с партициями

-- создадим временную managed таблицу нужной структуры
create table akrasnova_tmp.countries_wrh (
	country_id int,
	country_name varchar(50),
	region_id int,
	continent_id int,
	parent_country_id int,
	capital_name varchar(50),
	population int,
	area_sq_miles int,
	pop_dencity_per_sq_mile decimal(8,2),
	coastline decimal(7,3),
	net_migration decimal(6,3),
	infant_mortality_per_1000 decimal(7,3),
	gdb_dollar_per_capita int,
	percent_literacy decimal(6,3),
	phones_per_1000 decimal(7,3),
	percent_arable decimal(6,3),
	percent_crops decimal(6,3),
	percent_other decimal(6,3),
	climate decimal(4,2),
	birthrate decimal(6,3),
	deathrate decimal(6,3),
	agriculture decimal(5,4),
	industry decimal(5,4),
	service decimal(5,4),
	upd_date date -- новое поле, дата загрузки
);

-- insert из таблицы, созданной в процессе преобразования Spark, во временную таблицу
insert overwrite table akrasnova_tmp.countries_wrh select * from 
(
	(select * from akrasnovadb.countries), 
	current_date
);

-- создаем managed таблицу
create table akrasnova_wrh.countries (
	country_id int,
	country_name varchar(50),
	region_id int,
	continent_id int,
	parent_country_id int,
	capital_name varchar(50),
	population int,
	area_sq_miles int,
	pop_dencity_per_sq_mile decimal(8,2),
	coastline decimal(7,3),
	net_migration decimal(6,3),
	infant_mortality_per_1000 decimal(7,3),
	gdb_dollar_per_capita int,
	percent_literacy decimal(6,3),
	phones_per_1000 decimal(7,3),
	percent_arable decimal(6,3),
	percent_crops decimal(6,3),
	percent_other decimal(6,3),
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
insert into akrasnova_wrh.countries partition (upd_date) select * from akrasnova_tmp.countries_wrh;