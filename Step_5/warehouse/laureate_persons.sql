-- LAUREATE_PERSONS - DATA WAREHOUSE

-- данные по вручению Премии обновляются еженедельно, предположим, что данных много 
-- => лауреатов-людей, скорей всего, также много, и их на гораздо больше, чем лауреатов-организаций
-- берется дельта значений, т.е. только новые значения
-- партиции по дате загрузки, кластеризация по имени лауреата, разделим на 32 бакета.
-- бакетов взято большее количество, чем для таблицы Премии, т.к. партиционирование идет только по дате загрузки - неясно, по какому еще полю можно разделить
-- данные уже существуют в созданной ранее таблице akrasnovadb.laureate_persons, 
-- переложим их в таблицу в новой db akrasnova_wrh.laureate_persons с партициями и бакетами

-- создадим временную managed таблицу нужной структуры
create table akrasnova_tmp.laureate_persons_wrh (
	laureate_id int,
	full_name varchar(255),
	gender_id int,
	birth_date date,
	birth_city_id int,
	birth_country_id int,
	death_date date,
	death_city_id int,
	death_country_id int,
	upd_date date -- новое поле, дата загрузки
);

-- insert из таблицы, созданной в процессе преобразования Spark, во временную таблицу
insert overwrite table akrasnova_tmp.laureate_persons_wrh select * from 
(
	(select 
		laureate_id, full_name, gender_id, birth_date, birth_city_id, birth_country_id, death_date, death_city_id, death_country_id
	from akrasnovadb.laureate_persons), 
	current_date
);

-- создаем managed таблицу
create table akrasnova_wrh.laureate_persons (
	laureate_id int,
	full_name varchar(255),
	gender_id int,
	birth_date date,
	birth_city_id int,
	birth_country_id int,
	death_date date,
	death_city_id int,
	death_country_id int
)
partitioned by (
	upd_date date
)
clustered by (laureate_id) into 32 buckets
stored as orc;

-- указываем, что партиционирование динамическое
set hive.exec.dynamic.partition.mode=nonstrict
-- инсертим данные из временной таблицы
insert into akrasnova_wrh.laureate_persons partition (upd_date) select * from akrasnova_tmp.laureate_persons_wrh;