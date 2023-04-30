-- NOBEL PRIZES - DATA WAREHOUSE

-- данные обновляются еженедельно, предположим, что данных много
-- берется дельта значений, т.е. только новые значения
-- партиции по дате загрузки и категории Премии, кластеризация по имени лауреата, разделим на 16 бакетов.
-- данные уже существуют в созданной ранее таблице akrasnovadb.nobel_prizes, 
-- переложим их в таблицу в новой db akrasnova_wrh.nobel_prizes с партициями и бакетами

-- создадим временную managed таблицу нужной структуры
create table akrasnova_tmp.nobel_prizes_wrh (
	nobel_prize_id int,
	year int,
	laureate_id int,
	laureate_type_id int,
	prize_type_id int,
	prize_share varchar(10),
	motivation varchar(255),
	category_id int,
	upd_date date -- новое поле, дата загрузки
);

-- insert из таблицы, созданной в процессе преобразования Spark, во временную таблицу
insert overwrite table akrasnova_tmp.nobel_prizes_wrh select * from 
(
	(select 
		nobel_prize_id, year, laureate_id, laureate_type_id, prize_type_id, prize_share, motivation, category_id 
	from akrasnovadb.nobel_prizes), 
	current_date
);

-- создаем managed таблицу
create table akrasnova_wrh.nobel_prizes (
	nobel_prize_id int,
	year int,
	laureate_id int,
	laureate_type_id int,
	prize_type_id int,
	prize_share varchar(10),
	motivation varchar(255)
)
partitioned by (
	category_id int,
	upd_date date
)
clustered by (laureate_id) into 16 buckets
stored as orc;

-- указываем, что партиционирование динамическое
set hive.exec.dynamic.partition.mode=nonstrict
-- инсертим данные из временной таблицы
insert into akrasnova_wrh.nobel_prizes partition (category_id,upd_date) select * from akrasnova_tmp.nobel_prizes_wrh;