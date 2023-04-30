-- PRIZE_TYPES - DATA WAREHOUSE

-- таблица Prize Types содержит справочные значения типов Премий, но, судя по данным, значения в ней содержат год вручения Премии
-- => известно, что таблица будет обновляться минимум раз в год.
-- берется дельта значений, т.е. только новые значения
-- настроим партиции по дате загрузки без кластеризации, т.к. много новых значений не ожидается
-- данные уже существуют в созданной ранее таблице akrasnovadb.prize_types, 
-- переложим их в таблицу в новой db akrasnova_wrh.prize_types с партициями и бакетами

-- создадим временную managed таблицу нужной структуры
create table akrasnova_tmp.prize_types_wrh (
	prize_type_id int,
	prize_type varchar(50),
	upd_date date -- новое поле, дата загрузки
);

-- insert из таблицы, созданной в процессе преобразования Spark, во временную таблицу
insert overwrite table akrasnova_tmp.prize_types_wrh select * from 
(
	(select * from akrasnovadb.prize_types), 
	current_date
);

-- создаем managed таблицу
create table akrasnova_wrh.prize_types (
	prize_type_id int,
	prize_type varchar(50)
)
partitioned by (
	upd_date date
)
stored as orc;

-- указываем, что партиционирование динамическое
set hive.exec.dynamic.partition.mode=nonstrict
-- инсертим данные из временной таблицы
insert into akrasnova_wrh.prize_types partition (upd_date) select * from akrasnova_tmp.prize_types_wrh;