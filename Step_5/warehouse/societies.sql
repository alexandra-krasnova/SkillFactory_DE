-- SOCIETIES - DATA WAREHOUSE

-- данные по вручению Премии обновляются еженедельно, предположим, что данных много 
-- => лауреатов-организаций, скорей всего, много, но меньше, чем лауреатов-людей
-- берется дельта значений, т.е. только новые значения
-- партиции по дате загрузки, кластеризация по имени лауреата, разделим на 8 бакетов.
-- данные уже существуют в созданной ранее таблице akrasnovadb.societies, 
-- переложим их в таблицу в новой db akrasnova_wrh.societies с партициями и бакетами

-- создадим временную managed таблицу нужной структуры
create table akrasnova_tmp.societies_wrh (
	laureate_id int,
	society_name varchar(255),
	upd_date date -- новое поле, дата загрузки
);

-- insert из таблицы, созданной в процессе преобразования Spark, во временную таблицу
insert overwrite table akrasnova_tmp.societies_wrh select * from 
(
	(select * from akrasnovadb.societies), 
	current_date
);

-- создаем managed таблицу
create table akrasnova_wrh.societies (
	laureate_id int,
	society_name varchar(255)
)
partitioned by (
	upd_date date
)
clustered by (laureate_id) into 8 buckets
stored as orc;

-- указываем, что партиционирование динамическое
set hive.exec.dynamic.partition.mode=nonstrict
-- инсертим данные из временной таблицы
insert into akrasnova_wrh.societies partition (upd_date) select * from akrasnova_tmp.societies_wrh;