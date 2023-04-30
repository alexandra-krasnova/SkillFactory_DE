-- PERSONS_IN_ORGS - DATA WAREHOUSE

-- данные по вручению Премии обновляются еженедельно, предположим, что данных много 
-- => лауреатов в организациях состоит также много
-- берется дельта значений, т.е. только новые значения
-- партиции по дате загрузки, кластеризация по имени лауреата, разделим на 16 бакетов.
-- данные уже существуют в созданной ранее таблице akrasnovadb.persons_in_orgs, 
-- переложим их в таблицу в новой db akrasnova_wrh.persons_in_orgs с партициями и бакетами

-- создадим временную managed таблицу нужной структуры
create table akrasnova_tmp.persons_in_orgs_wrh (
	per_org_id int,
	laureate_id int,
	organization_id int,
	upd_date date -- новое поле, дата загрузки
);

-- insert из таблицы, созданной в процессе преобразования Spark, во временную таблицу
insert overwrite table akrasnova_tmp.persons_in_orgs_wrh select * from 
(
	(select * from akrasnovadb.persons_in_orgs), 
	current_date
);

-- создаем managed таблицу
create table akrasnova_wrh.persons_in_orgs (
	per_org_id int,
	laureate_id int,
	organization_id int
)
partitioned by (
	upd_date date
)
clustered by (laureate_id) into 16 buckets
stored as orc;

-- указываем, что партиционирование динамическое
set hive.exec.dynamic.partition.mode=nonstrict
-- инсертим данные из временной таблицы
insert into akrasnova_wrh.persons_in_orgs partition (upd_date) select * from akrasnova_tmp.persons_in_orgs_wrh;