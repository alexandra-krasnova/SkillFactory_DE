-- ORGANIZATIONS - DATA WAREHOUSE

-- данные по вручению Премии обновляются еженедельно, предположим, что данных много 
-- предположим, что организаций, в которых работают лауреаты, гораздо меньше, чем самих лауреатов
-- берется дельта значений, т.е. только новые значения
-- партиции по дате загрузки, кластеризация по названию организации, разделим на 4 бакета.
-- данные уже существуют в созданной ранее таблице akrasnovadb.organizations, 
-- переложим их в таблицу в новой db akrasnova_wrh.organizations с партициями и бакетами

-- создадим временную managed таблицу нужной структуры
create table akrasnova_tmp.organizations_wrh (
	organization_id int,
	organization_name varchar(255),
	organization_country_id int,
	organization_city_id int,
	parent_organization_id int,
	upd_date date -- новое поле, дата загрузки
);

-- insert из таблицы, созданной в процессе преобразования Spark, во временную таблицу
insert overwrite table akrasnova_tmp.organizations_wrh select * from 
(
	(select * from akrasnovadb.organizations), 
	current_date
);

-- создаем managed таблицу
create table akrasnova_wrh.organizations (
	organization_id int,
	organization_name varchar(255),
	organization_country_id int,
	organization_city_id int,
	parent_organization_id int
)
partitioned by (
	upd_date date
)
clustered by (organization_id) into 4 buckets
stored as orc;

-- указываем, что партиционирование динамическое
set hive.exec.dynamic.partition.mode=nonstrict
-- инсертим данные из временной таблицы
insert into akrasnova_wrh.organizations partition (upd_date) select * from akrasnova_tmp.organizations_wrh;