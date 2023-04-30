-- NOBEL PRIZES - DATA LAKE

-- данные обновляются еженедельно, предположим, что данных много
-- берется дельта значений, т.е. только новые значения
-- партиции по дате загрузки и категории Премии, кластеризация по имени лауреата, разделим на 16 бакетов.
-- данные лежат в директорию '/staging/nobel_laureates'

create external table if not exists akrasnova_staging.nobel_laureates_ext (
    year int,
    category varchar(20),
    prize varchar(50),
    motivation varchar(255),
    prize_share varchar(10),
    laureate_id int,
    laureate_type varchar(20),
    full_name varchar(255),
    birth_date date,
    birth_city varchar(50),
    birth_country varchar(50),
    gender varchar(10),
    organization_name varchar(255),
    organization_city varchar(50),
    organization_country varchar(50),
    deathdate date,
    death_city varchar(50),
    death_country varchar(50)
)
comment 'Nobel Laureate table - for data in CSV file nobel.csv'
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\"")
stored as textfile
location '/staging/nobel_laureates'
tblproperties ('skip.header.line.count'='1');

-- создадим временную managed таблицу нужной структуры, в которую будут падать и перезаписываться новые данные
create table akrasnova_tmp.nobel_laureates_stg (
    year int,
    prize varchar(50),
    motivation varchar(255),
    prize_share varchar(10),
    laureate_id int,
    laureate_type varchar(20),
    full_name varchar(255),
    birth_date date,
    birth_city varchar(50),
    birth_country varchar(50),
    gender varchar(10),
    organization_name varchar(255),
    organization_city varchar(50),
    organization_country varchar(50),
    deathdate date,
    death_city varchar(50),
    death_country varchar(50),
	category varchar(20),
	upd_date date -- новое поле, дата загрузки
);

-- insert overwrite из внешней таблицы во временную
insert overwrite table akrasnova_tmp.nobel_laureates_stg select * from 
(
	(select 
		year, prize, motivation, prize_share, laureate_id, laureate_type, full_name, birth_date, birth_city, birth_country,
		gender, organization_name, organization_city, organization_country, deathdate, death_city, death_country, category
	from akrasnova_staging.nobel_laureates_ext), 
	current_date
);

-- создаем managed таблицу
create table akrasnova_staging.nobel_laureates (
    year int,
    prize varchar(50),
    motivation varchar(255),
    prize_share varchar(10),
    laureate_id int,
    laureate_type varchar(20),
    full_name varchar(255),
    birth_date date,
    birth_city varchar(50),
    birth_country varchar(50),
    gender varchar(10),
    organization_name varchar(255),
    organization_city varchar(50),
    organization_country varchar(50),
    deathdate date,
    death_city varchar(50),
    death_country varchar(50)
)
partitioned by (
	category varchar(20),
	upd_date date, -- новое поле, дата загрузки
)
clustered by (laureate_id) into 16 buckets
stored as orc;

-- указываем, что партиционирование динамическое
set hive.exec.dynamic.partition.mode=nonstrict
-- инсертим данные из временной таблицы
insert into akrasnova_staging.nobel_laureates partition (category,upd_date) select * from akrasnova_tmp.nobel_laureates_stg;

-- удаляем внешнюю таблицу
drop table akrasnova_staging.nobel_laureates_ext;