# COUNTRIES - staging
# приходит полный список городов, дельта значений отсутствует
# данные лежат в директории '/akrasnova/countries_of_the_world'

# create Countries table
LOAD_COUNTRIES_STG = """
/usr/bin/beeline -u jdbc:hive2://localhost:10000/default -n hive -p 123 <<END_SQL
-- создаем external таблицу для новых данных
create external table akrasnova_staging.countries_of_the_world_ext (
    country varchar(50),
    region varchar(50),
    population int,
    area_sq_mi int,
    pop_density_per_sq_mi decimal(8,2),
    coastline decimal(7,3),
    net_migration decimal(6,3),
    infant_mortality_per_1000_births decimal(7,3),
    gdp_dol_per_capita int,
    literacy_percent decimal(6,3),
    phones_per_1000 decimal(7,3),
    arable_percent decimal(6,3),
    crops_percent decimal(6,3),
    other_percent decimal(6,3),
    climate decimal(4,2),
    birthrate decimal(6,3),
    deathrate decimal(6,3),
    agriculture decimal(5,4),
    industry decimal(5,4),
    service decimal(5,4)
)
comment 'Countries table - for data in CSV file countries_of_the_world.csv'
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\"")
stored as textfile
location '/akrasnova/countries_of_the_world'
tblproperties ('skip.header.line.count'='1');

-- создадим временную managed таблицу нужной структуры, в которую будут падать и накапливаться новые данные
-- таблица накапливает данные до преобразования в таблицы Data Warehouse, затем таблица дропается в графе Airflow
create table if not exists akrasnova_tmp.countries_delta (
    country varchar(50),
    region varchar(50),
    population int,
    area_sq_mi int,
    pop_density_per_sq_mi decimal(8,2),
    coastline decimal(7,3),
    net_migration decimal(6,3),
    infant_mortality_per_1000_births decimal(7,3),
    gdp_dol_per_capita int,
    literacy_percent decimal(6,3),
    phones_per_1000 decimal(7,3),
    arable_percent decimal(6,3),
    crops_percent decimal(6,3),
    other_percent decimal(6,3),
    climate decimal(4,2),
    birthrate decimal(6,3),
    deathrate decimal(6,3),
    agriculture decimal(5,4),
    industry decimal(5,4),
    service decimal(5,4),
	upd_date date -- новое поле, дата загрузки
);

-- insert данных из внешней таблицы во временную
insert into table akrasnova_tmp.countries_delta select * from 
(
	(select * from akrasnova_staging.countries_of_the_world_ext), 
	current_date
);

-- создаем managed таблицу
create table if not exists akrasnova_staging.countries_of_the_world (
    country varchar(50),
    region varchar(50),
    population int,
    area_sq_mi int,
    pop_density_per_sq_mi decimal(8,2),
    coastline decimal(7,3),
    net_migration decimal(6,3),
    infant_mortality_per_1000_births decimal(7,3),
    gdp_dol_per_capita int,
    literacy_percent decimal(6,3),
    phones_per_1000 decimal(7,3),
    arable_percent decimal(6,3),
    crops_percent decimal(6,3),
    other_percent decimal(6,3),
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
insert into akrasnova_staging.countries_of_the_world partition (upd_date) select * from akrasnova_tmp.countries_delta;

-- удаляем внешнюю таблицу
drop table akrasnova_staging.countries_of_the_world_ext;
END_SQL
"""