# CITIES - staging
# приходит полный список городов, дельта значений отсутствует

# sqoop data from mysql to hive
SQOOP_CITIES_STG = """
hdfs dfs -rm -r -skipTrash /tmp/{0} >/dev/null 2>&1
sqoop import --connect jdbc:mysql://10.93.1.9/skillfactory --username mysql --password arenadata \
--table skillfactory.cities --hive-import --hive-table akrasnova_staging.cities_mysql \
--hs2-url jdbc:hive2://localhost:10000
exit $?
"""

# update Cities table
LOAD_CITIES_STG = """
/usr/bin/beeline -u jdbc:hive2://localhost:10000/default -n hive -p 123 <<END_SQL
-- создадим временную managed таблицу нужной структуры, в которую будут падать и накапливаться новые данные
-- таблица накапливает данные до преобразования в таблицы Data Warehouse, затем таблица дропается в графе Airflow
create table if not exists akrasnova_tmp.cities_delta (
    city string,
    accentcity string,
    region int,
    population int,
    latitude decimal(20,17),
    longitude decimal(20,17),
	country string,
	upd_date date -- новое поле, дата загрузки
);

-- insert из sqoop'нутой таблицы во временную
insert into table akrasnova_tmp.cities_delta select * from 
(
	(select city, accentcity, region, population, latitude, longitude, country from akrasnova_staging.cities_mysql), 
	current_date
);

-- создаем managed таблицу
create table if not exists akrasnova_staging.cities (
    city string,
    accentcity string,
    region int,
    population int,
    latitude decimal(20,17),
    longitude decimal(20,17)
)
partitioned by (
	country string,
	upd_date date
)
clustered by (region,accentcity) into 16 buckets
stored as orc;

-- указываем, что партиционирование динамическое
set hive.exec.dynamic.partition.mode=nonstrict
-- инсертим данные из временной таблицы
insert into akrasnova_staging.cities partition (country,upd_date) select * from akrasnova_tmp.cities_delta;

-- удаляем sqoop'нутую таблицу
drop table akrasnova_staging.cities_mysql;
END_SQL
"""


