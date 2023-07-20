# create a database in hive
CREATE_DB_CMD_DWH = """
/usr/bin/beeline -u jdbc:hive2://localhost:10000/default -n hive -p 123 <<END_SQL
drop database if exists akrasnova_wrh cascade;
create database akrasnova_wrh;
END_SQL
"""

# drop delta tables
DROP_DELTA_TABLES = """
/usr/bin/beeline -u jdbc:hive2://localhost:10000/default -n hive -p 123 <<END_SQL
drop table if exists akrasnova_tmp.countries_delta;
drop table if exists akrasnova_tmp.cities_delta;
drop table if exists akrasnova_tmp.nobel_laureates_delta;
END_SQL
"""