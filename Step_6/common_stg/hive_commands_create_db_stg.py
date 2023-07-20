# create a database in hive
CREATE_DB_CMD_STG = """
/usr/bin/beeline -u jdbc:hive2://localhost:10000/default -n hive -p 123 <<END_SQL
drop database if exists akrasnova_staging cascade;
create database akrasnova_staging;
drop database if exists akrasnova_tmp cascade;
create database akrasnova_tmp;
END_SQL
"""

