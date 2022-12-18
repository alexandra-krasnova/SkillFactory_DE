#!/bin/bash

# execHiveCmd.sh
sql=$1

# выполянет команду Hive, перенаправляет вывод, проверяет результат
sql_file="./akrasnova/sql.txt"

echo "Executing Hive QL command: $sql ..."

echo -e "!connect jdbc:hive2://10.93.1.9:10000 hive eee;\n$sql;">$sql_file
beeline -f $sql_file >ak_hive_stdout.txt 2>ak_hive_stderr.txt

if [ $? -eq 0 ]; then
	echo "Done"
else
	echo "There were errors while hive-ing, check ak_hive_stderr.txt"
fi