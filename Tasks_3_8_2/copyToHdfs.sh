#!/bin/bash

# copyToHdfs.sh
fileName=$1
hdfsDir=$2
hdfsName=$3

echo "копирует файл в HDFS \"таблицу\" или удаляет директорию, перенаправляет протоколы, проверяет результат"

if [ ! -z "$hdfsName" ]; then
    # если имя файла есть - копируем
    echo "Copying $hdfsName into HDFS..."
	hdfs dfs -mkdir -p $hdfsDir >ak_hdfs_stdout.txt 2>ak_hdfs_stderr.txt
else
	# если имя файла пустое, удаляем директорию
	echo "Deleting $hdfsDir from HDFS..."
	hdfs dfs -rm -r -f -skipTrash $hdfsDir >ak_hdfs_stdout.txt 2>ak_hdfs_stderr.txt
fi

# проверка, что копирование или удаление выполнилось без ошибок, выход, если есть ошибки
if [ ! $? -eq 0 ]; then
	echo "There were errors while working with dir, check ak_hdfs_stderr.txt"
	exit 1
fi

# для удаления директории - все сделали, возвращаемся
if [ -z "$hdfsName" ]; then
	echo "Done"
	exit 1

# копирование
hdfs dfs -put -f $fileName "$hdfsDir/$hdfsName" >ak_hdfs_stdout.txt 2>ak_hdfs_stderr.txt

if [ $? -eq 0 ]; then
	echo "Done"
else
	echo "There were errors while copying, check ak_hdfs_stderr.txt"
fi