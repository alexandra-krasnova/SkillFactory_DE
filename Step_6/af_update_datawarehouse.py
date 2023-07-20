import sys
sys.path.append('/usr/lib/spark/python')
sys.path.append('/usr/lib/spark/python/lib/py4j-0.10.7-src.zip')

import datetime
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator

from dwh_common.preprocessing import createTmpCountriesTbl, createTmpLaureatesTbl, createTmpCitiesTbl
from dwh_common.load_countries_cities import loadCountries, loadCodesInCountries, loadCities
from dwh_common.load_reference import loadFromNobelDf
from dwh_common.load_laureates_orgs import loadLaureates, loadOrganizations, loadPersonsInOrgs
from dwh_common.load_nobel_prizes import loadNobelPrizes
from dwh_common.load_dataset import loadDataset
from dwh_common.hive_commands import DROP_DELTA_TABLES


# Данный граф представляет собой загрузку обновленных данных из Staging в DWH, загрузка будет происходить не вот все таблицы DWH

# DAG settings
DAG_NAME = "update_datawarehouse_sf"
schedule = None
startDate = datetime.datetime(2023, 07, 10)

# main DAG
dag = DAG( dag_id=DAG_NAME, schedule_interval=schedule, start_date=startDate, catchup=False )

# ----------- Main DAG - description of operators start ----------

# start
start = EmptyOperator(task_id="start")

# preprocessing
createTmpCountriesTbl = PythonOperator(task_id='createTmpCountriesTbl', python_callable=createTmpCountriesTbl, op_args=[True], dag=dag)
createTmpLaureatesTbl = PythonOperator(task_id='createTmpLaureatesTbl', python_callable=createTmpLaureatesTbl, dag=dag)
createTmpCitiesTbl = PythonOperator(task_id='createTmpCitiesTbl', python_callable=createTmpCitiesTbl, dag=dag)

point1 = EmptyOperator(task_id="point1")

# update cities and countries tables
loadCountries = PythonOperator(task_id='loadCountries', python_callable=loadCountries, op_args=[True], dag=dag)
loadCodesInCountries = PythonOperator(task_id='loadCountries', python_callable=loadCountries, op_args=[True], dag=dag)
loadCities = PythonOperator(task_id='loadCities', python_callable=loadCities, op_args=[True], dag=dag)

point2 = EmptyOperator(task_id="point2")

# create tables with Laureates and Organizations information
loadPrizeTypes = PythonOperator(task_id='loadFromNobelDf', python_callable=loadFromNobelDf, op_args=[True], dag=dag)
loadLaureates = PythonOperator(task_id='loadLaureates', python_callable=loadLaureates, op_args=[True], dag=dag)
loadOrganizations = PythonOperator(task_id='loadOrganizations', python_callable=loadOrganizations, op_args=[True], dag=dag)
loadPersonsInOrgs = PythonOperator(task_id='loadPersonsInOrgs', python_callable=loadPersonsInOrgs, op_args=[True], dag=dag)

point3 = EmptyOperator(task_id="point3")

# update table with Nobel Prizes
loadNobelPrizes = PythonOperator(task_id='loadNobelPrizes', python_callable=loadNobelPrizes, dag=dag)

point4 = EmptyOperator(task_id="point4")

# update table Dataset
loadDataset = PythonOperator(task_id='loadDataset', python_callable=loadDataset, op_args=[True], dag=dag)

point5 = EmptyOperator(task_id="point5")

# drop Staging Delta tables
dropDeltaTables = BashOperator(task_id='createTmpMisCitiesTbl', bash_command=DROP_DELTA_TABLES, dag=dag)

# end
end = EmptyOperator(task_id="end")

# ------------Main DAG - description of operators end ----------

# ----------- Main DAG - sequence of operators start ----------

# start, preprocessing
start >> createTmpCountriesTbl >> createTmpLaureatesTbl >> createTmpCitiesTbl >> point1
# update cities and countries tables
point1 >> loadCountries >> loadCodesInCountries >> loadCities >> point2
# update tables with Laureates and Organizations information
point2 >> loadPrizeTypes >> loadLaureates >> loadOrganizations >> loadPersonsInOrgs >> point3
# update table with Nobel Prizes
point3 >> loadNobelPrizes >> point4
# update table Dataset
point4 >> loadDataset >> point5
# drop Staging Delta tables
point5 >> dropDeltaTables >> end

# ----------- Main DAG - sequence of operators end ----------
