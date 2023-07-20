import sys
sys.path.append('/usr/lib/spark/python')
sys.path.append('/usr/lib/spark/python/lib/py4j-0.10.7-src.zip')

import datetime
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator

from common.hive_commands import CREATE_DB_CMD, DROP_DELTA_TABLES
from common.preprocessing import createTmpCountriesTbl, createTmpLaureatesTbl, createTmpCitiesTbl, \
    CREATE_TMP_HIST_COUNTRIES_TBL, CREATE_TMP_MIS_CITIES_TBL, CREATE_TMP_MIS_ORGS_TBL
from common.load_reference import loadFromNobelDf, loadRegions, loadContinents, loadCurrencyCodes
from common.load_countries_cities import loadCountries, loadCountryCodes, loadCodesInCountries, loadCities
from common.load_laureates_orgs import loadLaureates, loadOrganizations, loadPersonsInOrgs
from common.load_nobel_prizes import loadNobelPrizes
from common.load_dataset import loadDataset

# Данный граф представляет собой первичную загрузку данных из Staging в DWH, загрузка будет осуществляться во все таблицы DWH

# DAG settings
DAG_NAME = "create_datawarehouse_sf"
schedule = None
startDate = datetime.datetime(2023, 07, 10)

# main DAG
dag = DAG( dag_id=DAG_NAME, schedule_interval=schedule, start_date=startDate, catchup=False )

# ----------- Main DAG - description of operators start ----------

# start
start = EmptyOperator(task_id="start")
createDB = BashOperator(task_id='createDB', bash_command=CREATE_DB_CMD, dag=dag)

point1 = EmptyOperator(task_id="point1")

# preprocessing
createTmpCountriesTbl = PythonOperator(task_id='createTmpCountriesTbl', python_callable=createTmpCountriesTbl, op_args=[False], dag=dag)
createTmpHistCountriesTbl = BashOperator(task_id='createTmpHistCountriesTbl', bash_command=CREATE_TMP_HIST_COUNTRIES_TBL, dag=dag)
createTmpLaureatesTbl = PythonOperator(task_id='createTmpLaureatesTbl', python_callable=createTmpLaureatesTbl, dag=dag)
createTmpCitiesTbl = PythonOperator(task_id='createTmpCitiesTbl', python_callable=createTmpCitiesTbl, dag=dag)
createTmpMisCitiesTbl = BashOperator(task_id='createTmpMisCitiesTbl', bash_command=CREATE_TMP_MIS_CITIES_TBL, dag=dag)
createTmpMisOrgsTbl = BashOperator(task_id='createTmpMisOrgsTbl', bash_command=CREATE_TMP_MIS_ORGS_TBL, dag=dag)

point2 = EmptyOperator(task_id="point2")

# create simple reference tables
loadFromNobelDf = PythonOperator(task_id='loadFromNobelDf', python_callable=loadFromNobelDf, op_args=[False], dag=dag)
loadRegions = PythonOperator(task_id='loadRegions', python_callable=loadRegions, dag=dag)
loadContinents = PythonOperator(task_id='loadContinents', python_callable=loadContinents, dag=dag)
loadCurrencyCodes = PythonOperator(task_id='loadCurrencyCodes', python_callable=loadCurrencyCodes, dag=dag)

point3 = EmptyOperator(task_id="point3")

# create tables with Country and City information
loadCountries = PythonOperator(task_id='loadCountries', python_callable=loadCountries, op_args=[False], dag=dag)
loadCountryCodes = PythonOperator(task_id='loadCountryCodes', python_callable=loadCountryCodes, dag=dag)
loadCodesInCountries = PythonOperator(task_id='loadCodesInCountries', python_callable=loadCodesInCountries, op_args=[False], dag=dag)
loadCities = PythonOperator(task_id='loadCities', python_callable=loadCities, dag=dag)

point4 = EmptyOperator(task_id="point4")

# create tables with Laureates and Organizations information
loadLaureates = PythonOperator(task_id='loadLaureates', python_callable=loadLaureates, op_args=[False], dag=dag)
loadOrganizations = PythonOperator(task_id='loadOrganizations', python_callable=loadOrganizations, op_args=[False], dag=dag)
loadPersonsInOrgs = PythonOperator(task_id='loadPersonsInOrgs', python_callable=loadPersonsInOrgs, op_args=[False], dag=dag)

point5 = EmptyOperator(task_id="point5")

# create table with Nobel Prizes
loadNobelPrizes = PythonOperator(task_id='loadNobelPrizes', python_callable=loadNobelPrizes, dag=dag)

point6 = EmptyOperator(task_id="point6")

# create table Dataset
loadDataset = PythonOperator(task_id='loadDataset', python_callable=loadDataset, op_args=[False], dag=dag)

point7 = EmptyOperator(task_id="point7")

# drop Staging Delta tables
dropDeltaTables = BashOperator(task_id='createTmpMisCitiesTbl', bash_command=DROP_DELTA_TABLES, dag=dag)

# end
end = EmptyOperator(task_id="end")

# ------------Main DAG - description of operators end ----------

# ----------- Main DAG - sequence of operators start ----------

# start
start >> createDB >> point1
# preprocessing
point1 >> createTmpCountriesTbl >> createTmpHistCountriesTbl >> createTmpLaureatesTbl >> createTmpCitiesTbl \
    >> createTmpMisCitiesTbl >> createTmpMisOrgsTbl >> point2
# create simple reference tables
point2 >> loadFromNobelDf >> loadRegions >> loadContinents >> loadCurrencyCodes >> point3
# create tables with Country and City information
point3 >> loadCountries >> loadCountryCodes >> loadCodesInCountries >> loadCities >> point4
# create tables with Laureates and Organizations information
point4 >> loadLaureates >> loadOrganizations >> loadPersonsInOrgs >> point5
# create table with Nobel Prizes
point5 >> loadNobelPrizes >> point6
# create table Dataset
point6 >> loadDataset >> point7
# drop Staging Delta tables
point7 >> dropDeltaTables >> end

# ----------- Main DAG - sequence of operators end ----------
