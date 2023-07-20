import sys
sys.path.append('/usr/lib/spark/python')
sys.path.append('/usr/lib/spark/python/lib/py4j-0.10.7-src.zip')

import datetime
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator

from common_dwh.hive_commands_dwh import CREATE_DB_CMD_DWH, DROP_DELTA_TABLES
from common_dwh.preprocessing_dwh import createTmpCountriesTbl, createTmpLaureatesTbl, createTmpCitiesTbl, \
    CREATE_TMP_HIST_COUNTRIES_TBL, CREATE_TMP_MIS_CITIES_TBL, CREATE_TMP_MIS_ORGS_TBL
from common_dwh.load_reference_dwh import loadFromNobelDfDWH, loadRegionsDWH, loadContinentsDWH, loadCurrencyCodesDWH
from common_dwh.load_countries_cities_dwh import loadCountriesDWH, loadCountryCodesDWH, loadCodesInCntDWH, loadCitiesDWH
from common_dwh.load_laureates_orgs_dwh import loadLaureatesDWH, loadOrganizationsDWH, loadPersonsInOrgsDWH
from common_dwh.load_nobel_prizes_dwh import loadNobelPrizesDWH
from common_dwh.load_dataset_dwh import loadDatasetDWH

# Данный граф представляет собой загрузку данных из Staging в DWH, загрузка будет осуществляться во все таблицы DWH
# При первичной трансформации данные будут загружаться во все таблицы DWH, при обновлении данных - только в часть из них

# Mode: CREATE (первичная загрузка) или UPDATE (загрузка обновленных данных)
isUpdate = False if Upper(Variable.get(DAG_NAME_MODE)) != 'UPDATE' else True

# DAG settings
DAG_NAME = "create_datawarehouse_sf"
schedule = None
startDate = datetime.datetime(2023, 07, 10)

# main DAG
dag = DAG( dag_id=DAG_NAME, schedule_interval=schedule, start_date=startDate, catchup=False )

# ----------- Main DAG - description of operators start ----------

# start
start = EmptyOperator(task_id="start")

if not isUpdate:
    createDBDWH = BashOperator(task_id='createDB', bash_command=CREATE_DB_CMD_DWH, dag=dag)

point1 = EmptyOperator(task_id="point1")

# preprocessing
createTmpCntTbl = PythonOperator(task_id='createTmpCountriesTbl', python_callable=createTmpCountriesTbl, op_args=[isUpdate], dag=dag)
createTmpLaureatesTbl = PythonOperator(task_id='createTmpLaureatesTbl', python_callable=createTmpLaureatesTbl, dag=dag)
createTmpCitiesTbl = PythonOperator(task_id='createTmpCitiesTbl', python_callable=createTmpCitiesTbl, dag=dag)

point2 = EmptyOperator(task_id="point2")

if not isUpdate:
    createTmpHistCntTbl = BashOperator(task_id='createTmpHistCountriesTbl', bash_command=CREATE_TMP_HIST_COUNTRIES_TBL, dag=dag)
    createTmpMisCitiesTbl = BashOperator(task_id='createTmpMisCitiesTbl', bash_command=CREATE_TMP_MIS_CITIES_TBL, dag=dag)
    createTmpMisOrgsTbl = BashOperator(task_id='createTmpMisOrgsTbl', bash_command=CREATE_TMP_MIS_ORGS_TBL, dag=dag)

point3 = EmptyOperator(task_id="point3")

# create simple reference tables
loadFromNobelDfDWH = PythonOperator(task_id='loadFromNobelDf', python_callable=loadFromNobelDfDWH, op_args=[isUpdate], dag=dag)

if not isUpdate:
    loadRegionsDWH = PythonOperator(task_id='loadRegions', python_callable=loadRegionsDWH, dag=dag)
    loadContinentsDWH = PythonOperator(task_id='loadContinents', python_callable=loadContinentsDWH, dag=dag)
    loadCurrencyCodesDWH = PythonOperator(task_id='loadCurrencyCodes', python_callable=loadCurrencyCodesDWH, dag=dag)

point4 = EmptyOperator(task_id="point4")

# create tables with Country and City information
loadCountriesDWH = PythonOperator(task_id='loadCountries', python_callable=loadCountriesDWH, op_args=[isUpdate], dag=dag)
loadCountryCodesDWH = PythonOperator(task_id='loadCountryCodes', python_callable=loadCountryCodesDWH, dag=dag)
loadCodesInCntDWH = PythonOperator(task_id='loadCodesInCountries', python_callable=loadCodesInCntDWH, op_args=[isUpdate], dag=dag)
loadCitiesDWH = PythonOperator(task_id='loadCities', python_callable=loadCities, op_args=[isUpdate], dag=dag)

point5 = EmptyOperator(task_id="point5")

# create tables with Laureates and Organizations information
loadLaureatesDWH = PythonOperator(task_id='loadLaureates', python_callable=loadLaureatesDWH, op_args=[isUpdate], dag=dag)
loadOrganizationsDWH = PythonOperator(task_id='loadOrganizations', python_callable=loadOrganizationsDWH, op_args=[isUpdate], dag=dag)
loadPersonsInOrgsDWH = PythonOperator(task_id='loadPersonsInOrgs', python_callable=loadPersonsInOrgsDWH, op_args=[isUpdate], dag=dag)

point6 = EmptyOperator(task_id="point6")

# create table with Nobel Prizes
loadNobelPrizesDWH = PythonOperator(task_id='loadNobelPrizes', python_callable=loadNobelPrizesDWH, dag=dag)

point7 = EmptyOperator(task_id="point7")

# create table Dataset
loadDatasetDWH = PythonOperator(task_id='loadDataset', python_callable=loadDatasetDWH, op_args=[isUpdate], dag=dag)

point8 = EmptyOperator(task_id="point8")

# drop Staging Delta tables
dropDeltaTables = BashOperator(task_id='dropDeltaTables', bash_command=DROP_DELTA_TABLES, dag=dag)

# end
end = EmptyOperator(task_id="end")

# ------------Main DAG - description of operators end ----------

# ----------- Main DAG - sequence of operators start ----------

# start
if not isUpdate:
    start >> createDBDWH >> point1
else:
    start >> point1
    
# preprocessing
point1 >> createTmpCntTbl >> createTmpLaureatesTbl >> createTmpCitiesTbl >> point2

# preprocessing - missing data
if not isUpdate:
    point2 >> createTmpHistCntTbl >> createTmpMisCitiesTbl >> createTmpMisOrgsTbl >> point3
else:
    point2 >> point3

# create simple reference tables
if not isUpdate:
    point3 >> loadFromNobelDfDWH >> loadRegionsDWH >> loadContinentsDWH >> loadCurrencyCodesDWH >> point4
else:
    point3 >> loadFromNobelDfDWH >> point4

# create tables with Country and City information
if not isUpdate:
    point4 >> loadCountriesDWH >> loadCountryCodesDWH >> loadCodesInCntDWH >> loadCitiesDWH >> point5
else:
    point4 >> loadCountriesDWH >> loadCodesInCntDWH >> loadCitiesDWH >> point5

# create tables with Laureates and Organizations information
point5 >> loadLaureatesDWH >> loadOrganizationsDWH >> loadPersonsInOrgsDWH >> point6

# create table with Nobel Prizes
point6 >> loadNobelPrizesDWH >> point7
# create table Dataset
point7 >> loadDatasetDWH >> point8
# drop Staging Delta tables
point8 >> dropDeltaTables >> end

# ----------- Main DAG - sequence of operators end ----------
