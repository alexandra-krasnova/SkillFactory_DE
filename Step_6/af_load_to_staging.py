import sys
sys.path.append('/usr/lib/spark/python')
sys.path.append('/usr/lib/spark/python/lib/py4j-0.10.7-src.zip')

import datetime
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator

from common_stg.hive_commands_create_db_stg import CREATE_DB_CMD_STG
from load_cities_stg import SQOOP_CITIES_STG, LOAD_CITIES_STG
from load_countries_stg import LOAD_COUNTRIES_STG
from load_nobel_prizes_stg import LOAD_NOB_PRIZES_STG
from load_from_json_stg import LOAD_NAMES_STG, LOAD_ISO3_STG, LOAD_CURRENCIES_STG, \
    LOAD_CONTINENTS_STG, LOAD_PHONES_STG, LOAD_CAPITALS_STG

# Данный граф представляет собой создание таблиц и загрузку данных в Staging.
# В JSON таблицы данные загружаются только при первичной трансформации.

# Mode: CREATE (первичная загрузка) или UPDATE (загрузка обновленных данных)
isUpdate = False if Upper(Variable.get(DAG_NAME_MODE)) != 'UPDATE' else True

# DAG settings
DAG_NAME = "load_to_staging"
schedule = None
startDate = datetime.datetime(2023, 07, 10)

# main DAG
dag = DAG( dag_id=DAG_NAME, schedule_interval=schedule, start_date=startDate, catchup=False )

# ----------- Main DAG - description of operators start ----------

# start, preprocessing
start = EmptyOperator(task_id="start")

if not isUpdate:
    createDBSTG = BashOperator(task_id='createDB', bash_command=CREATE_DB_CMD_STG, dag=dag)

point1 = EmptyOperator(task_id="point1")

# create tables from MSSQL table
sqoopCitiesSTG = BashOperator(task_id='sqoopCities', bash_command=SQOOP_CITIES_STG, dag=dag)
loadCitiesSTG = BashOperator(task_id='loadCities', bash_command=LOAD_CITIES_STG, dag=dag)

point2 = EmptyOperator(task_id="point2")

# create tables from CSV files
loadCountriesSTG = BashOperator(task_id='loadCountries', bash_command=LOAD_COUNTRIES_STG, dag=dag)
loadNobelPrizesSTG = BashOperator(task_id='loadNobelPrizes', bash_command=LOAD_NOB_PRIZES_STG, dag=dag)

point3 = EmptyOperator(task_id="point3")

if not isUpdate:
    # create tables from JSON files
    loadNamesSTG = BashOperator(task_id='loadNames', bash_command=LOAD_NAMES_STG, dag=dag)
    loadISO3STG = BashOperator(task_id='loadISO3', bash_command=LOAD_ISO3_STG, dag=dag)
    loadCurrenciesSTG = BashOperator(task_id='loadCurrencies', bash_command=LOAD_CURRENCIES_STG, dag=dag)
    loadContinentsSTG = BashOperator(task_id='loadContinents', bash_command=LOAD_CONTINENTS_STG, dag=dag)
    loadPhonesSTG = BashOperator(task_id='loadPhones', bash_command=LOAD_PHONES_STG, dag=dag)
    loadCapitalsSTG = BashOperator(task_id='loadCapitals', bash_command=LOAD_CAPITALS_STG, dag=dag)

# end
end = EmptyOperator(task_id="end")

# ------------Main DAG - description of operators end ----------

# ----------- Main DAG - sequence of operators start ----------

# start
if not isUpdate:
    start >> createDBSTG >> point1
else:
    start >> point1

# create tables from MSSQL table
point1 >> sqoopCitiesSTG >> loadCitiesSTG >> point2

# create tables from CSV files
point2 >> loadCountriesSTG >> loadNobelPrizesSTG >> point3

if not isUpdate:
    # create tables from JSON files
    point3 >> loadNamesSTG >> loadISO3STG >> loadCurrenciesSTG >> loadContinentsSTG \
        >> loadPhonesSTG >> loadCapitalsSTG >> end
else:
    point3 >> end

# ----------- Main DAG - sequence of operators end ----------
