# JSON файлы с Country Code будут загружены только при первичной загрузке, delta таблицы не создаются

# Функция, создающая текст скрипта для создания Staging таблиц из JSON файлов
def createScript(tableName,columnName,fileName):
    result = """
    /usr/bin/beeline -u jdbc:hive2://localhost:10000/default -n hive -p 123 <<END_SQL
    create external table akrasnova_staging."""+tableName+"""_ext (
        country string,
        """+columnName+""" string
    )
    comment '"""+fileName+""" table - for data in JSON file """+fileName+""".json'
    row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
    stored as textfile
    location '/akrasnova/"""+tableName+"""';

    create table if not exists akrasnova_staging."""+tableName+""" stored as orc as
        select * from akrasnova_staging."""+tableName+"""_ext;
    
    drop table akrasnova_staging."""+tableName+"""_ext;
    END_SQL
    """
    
    return result

# Names - ISO2 codes
LOAD_NAMES_STG = createScript('country_names','name','names')

# ISO3 codes
LOAD_ISO3_STG = createScript('country_iso3','iso3','iso3')

# Currency
LOAD_CURRENCIES_STG = createScript('country_currencies','currency','currency')

# Continents
LOAD_CONTINENTS_STG = createScript('country_continents','continent','continent')

# Phones
LOAD_PHONES_STG = createScript('country_phones','phone','phone')

# Capitals
LOAD_CAPITALS_STG = createScript('country_capital','capital','capital')
