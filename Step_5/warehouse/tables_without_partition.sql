-- TABLES WITHOUT PARTITION - DATA WAREHOUSE

-- следующие таблицы являются справочниками и не должны обновляться на регулярной основе:
-- CONTINENTS, REGIONS, COUNTRY_CODES, CURRENCIES, CODES_IN_COUNTRIES, CATEGORIES, GENDERS, LAUREATE_TYPES
-- для этих таблиц не будет применяться партиционирование и кластеризация
-- при необходимости, данные должны обновляться по запросу вручную
-- загрузим данные из таблиц, указанных выше, из базы akrasnovadb в базу akrasnova_wrh, где будут находиться таблицы с партиционированием

create table akrasnova_wrh.continents stored as orc as select * from akrasnovadb.continents;
create table akrasnova_wrh.regions stored as orc as select * from akrasnovadb.regions;
create table akrasnova_wrh.country_codes stored as orc as select * from akrasnovadb.country_codes;
create table akrasnova_wrh.currencies stored as orc as select * from akrasnovadb.currencies;
create table akrasnova_wrh.codes_in_countries stored as orc as select * from akrasnovadb.codes_in_countries;
create table akrasnova_wrh.categories stored as orc as select * from akrasnovadb.categories;
create table akrasnova_wrh.genders stored as orc as select * from akrasnovadb.genders;
create table akrasnova_wrh.laureate_types stored as orc as select * from akrasnovadb.laureate_types;