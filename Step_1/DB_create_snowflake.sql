/* 
Таблица NOBEL_PRIZES является фактом и содержит информацию о вручении Нобелевской Премии.
Информация берется из датасета "Nobel Laureates".
Содержит ссылки на таблицы:
	LAUREATE_TYPES - тип лауреата, "Individual" или "Organization".
	CATHEGORIES - категория Премии, например, "Peace" или "Physics".
	PRIZE_TYPES - наименование Премии, например, "The Nobel Prize in Physics 2011".
	LAUREATE_PERSONS - Нобелевский лауреат - человек (т.е. если тип лауреата = "Individual").
	SOCIETIES - Нобелевский лауреат - сообщество (т.е. если тип лауреата = "Organization").	
*/
CREATE TABLE NOBEL_PRIZES (
	NOBEL_PRIZE_ID number(38) GENERATED AS IDENTITY, 
	CATHEGORY_ID number(38) NOT NULL, 
	LAUREATE_ID number(38) NOT NULL, 
	LAUREATE_TYPE_ID number(38) NOT NULL, 
	YEAR number(38), 
	PRIZE_TYPE_ID number(38) NOT NULL, 
	MOTIVATION varchar2(255), 
	PRIZE_SHARE number(38, 12), 
	PRIMARY KEY (NOBEL_PRIZE_ID)
);

ALTER TABLE NOBEL_PRIZES ADD CONSTRAINT FKNOBEL_PRIZ504992 FOREIGN KEY (LAUREATE_TYPE_ID) REFERENCES LAUREATE_TYPES (LAUREATE_TYPE_ID);
ALTER TABLE NOBEL_PRIZES ADD CONSTRAINT FKNOBEL_PRIZ9224 FOREIGN KEY (CATHEGORY_ID) REFERENCES CATHEGORIES (CATHEGORY_ID);
ALTER TABLE NOBEL_PRIZES ADD CONSTRAINT FKNOBEL_PRIZ244115 FOREIGN KEY (PRIZE_TYPE_ID) REFERENCES PRIZE_TYPES (PRIZE_TYPE_ID);
ALTER TABLE NOBEL_PRIZES ADD CONSTRAINT FKNOBEL_PRIZ84225 FOREIGN KEY (LAUREATE_ID) REFERENCES LAUREATE_PERSONS (LAUREATE_ID);
ALTER TABLE NOBEL_PRIZES ADD CONSTRAINT FKNOBEL_PRIZ378469 FOREIGN KEY (LAUREATE_ID) REFERENCES SOCIETIES (LAUREATE_ID);

/* 
Таблица LAUREATE_PERSONS содержит информацию о Нобелевских лауреатах - людях, т.е. Лауреатов с типом  = "Individual".
Информация берется из датасета "Nobel Laureates".
Содержит ссылки на таблицы:
	GENDERS - пол лауреата, "Male" или "Female".
	SITIES - ссылается на таблицу дважды, ссылка 1 - информация о городе рождения лауреата, ссылка 2 - информация о городе, в котором умер лауреат.
*/
CREATE TABLE LAUREATE_PERSONS (
	LAUREATE_ID number(38) GENERATED AS IDENTITY, 
	FULL_NAME varchar2(255) NOT NULL, 
	GENDER_ID number(38) NOT NULL, 
	BIRTH_DATE date, 
	BIRTH_CITY_ID number(38) NOT NULL, 
	DEATH_DATE date, 
	DEATH_CITY_ID number(38) NOT NULL, 
	PRIMARY KEY (LAUREATE_ID)
);

ALTER TABLE LAUREATE_PERSONS ADD CONSTRAINT FKLAUREATE_P646534 FOREIGN KEY (GENDER_ID) REFERENCES GENDERS (GENDER_ID);
ALTER TABLE LAUREATE_PERSONS ADD CONSTRAINT FKLAUREATE_P770247 FOREIGN KEY (BIRTH_CITY_ID) REFERENCES SITIES (SITY_ID);
ALTER TABLE LAUREATE_PERSONS ADD CONSTRAINT FKLAUREATE_P965230 FOREIGN KEY (DEATH_CITY_ID) REFERENCES SITIES (SITY_ID);

/* 
Таблица ORGANIZATIONS содержит информацию об организациях - институтах, в которых работал лауреат (колонки Organization... в датасете Nobel Laureates).
Информация берется из датасета "Nobel Laureates".
Содержит ссылки на таблицы:
	SITIES - информация о городе, в котором располагалась организация.
	ORGANIZATIONS - ссылка на другую строку в этой же таблице, т.е. на Parent Id в случае, когда в датасете одни и те же организации имеют подразделения 
		либо разные имена. Например, "Harvard University, Biological Laboratories" и "Harvard University" - скорей всего, одна организация, и первое название 
		должно ссылаться на второе.
*/
CREATE TABLE ORGANIZATIONS (
	ORGANIZATION_ID number(38) GENERATED AS IDENTITY, 
	ORGANIZATION_NAME varchar2(255) NOT NULL, 
	ORGANIZATION_SITY_ID number(38) NOT NULL, 
	PARENT_ORGANIZATION_ID number(38), 
	PRIMARY KEY (ORGANIZATION_ID)
);

ALTER TABLE ORGANIZATIONS ADD CONSTRAINT FKORGANIZATI17103 FOREIGN KEY (ORGANIZATION_SITY_ID) REFERENCES SITIES (SITY_ID);
ALTER TABLE ORGANIZATIONS ADD CONSTRAINT FKORGANIZATI325887 FOREIGN KEY (PARENT_ORGANIZATION_ID) REFERENCES ORGANIZATIONS (ORGANIZATION_ID);

/* 
Таблица PERSONS_ORGANIZATIONS связывает лауреата-человека с организацией, в которой он работал. 
Информация берется из датасета "Nobel Laureates".
Вынесено в отдельную таблицу, т.к. лауреат мог работать в 2 и более организациях на момент получения Премии.
Содержит ссылки на таблицы:
	ORGANIZATIONS - организация, в которой работал лауреат.
	LAUREATE_PERSONS - информация о лауреате.
*/
CREATE TABLE PERSONS_ORGANIZATIONS (
	PER_ORG_ID number(38) GENERATED AS IDENTITY, 
	LAUREATE_ID number(38) NOT NULL, 
	ORGANIZATION_ID number(38) NOT NULL, 
	PRIMARY KEY (PER_ORG_ID)
);

ALTER TABLE PERSONS_ORGANIZATIONS ADD CONSTRAINT FKPERSONS_OR691725 FOREIGN KEY (ORGANIZATION_ID) REFERENCES ORGANIZATIONS (ORGANIZATION_ID);
ALTER TABLE PERSONS_ORGANIZATIONS ADD CONSTRAINT FKPERSONS_OR504321 FOREIGN KEY (LAUREATE_ID) REFERENCES LAUREATE_PERSONS (LAUREATE_ID);

/* 
Таблица SOCIETIES содержит информацию о лауреате-сообществе в случае, если тип лауреата = "Organization".
Информация берется из датасета "Nobel Laureates".
*/
CREATE TABLE SOCIETIES (
	LAUREATE_ID number(38) GENERATED AS IDENTITY, 
	SOCIETY_NAME_ORIG varchar2(255) NOT NULL, 
	SOCIETY_NAME_ENG varchar2(255), 
	PRIMARY KEY (LAUREATE_ID)
);

/* 
Таблица COUNTRIES содержит информацию о странах: регион страны, ее население, валюта и т.д.
Информация берется из датасетов "Countries of the World" и "Country Data".
Содержит ссылки на таблицы:
	REGIONS - регион, в котором находится страна (колонка Region в "Countries of the World").
	CONTINENTS - континент, на котором находится страна.
	CURRENCIES - валюта страны.
	COUNTRIES - ссылка на другую запись в этой же таблицы в случае, если страна меняла название. Родительская запись должна содержать 
		современное название страны, дочерняя запись - название страны на момент вручения премии.
*/
CREATE TABLE COUNTRIES (
	COUNTRY_ID number(38) GENERATED AS IDENTITY, 
	REGION_ID number(38) NOT NULL, 
	CONTINENT_ID number(38) NOT NULL, 
	CURRENCY_ID number(38) NOT NULL, 
	COUNTRY_NAME varchar2(50) NOT NULL, 
	COUNTRY_CODE varchar2(5) NOT NULL, 
	COUNTRY_ISO3 varchar2(5), 
	PHONE_CODE varchar2(10), 
	POPULATION number(38), 
	AREA_SQ_MILES number(38), 
	POP_DENCITY_PER_SQ_MILE number(38, 12), 
	COASTLINE number(38, 12), 
	NET_MIGRATION number(38, 12), 
	INFANT_MORTALITY_PER_1000 number(38, 12), 
	GDB_DOLLAR_PER_CAPITA number(38), 
	PERCENT_LITERACY number(38, 12), 
	PHONES_PER_1000 number(38, 12), 
	PERCENT_ARABLE number(38, 12), 
	PERCENT_CROPS number(38, 12), 
	PERCENT_OTHER number(38, 12), 
	CLIMATE number(38, 12), 
	BIRTHRATE number(38, 12), 
	DEATHRATE number(38, 12), 
	AGRICULTURE number(38, 12), 
	INDUSTRY number(38, 12), 
	SERVICE number(38, 12), 
	PARENT_COUNTRY_ID number(38) NOT NULL, 
	PRIMARY KEY (COUNTRY_ID)
);

ALTER TABLE COUNTRIES ADD CONSTRAINT FKCOUNTRIES631556 FOREIGN KEY (REGION_ID) REFERENCES REGIONS (REGION_ID);
ALTER TABLE COUNTRIES ADD CONSTRAINT FKCOUNTRIES264430 FOREIGN KEY (CONTINENT_ID) REFERENCES CONTINENTS (CONTINENT_ID);
ALTER TABLE COUNTRIES ADD CONSTRAINT FKCOUNTRIES159421 FOREIGN KEY (CURRENCY_ID) REFERENCES CURRENCIES (CURRENCY_ID);
ALTER TABLE COUNTRIES ADD CONSTRAINT FKCOUNTRIES601390 FOREIGN KEY (PARENT_COUNTRY_ID) REFERENCES COUNTRIES (COUNTRY_ID);

/* 
Таблица SITIES содержит информацию о городах: страну, население и т.д.
Информация берется из датасета "World Cities Database".
Содержит ссылки на таблицы:
	COUNTRIES - информация о стране, в которой находится город.
	SITIES - ссылка на другую запись в этой же таблицы в случае, если город менял название. Родительская запись должна содержать 
		современное название города, дочерняя запись - название города (и, возможно, другой страны, в которой находился ранее город) 
		на момент вручения премии.
*/
CREATE TABLE SITIES (
	SITY_ID number(38) GENERATED AS IDENTITY, 
	SITY_NAME varchar2(50) NOT NULL, 
	COUNTRY_ID number(38) NOT NULL, 
	SITY_REGION varchar2(10), 
	POPULATION number(38), 
	LATITUDE varchar2(100), 
	LONGITUDE varchar2(100), 
	PARENT_CITY_ID number(38), 
	PRIMARY KEY (SITY_ID)
);

ALTER TABLE SITIES ADD CONSTRAINT FKSITIES819550 FOREIGN KEY (COUNTRY_ID) REFERENCES COUNTRIES (COUNTRY_ID);
ALTER TABLE SITIES ADD CONSTRAINT FKSITIES751193 FOREIGN KEY (PARENT_CITY_ID) REFERENCES SITIES (SITY_ID);

/* 
Таблица CATHEGORIES содержит информацию о категориях Нобелевской Премии, например, "Peace" или "Physics".
Информация берется из датасета "Nobel Laureates".
*/
CREATE TABLE CATHEGORIES (
	CATHEGORY_ID number(38) GENERATED AS IDENTITY, 
	CATHEGORY varchar2(30) NOT NULL, 
	PRIMARY KEY (CATHEGORY_ID)
);

/* 
Таблица LAUREATE_TYPES содержит информацию о типах лауреата, "Individual" или "Organization".
Информация берется из датасета "Nobel Laureates".
*/
CREATE TABLE LAUREATE_TYPES (
	LAUREATE_TYPE_ID number(38) GENERATED AS IDENTITY, 
	LAYREATE_TYPE varchar2(20) NOT NULL, 
	PRIMARY KEY (LAUREATE_TYPE_ID)
);

/* 
Таблица PRIZE_TYPES содержит информацию о видах Премияй, например, "The Nobel Prize in Physics 2011".
Информация берется из датасета "Nobel Laureates".
*/
CREATE TABLE PRIZE_TYPES (
	PRIZE_TYPE_ID number(38) GENERATED AS IDENTITY, 
	PRIZE_TYPE varchar2(255) NOT NULL, 
	PRIMARY KEY (PRIZE_TYPE_ID)
);

/* 
Таблица GENDERS содержит информацию о поле лауреата, "Male" или "Female".
Информация берется из датасета "Nobel Laureates".
*/
CREATE TABLE GENDERS (
	GENDER_ID number(38) NOT NULL, 
	GENDER varchar2(30) NOT NULL, 
	PRIMARY KEY (GENDER_ID)
);

/* 
Таблица CONTINENTS содержит информацию о континентах.
Информация берется из датасета "Country Data" - "continent.json".
*/
CREATE TABLE CONTINENTS (
	CONTINENT_ID number(38) GENERATED AS IDENTITY, 
	CONTINENT_NAME varchar2(50) NOT NULL, 
	CONTINENT_CODE varchar2(5) NOT NULL, 
	PRIMARY KEY (CONTINENT_ID)
);

/* 
Таблица CURRENCIES содержит информацию о валютах.
Информация берется из датасета "Country Data" - "currency.json".
*/
CREATE TABLE CURRENCIES (
	CURRENCY_ID number(38) GENERATED AS IDENTITY, 
	CURRENCY varchar2(50) NOT NULL, 
	CURRENCY_CODE varchar2(5) NOT NULL, 
	PRIMARY KEY (CURRENCY_ID)
);

/* 
Таблица REGIONS содержит информацию о регионах, в которых находятся города
Информация берется из датасета "World Cities Database" - колонки "Region".
*/
CREATE TABLE REGIONS (
	REGION_ID number(38) GENERATED AS IDENTITY, 
	REGION_NAME varchar2(50) NOT NULL, 
	PRIMARY KEY (REGION_ID)
);













