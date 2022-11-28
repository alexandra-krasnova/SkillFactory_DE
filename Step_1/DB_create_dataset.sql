/* Типы колонок:

1. main columns - основная информация о факте - Нобелевской Премии.

2. birth date, city and country description columns - информация о дате рождения, городе и стране, в котором родился Нобелевский лауреат.
Содержит экономическую и географическую информацию о месте, т.к. важны все условия жизни, в которых жил лауреат до получения Премии.

3. death date, city and country description columns - информация о дате смерти, городе и стране, в котором умер Нобелевский лауреат.
Не содержит подробную экономическую и географическую информацию о месте, только об условиях жизни (расположение, климат, население).

4. organization name, city and country description columns - информация о городе и стране, в которой находится организация. 
Содержит только основные характеристики: имя, регион, население и т.д.
*/

CREATE TABLE LAUREATES
(
	-- main columns
	LAUREATE_ID number(38) GENERATED AS IDENTITY, 
	FULL_NAME varchar2(255), 
	YEAR number(38), 
	CATHEGORY varchar2(30), 
	PRIZE varchar2(255), 
	MOTIVATION varchar2(255), 
	PRIZE_SHARE number(38, 12), 
	LAYREATE_TYPE varchar2(20) 
	GENDER varchar2(30), 
	
	-- birth date, city and country description columns
	BIRTH_DATE date, 
	BIRTH_SITY_ID number(38), 
	BIRTH_SITY_NAME varchar2(50), 
	BIRTH_SITY_NAME_OLD varchar2(50), 
	BIRTH_SITY_REGION varchar2(10), 
	BIRTH_SITY_POPULATION number(38), 
	BIRTH_SITY_LATITUDE varchar2(100), 
	BIRTH_SITY_LONGITUDE varchar2(100), 
	BIRTH_COUNTRY_ID number(38), 
	BIRTH_COUNTRY_NAME varchar2(50), 
	BIRTH_COUNTRY_NAME_OLD varchar2(50), 
	BIRTH_REGION_NAME varchar2(50), 
	BIRTH_CONTINENT_NAME varchar2(50), 
	BIRTH_COUNTRY_POPULATION number(38), 
	BIRTH_COUNTRY_AREA_SQ_MILES number(38), 
	BIRTH_COUNTRY_POP_DENCITY_PER_SQ_MILE number(38, 12), 
	BIRTH_COUNTRY_COASTLINE number(38, 12), 
	BIRTH_COUNTRY_NET_MIGRATION number(38, 12), 
	BIRTH_COUNTRY_INFANT_MORTALITY_PER_1000 number(38, 12), 
	BIRTH_COUNTRY_GDB number(38, 12), 
	BIRTH_COUNTRY_PERCENT_LITERACY number(38, 12), 
	BIRTH_COUNTRY_PHONES_PER_1000 number(38, 12), 
	BIRTH_COUNTRY_PERCENT_ARABLE number(38, 12), 
	BIRTH_COUNTRY_PERCENT_CROPS number(38, 12), 
	BIRTH_COUNTRY_PERCENT_OTHER number(38, 12), 
	BIRTH_COUNTRY_CLIMATE number(38, 12), 
	BIRTH_COUNTRY_BIRTHRATE number(38, 12), 
	BIRTH_COUNTRY_DEATHRATE number(38, 12), 
	BIRTH_COUNTRY_AGRICULTURE number(38, 12), 
	BIRTH_COUNTRY_INDUSTRY number(38, 12), 
	BIRTH_COUNTRY_SERVICE number(38, 12), 
	
	-- death date, city and country description columns
	DEATH_DATE date, 
	DEATH_SITY_ID number(38), 
	DEATH_SITY_NAME varchar2(50), 
	DEATH_SITY_NAME_OLD varchar2(50), 
	DEATH_SITY_REGION varchar2(10), 
	DEATH_SITY_POPULATION number(38), 
	DEATH_SITY_LATITUDE varchar2(100), 
	DEATH_SITY_LONGITUDE varchar2(100), 
	DEATH_COUNTRY_ID number(38), 
	DEATH_COUNTRY_NAME varchar2(50), 
	DEATH_COUNTRY_NAME_OLD varchar2(50), 
	DEATH_REGION_NAME varchar2(50), 
	DEATH_CONTINENT_NAME varchar2(50), 
	DEATH_COUNTRY_POPULATION number(38), 
	DEATH_COUNTRY_GDB number(38, 12), 
	DEATH_COUNTRY_COASTLINE number(38, 12), 
	DEATH_COUNTRY_CLIMATE number(38, 12), 
	
	-- organization name, city and country description columns
	ORGANIZATION_NAME varchar2(255), 
	ORGANIZATION_NAME_OLD varchar2(255), 
	ORGANIZATION_SITY_ID number(38), 
	ORGANIZATION_SITY_NAME varchar2(50), 
	ORGANIZATION_SITY_NAME_OLD varchar2(50), 
	ORGANIZATION_SITY_REGION varchar2(10), 
	ORGANIZATION_SITY_POPULATION number(38), 
	ORGANIZATION_SITY_LATITUDE varchar2(100), 
	ORGANIZATION_SITY_LONGITUDE varchar2(100), 
	ORGANIZATION_COUNTRY_ID number(38), 
	ORGANIZATION_COUNTRY_NAME varchar2(50), 
	ORGANIZATION_COUNTRY_NAME_OLD varchar2(50), 
	ORGANIZATION_REGION_NAME varchar2(50), 
	ORGANIZATION_CONTINENT_NAME varchar2(50), 
	ORGANIZATION_COUNTRY_POPULATION number(38), 
	ORGANIZATION_COUNTRY_GDB number(38, 12), 
);