create external table if not exists db_staging.nobel_laureates_ext (
    year int,
    category varchar(20),
    prize varchar(50),
    motivation varchar(255),
    prize_share varchar(10),
    laureate_id int,
    laureate_type varchar(20),
    full_name varchar(255),
    birth_date date,
    birth_city varchar(50),
    birth_country varchar(50),
    gender varchar(10),
    organization_name varchar(255),
    organization_city varchar(50),
    organization_country varchar(50),
    deathdate date,
    death_city varchar(50),
    death_country varchar(50)
)
comment 'Nobel Laureate table - for data in CSV file nobel.csv'
--row format delimited
--fields terminated by ','
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\"")
stored as textfile
location '/staging/nobel_laureates'
tblproperties ('skip.header.line.count'='1');

create table db_staging.nobel_laureates stored as orc as select * from db_staging.nobel_laureates_ext;

drop table db_staging.nobel_laureates_ext;