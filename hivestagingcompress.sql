use dhanuja_hive;
drop table if exists customer_orc;
drop table if exists customer_parquet;
drop table if exists customer_seq;
drop table if exists customer_rcf;

create table customer_orc(C_CUSTKEY int,C_NAME string,C_ADDRESS string,C_NATIONKEY int,C_PHONE string,C_ACCTBAL float,C_MKTSEGMENT string,C_COMMENT string) row format delimited fields terminated by '|' lines terminated by '\n' stored as orcfile tblproperties ("skip.header.line.count"="1");
insert overwrite table customer_orc select * from customer;

create table customer_rcf(C_CUSTKEY int,C_NAME string,C_ADDRESS string,C_NATIONKEY int,C_PHONE string,C_ACCTBAL float,C_MKTSEGMENT string,C_COMMENT string) row format delimited fields terminated by '|' lines terminated by '\n' stored as rcfile tblproperties ("skip.header.line.count"="1");
insert overwrite table customer_rcf select * from customer;

create table customer_parquet(C_CUSTKEY int,C_NAME string,C_ADDRESS string,C_NATIONKEY int,C_PHONE string,C_ACCTBAL float,C_MKTSEGMENT string,C_COMMENT string) row format delimited fields terminated by '|' lines terminated by '\n' stored as parquetfile tblproperties ("skip.header.line.count"="1");
insert overwrite table customer_parquet select * from customer;

create table customer_seq(C_CUSTKEY int,C_NAME string,C_ADDRESS string,C_NATIONKEY int,C_PHONE string,C_ACCTBAL float,C_MKTSEGMENT string,C_COMMENT string) row format delimited fields terminated by '|' lines terminated by '\n' stored as sequencefile tblproperties ("skip.header.line.count"="1");
insert overwrite table customer_seq select * from customer;