use unextjun12;


drop table if exists customer;
create table customer(C_CUSTKEY int,C_NAME varchar(200),C_ADDRESS varchar(200),C_NATIONKEY int,C_PHONE varchar(200),C_ACCTBAL float,C_MKTSEGMENT varchar(200),C_COMMENT varchar(200));


drop table if exists lineitem;
create table lineitem(L_ORDERKEY  int,L_PARTKEY	 int,L_SUPPKEY	 int,L_LINENUMBER   int,L_QUANTITY  float,L_EXTENDEDPRICE  float,L_DISCOUNT  float,L_TAX  float,L_RETURNFLAG  varchar(10),L_LINESTATUS  varchar(10),L_SHIPDATE  date,L_COMMITDATE  date,L_RECEIPTDATE  date,L_SHIPINSTRUCT  varchar(100),L_SHIPMODE  varchar(100),L_COMMENT  varchar(100));


drop table if exists nation;
create table nation(N_NATIONKEY  int,N_NAME  varchar(200),N_REGIONKEY int,N_COMMENT  varchar(200));


drop table if exists orders;
create table orders(O_ORDERKEY  int,O_CUSTKEY  int,O_ORDERSTATUS  varchar(200),O_TOTALPRICE  float,O_ORDERDATE  date,O_ORDERPRIORITY varchar(100),O_CLERK varchar(200),O_SHIPPRIORITY  varchar(100),O_COMMENT varchar(200));


drop table if exists part;
create table part(P_PARTKEY  int,P_NAME  varchar(200),P_MFGR  varchar(200),P_BRAND  varchar(200),P_TYPE  varchar(200),P_SIZE  int,P_CONTAINER  varchar(200),P_RETAILPRICE  float,P_COMMENT  varchar(200));


drop table if exists partsupp;
create table partsupp(PS_PARTKEY  int,PS_SUPPKEY  int,PS_AVAILQTY  int,PS_SUPPLYCOST  float,PS_COMMENT  varchar(200));


drop table if exists region;
create table region(R_REGIONKEY  int,R_NAME  varchar(200),R_COMMENT  varchar(200));


drop table if exists supplier;
create table supplier(S_SUPPKEY  int,S_NAME  varchar(200),S_ADDRESS  varchar(200),S_NATIONKEY  int,S_PHONE  varchar(200),S_ACCTBAL  float,S_COMMENT  varchar(200));












