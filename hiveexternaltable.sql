use dhanuja_hive;

drop table if exists customer;
create external table customer(C_CUSTKEY int,C_NAME string,C_ADDRESS string,C_NATIONKEY int,C_PHONE string,C_ACCTBAL float,C_MKTSEGMENT string,C_COMMENT string) row format delimited fields terminated by '|' lines terminated by '\n' location '/user/unextjun12/retail/customer' tblproperties ("skip.header.line.count"="1");


drop table if exists lineitem;
create external table lineitem(L_ORDERKEY  int,L_PARTKEY	 int,L_SUPPKEY	 int,L_LINENUMBER   int,L_QUANTITY  float,L_EXTENDEDPRICE  float,L_DISCOUNT  float,L_TAX  float,L_RETURNFLAG  string,L_LINESTATUS  string,L_SHIPDATE  date,L_COMMITDATE  date,L_RECEIPTDATE  date,L_SHIPINSTRUCT  string,L_SHIPMODE  string,L_COMMENT  string) row format delimited fields terminated by '|' lines terminated by '\n' location '/user/unextjun12/retail/lineitem' tblproperties ("skip.header.line.count"="1");


drop table if exists nation;
create external table nation(N_NATIONKEY  int,N_NAME  string,N_REGIONKEY int,N_COMMENT  string) row format delimited fields terminated by '|' lines terminated by '\n' location '/user/unextjun12/retail/nation'  tblproperties ("skip.header.line.count"="1");



drop table if exists orders;
create external table orders(O_ORDERKEY  int,O_CUSTKEY  int,O_ORDERSTATUS  string,O_TOTALPRICE  float,O_ORDERDATE  date,O_ORDERPRIORITY string,O_CLERK string,O_SHIPPRIORITY  string,O_COMMENT string) row format delimited fields terminated by '|' lines terminated by '\n' location '/user/unextjun12/retail/orders' tblproperties ("skip.header.line.count"="1");


drop table if exists part;
create external table part(P_PARTKEY  int,P_NAME  string,P_MFGR  string,P_BRAND  string,P_TYPE  string,P_SIZE  int,P_CONTAINER  string,P_RETAILPRICE  float,P_COMMENT  string) row format delimited fields terminated by '|' lines terminated by '\n' location '/user/unextjun12/retail/part' tblproperties ("skip.header.line.count"="1");


drop table if exists partsupp;
create external table partsupp(PS_PARTKEY  int,PS_SUPPKEY  int,PS_AVAILQTY  int,PS_SUPPLYCOST  float,PS_COMMENT  string) row format delimited fields terminated by '|' lines terminated by '\n' location '/user/unextjun12/retail/partsupp'  tblproperties ("skip.header.line.count"="1");


drop table if exists region;
create external table region(R_REGIONKEY  int,R_NAME  string,R_COMMENT  string) row format delimited fields terminated by '|' lines terminated by '\n' location '/user/unextjun12/retail/region' tblproperties ("skip.header.line.count"="1");


drop table if exists supplier;
create external table supplier(S_SUPPKEY  int,S_NAME  string,S_ADDRESS  string,S_NATIONKEY  int,S_PHONE  string,S_ACCTBAL  float,S_COMMENT  string) row format delimited fields terminated by '|' lines terminated by '\n' location '/user/unextjun12/retail/supplier'  tblproperties ("skip.header.line.count"="1");