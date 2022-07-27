drop table if exists lineitemkafka;
create table lineitemkafka(L_ORDERKEY  int,L_PARTKEY	 int,L_SUPPKEY	 int,L_LINENUMBER   int,L_QUANTITY  float,L_EXTENDEDPRICE  float,L_DISCOUNT  float,L_TAX  float,L_RETURNFLAG  string,L_LINESTATUS  string,L_SHIPDATE  date,L_COMMITDATE  date,L_RECEIPTDATE  date,L_SHIPINSTRUCT  string,L_SHIPMODE  string,L_COMMENT  string) row format delimited fields terminated by '|' lines terminated by '\n';

drop table if exists orderskafka;
create table orderskafka(O_ORDERKEY  int,O_CUSTKEY  int,O_ORDERSTATUS  string,O_TOTALPRICE  float,O_ORDERDATE  date,O_ORDERPRIORITY string,O_CLERK string,O_SHIPPRIORITY  string,O_COMMENT string) row format delimited fields terminated by '|' lines terminated by '\n';


