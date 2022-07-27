use dhanuja_hive;

create table orders_part_buc_retail(O_ORDERKEY  int,O_CUSTKEY  int,O_TOTALPRICE  float,O_ORDERDATE  date,O_CLERK string,O_SHIPPRIORITY  string,O_COMMENT string)PARTITIONED BY (O_ORDERPRIORITY string,O_ORDERSTATUS string) CLUSTERED BY (O_ORDERDATE) INTO 5 BUCKETS;

INSERT OVERWRITE TABLE orders_part_buc_retail PARTITION (o_orderpriority='1-URGENT',o_orderstatus) select O_ORDERKEY,O_CUSTKEY,O_TOTALPRICE,O_ORDERDATE,O_CLERK,O_SHIPPRIORITY,O_COMMENT,o_orderstatus from orders;                                                                                                                       