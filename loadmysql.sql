use unextjun12;
load data local infile '/home/unextjun12/retail/Retail/customer.csv' into table customer fields terminated by '|' lines terminated by '\n'  ignore 1 rows;
load data local infile '/home/unextjun12/retail/Retail/lineitem.csv' into table lineitem fields terminated by '|' lines terminated by '\n'  ignore 1 rows;
load data local infile '/home/unextjun12/retail/Retail/nation.csv' into table nation fields terminated by '|' lines terminated by '\n'  ignore 1 rows;
load data local infile '/home/unextjun12/retail/Retail/orders.csv' into table orders fields terminated by '|' lines terminated by '\n'  ignore 1 rows;
load data local infile '/home/unextjun12/retail/Retail/part.csv' into table part fields terminated by '|' lines terminated by '\n'  ignore 1 rows;
load data local infile '/home/unextjun12/retail/Retail/partsupp.csv' into table partsupp fields terminated by '|' lines terminated by '\n'  ignore 1 rows;
load data local infile '/home/unextjun12/retail/Retail/region.csv' into table region fields terminated by '|' lines terminated by '\n'  ignore 1 rows;
load data local infile '/home/unextjun12/retail/Retail/supplier.csv' into table supplier fields terminated by '|' lines terminated by '\n'  ignore 1 rows;
