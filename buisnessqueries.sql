set hive.cli.print.header=true;
use dhanuja_hive;

select a.yr as year,a.mn as month_of_yearwith_maxorders,a.orderspermonth as maxorders,a.dnk from (select a.*,dense_rank() over(partition by a.yr order by a.orderspermonth desc) as dnk from (select b.yr,b.mn,count(*) as orderspermonth from (select a.*,year(a.o_orderdate) as yr,month(a.o_orderdate) as mn from orders a)b group by b.yr,b.mn) a)a where a.dnk=1  limit 100

select a.nation_name,count(*) as no_of_customers from (select a.*,b.n_name as nation_name from customer a left join nation b on a.c_nationkey=b.n_nationkey)a group by nation_name order by no_of_customers desc;                                       
                                                                                                                               
select substring(n_name,0,2) as nation_code,n_name as nation_name from nation; 

select round(c_acctbal) as acct_balance from customer limit 10;

create table lucky_number_table as select a.*,rand() as lucky_draw_number from customer a;

select lucky_draw_number from lucky_number_table limit 10;

set lucky_no=0.21423143109213338;

select * from lucky_number_table where lucky_draw_number=${hiveconf:lucky_no};