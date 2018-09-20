#### Create DataSource one for lastTouch
#### The original table contain duplicates
#### This table does not include FS_LS

DROP TABLE sandbox_dev_bi.josh_lastTouch PURGE;
CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_lastTouch
(
sh_event_id           STRING,
last_touched_date     DATE,
last_touched_email    STRING,
user_api              STRING

);
set mapred.job.queue.name=root.dev;
INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_lastTouch

select distinct t1.sh_event_id,t1.last_touched_date,t1.last_touched_email,t1.user_api
from sandbox_dev_etl_dq.active_event_last_touched t1
inner join (
select  sh_event_id, user_api, max(last_touched_date) AS MaxLast_touched_date
from sandbox_dev_etl_dq.active_event_last_touched
group by sh_event_id,user_api
) t2 on t1.sh_event_id = t2.sh_event_id AND t1.user_api=t2.user_api AND t1.last_touched_date=t2.MaxLast_touched_date;


#### This table includes FS_LS

DROP TABLE sandbox_dev_bi.josh_lastTouch_FS_LS PURGE;
CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_lastTouch_FS_LS
(
sh_event_id           STRING,
user_api              STRING,
acc_type              STRING,
acc_email             STRING,
last_touched_date     DATE
);
set mapred.job.queue.name=root.dev;
INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_lastTouch_FS_LS
select distinct t1.sh_eventid,t1.user_api,t1.acc_type,t1.acc_email,t2.last_touched_date
from sandbox_dev_etl_dq.client_logs t1
inner join (
select  sh_eventid, user_api, acc_type, max(transaction_date) AS last_touched_date 
from  sandbox_dev_etl_dq.client_logs
where acc_type = "main" or acc_type = "pricer"
group by sh_eventid,user_api,acc_type) t2 
on t1.sh_eventid = t2.sh_eventid AND t1.user_api=t2.user_api AND t1.transaction_date = t2.last_touched_date ;

### FS_LS_reshape

DROP TABLE sandbox_dev_bi.josh_lastTouch_FS_LS_reshape PURGE;
CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_lastTouch_FS_LS_reshape
(
sh_event_id                             STRING,
user_api                                STRING,
last_touched_non_FS_pricer              STRING,
last_touched_FS_pricer                  STRING,
last_touched_date_non_FS                DATE,
last_touched_date_FS                    DATE
);
set mapred.job.queue.name=root.dev;
INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_lastTouch_FS_LS_reshape
SELECT sh_event_id, user_api,
       MAX( IF( acc_type='main', acc_email, NULL ) ) AS last_touched_non_FS_pricer,
       MAX( IF( acc_type='pricer', acc_email, NULL ) ) AS last_touched_FS_pricer,
       MAX( IF( acc_type='main', last_touched_date, NULL ) ) AS last_touched_date_non_FS,
       MAX( IF( acc_type='pricer', last_touched_date, NULL ) ) AS last_touched_date_FS

FROM sandbox_dev_bi.josh_lastTouch_FS_LS 
GROUP BY sh_event_id, user_api;




#### Create DataSource two for ClientSales

DROP TABLE sandbox_dev_bi.josh_ClientSales PURGE;
CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_ClientSales
(
event_id                        STRING,
user_api                        STRING,
total_qty_sold                  INT,
last_sale_date                  Date,
qty_sold_past_week              INT,
qty_sold_yesterday              INT

);
set mapred.job.queue.name=root.dev;
INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_ClientSales

select event_id, user_api, sum(total_tickets_sold), max(last_invoice_date), sum(total_tickets_sold_last_week), sum(total_tickets_sold_yesterday)
from sandbox_dev_etl_dq.sales_data_daily_aggregate
group by event_id, user_api;



#### Create DataSource three for stubHubSales

DROP TABLE sandbox_dev_bi.josh_StubHubSales PURGE;
CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_StubHubSales
(
event_id                           STRING,
qty_sold_on_StubHub_not_accurate   DOUBLE
);
set mapred.job.queue.name=root.dev;
INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_StubHubSales
select collect_set(event_id), sum(quantity) 
from sandbox_dev_etl_dq.stubhub_events_all_sales_info
where eventdatelocal > "2017-06-01"
group by event_id;


#### Create DataSource four for by_event_click_initial


DROP TABLE sandbox_dev_bi.josh_by_event_click_initial PURGE;
CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_by_event_click_initial
(
sh_event_id                           STRING,
user_api                              STRING,
acc_email                             STRING,
transaction_date                      Date,
Create1                               INT,
Edit1                                 INT,
Save1                                 INT
);

set mapred.job.queue.name=root.dev;
INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_by_event_click_initial

select sh_event_id,user_api,acc_email,transaction_date,
        CASE WHEN acc_action='create' 
        THEN action_count
        ELSE NULL
        END AS Create1,
        CASE WHEN acc_action='edit' 
        THEN action_count
        ELSE NULL
        END AS Edit1,
        CASE WHEN acc_action='save' 
        THEN action_count
        ELSE NULL
        END AS Save1

from sandbox_dev_etl_dq.active_event_user_action;



#### Create DataSource five 

add jar /mapr/bgcluster/dev/jars/es-hadoop-5.3.0/elasticsearch-hadoop-5.3.0.jar;
DROP TABLE sandbox_dev_bi.josh_clientHealthSummaryByEventIdLatestDate_withClientName PURGE;
CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_clientHealthSummaryByEventIdLatestDate_withClientName
(
eventdate                 STRING, 
process_date              STRING, 
category                  STRING,
subcategory               STRING,
user_id                   STRING,
activeOrSold              STRING,
event_id                  STRING,
event_name                STRING,
primary_performer         STRING,
secondary_performer       STRING,
venue_name                STRING,
TotalListings             INT,
Listings_at_floor         INT, 
total_stale_days          INT,
oldest_update_date        DATE,
latest_update_date        DATE,
company_name              STRING
);

set mapred.job.queue.name=root.dev;

INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_clientHealthSummaryByEventIdLatestDate_withClientName
select 
t1.eventdate                 , 
t1.process_date              , 
t1.category                  ,
t1.subcategory               ,
t1.user_id                   ,
t1.activeOrSold              ,
t1.event_id                  ,
t1.event_name                ,
t1.primary_performer         ,
t1.secondary_performer       ,
t1.venue_name                ,
t1.TotalListings             ,
t1.Listings_at_floor         , 
t1.total_stale_days          ,
t1.oldest_update_date        ,
t1.latest_update_date        ,
t2.company_name

from sandbox_dev_bi.clientHealthSummaryByEventIdLatestDate t1
left join 
sandbox_dev_etl_dq.client_information_external  as t2
on t1.user_id = t2.client_id;
DROP TABLE sandbox_dev_bi.josh_active_listings PURGE;
CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_active_listings
(
event_id                                            STRING,
active_listings                                     INT, 
listings_at_floor                                   INT, 
total_stale_days                                    INT,
oldest_update_date_for_active_listings              DATE,
latest_update_date_for_active_listings              DATE
);

set mapred.job.queue.name=root.dev;

INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_active_listings
select 
event_id,
sum(totallistings)     AS      active_listings,
sum(listings_at_floor) AS      listings_at_floor,
sum(total_stale_days)  AS      total_stale_days,
min(oldest_update_date)AS      oldest_update_date_for_active_listings,
max(latest_update_date)AS      latest_update_date_for_active_listings

from sandbox_dev_bi.josh_clientHealthSummaryByEventIdLatestDate_withClientName
where activeorsold = 1
group by event_id;




DROP TABLE sandbox_dev_bi.josh_sold_listings PURGE;


CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_sold_listings
(
event_id                                STRING,
sold_listings                           INT
);

set mapred.job.queue.name=root.dev;

INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_sold_listings

select 
event_id,
sum(totallistings)     AS      sold_listings
from sandbox_dev_bi.josh_clientHealthSummaryByEventIdLatestDate_withClientName
where activeorsold = 0
group by event_id;














###  This is the first join

DROP TABLE sandbox_dev_bi.josh_lastTouch_FS_LS_reshape_join_by_event_click_initial PURGE;
CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_lastTouch_FS_LS_reshape_join_by_event_click_initial
(
sh_event_id                           STRING,
user_api                              STRING,
acc_email                             STRING,
transaction_date                      Date,
Create1                               INT,
Edit1                                 INT,
Save1                                 INT,
last_touched_non_FS_pricer            STRING,
last_touched_FS_pricer                STRING,
last_touched_date_non_FS              DATE,
last_touched_date_FS                  DATE

);
set mapred.job.queue.name=root.dev;
INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_lastTouch_FS_LS_reshape_join_by_event_click_initial
select t1.sh_event_id, t1.user_api, t1.acc_email, t1.transaction_date, t1.Create1, t1.Edit1, t1.Save1,
t2.last_touched_non_FS_pricer, t2.last_touched_FS_pricer, t2.last_touched_date_non_FS, t2.last_touched_date_FS
from sandbox_dev_bi.josh_by_event_click_initial  AS t1
left join sandbox_dev_bi.josh_lastTouch_FS_LS_reshape  AS t2
on t1.sh_event_id =  t2.sh_event_id AND t1.user_api = t2.user_api; 





###  This is the second join

DROP TABLE sandbox_dev_bi.josh_reshape_click_initial_join_ClientSales PURGE;
CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_reshape_click_initial_join_ClientSales
(
event_id                              STRING,
user_api                              STRING,
acc_email                             STRING,
transaction_date                      Date,
Create1                               INT,
Edit1                                 INT,
Save1                                 INT,
last_touched_non_FS_pricer            STRING,
last_touched_FS_pricer                STRING,
last_touched_date_non_FS              DATE,
last_touched_date_FS                  DATE,
total_qty_sold                        INT,
last_sale_date                        Date,
qty_sold_past_week                    INT,
qty_sold_yesterday                    INT
);
set mapred.job.queue.name=root.dev;
INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_reshape_click_initial_join_ClientSales
select 
t1.sh_event_id, t1.user_api, t1.acc_email, t1.transaction_date, t1.Create1, t1.Edit1, t1.Save1,
t1.last_touched_non_FS_pricer, t1.last_touched_FS_pricer, t1.last_touched_date_non_FS, t1.last_touched_date_FS,
t2.total_qty_sold, t2.last_sale_date ,t2.qty_sold_past_week, t2.qty_sold_yesterday 
from sandbox_dev_bi.josh_lastTouch_FS_LS_reshape_join_by_event_click_initial  AS t1
left join  sandbox_dev_bi.josh_ClientSales AS t2
on t1.sh_event_id =  t2.event_id AND t1.user_api = t2.user_api;    



###  This is the third join

DROP TABLE sandbox_dev_bi.josh_reshape_click_initial_ClientSales_join_StubHubSales PURGE;
CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_reshape_click_initial_ClientSales_join_StubHubSales
(
event_id                              STRING,
user_api                              STRING,
acc_email                             STRING,
transaction_date                      Date,
Create1                               INT,
Edit1                                 INT,
Save1                                 INT,
last_touched_non_FS_pricer            STRING,
last_touched_FS_pricer                STRING,
last_touched_date_non_FS              DATE,
last_touched_date_FS                  DATE,
total_qty_sold                        INT,
last_sale_date                        Date,
qty_sold_past_week                    INT,
qty_sold_yesterday                    INT,
qty_sold_on_StubHub_not_accurate      INT
);
set mapred.job.queue.name=root.dev;
INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_reshape_click_initial_ClientSales_join_StubHubSales
select 
t1.event_id, t1.user_api, t1.acc_email, t1.transaction_date, t1.Create1, t1.Edit1, t1.Save1,
t1.last_touched_non_FS_pricer, t1.last_touched_FS_pricer, t1.last_touched_date_non_FS, t1.last_touched_date_FS,
t1.total_qty_sold, t1.last_sale_date ,t1.qty_sold_past_week, t1.qty_sold_yesterday, t2.qty_sold_on_StubHub_not_accurate    
from sandbox_dev_bi.josh_reshape_click_initial_join_ClientSales      AS t1
left join  sandbox_dev_bi.josh_StubHubSales  AS t2
on t1.event_id =  t2.event_id;        



#### prepare for the fourth join

DROP TABLE sandbox_dev_bi.josh_event_colors_with_user_api PURGE;
CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_event_colors_with_user_api
(
id                               BIGINT,
user_id                          INT,
event_id                         STRING,
color_code                       STRING,
start_date                       STRING,
num_of_days                      INT,
end_date                         STRING,
process_date                     DATE,
user_api                         STRING

);
set mapred.job.queue.name=root.dev;
INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_event_colors_with_user_api
select t1.id, t1.user_id , t1.event_id, t1.color_code , t1.start_date, t1.num_of_days, t1.end_date, t1.process_date, t2.user_api
from autopricer_db.event_colors AS t1
left join sandbox_dev_etl_dq.client_information_external AS t2
on t1.user_id = t2.client_id
where process_date= current_date();              



#### This is the fourth join


DROP TABLE sandbox_dev_bi.josh_reshape_click_initial_ClientSales_StubHubSales_color PURGE;
CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_reshape_click_initial_ClientSales_StubHubSales_color
(
event_id                              STRING,
user_api                              STRING,
acc_email                             STRING,
transaction_date                      Date,
Create1                               INT,
Edit1                                 INT,
Save1                                 INT,
last_touched_non_FS_pricer            STRING,
last_touched_FS_pricer                STRING,
last_touched_date_non_FS              DATE,
last_touched_date_FS                  DATE,
total_qty_sold                        INT,
last_sale_date                        Date,
qty_sold_past_week                    INT,
qty_sold_yesterday                    INT,
qty_sold_on_StubHub_not_accurate      INT,
color_code                            STRING
);
set mapred.job.queue.name=root.dev;
INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_reshape_click_initial_ClientSales_StubHubSales_color
select 
t1.event_id, t1.user_api, t1.acc_email, t1.transaction_date, t1.Create1, t1.Edit1, t1.Save1,
t1.last_touched_non_FS_pricer, t1.last_touched_FS_pricer, t1.last_touched_date_non_FS, t1.last_touched_date_FS,
t1.total_qty_sold, t1.last_sale_date ,t1.qty_sold_past_week, t1.qty_sold_yesterday, t1.qty_sold_on_StubHub_not_accurate,t2.color_code   
from sandbox_dev_bi.josh_reshape_click_initial_ClientSales_join_StubHubSales      AS t1
left join  sandbox_dev_bi.josh_event_colors_with_user_api  AS t2
on t1.event_id =  t2.event_id AND t1.user_api = t2.user_api;  



####  This is preparation for a  fifth join




DROP TABLE sandbox_dev_bi.josh_StubHubSupply PURGE;
CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_StubHubSupply
(
event_id                              STRING,
listings_on_StubHub                   INT,
tickets_on_StubHub                    INT,
get_in_price                          DOUBLE

);
set mapred.job.queue.name=root.dev;
INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_StubHubSupply
select eventId, sum(totalListings) AS listings_on_StubHub ,sum(totalTickets) AS tickets_on_StubHub, min(minListPrice) AS get_in_price
from prod_etl_dq.bg_event_tickets
where snapshotdate = date_sub(current_date(), 1)
group by eventId;




DROP TABLE sandbox_dev_bi.josh_StubHubSupply_AWeekAgo PURGE;
CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_StubHubSupply_AWeekAgo
(
event_id                              STRING,
listings_on_StubHub_a_week_ago        INT,
tickets_on_StubHub_a_week_ago         INT,
get_in_price_a_week_ago               DOUBLE

);
set mapred.job.queue.name=root.dev;
INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_StubHubSupply_AWeekAgo
select eventId, sum(totalListings) AS listings_on_StubHub ,sum(totalTickets) AS tickets_on_StubHub, min(minListPrice) AS get_in_price
from prod_etl_dq.bg_event_tickets
where snapshotdate = date_sub(current_date(), 8)
group by eventId;



DROP TABLE sandbox_dev_bi.josh_StubHubSupply_All PURGE;
CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_StubHubSupply_All
(
event_id                              STRING,
listings_on_StubHub                   INT,
tickets_on_StubHub                    INT,
get_in_price                          DOUBLE,
listings_on_StubHub_a_week_ago        INT,
tickets_on_StubHub_a_week_ago         INT,
get_in_price_a_week_ago               DOUBLE

);
set mapred.job.queue.name=root.dev;
INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_StubHubSupply_All
select t1.event_id,t1.listings_on_StubHub,t1.tickets_on_StubHub,t1.get_in_price,
t2.listings_on_StubHub_a_week_ago,t2.tickets_on_StubHub_a_week_ago,t2.get_in_price_a_week_ago
from sandbox_dev_bi.josh_StubHubSupply AS t1
left join sandbox_dev_bi.josh_StubHubSupply_AWeekAgo As t2
on t1.event_id = t2.event_id;

