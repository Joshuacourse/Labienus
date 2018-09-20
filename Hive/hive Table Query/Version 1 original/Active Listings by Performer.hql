DROP TABLE sandbox_dev_bi.josh_clientHealthSummaryByEventId PURGE;


CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_clientHealthSummaryByEventId
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
latest_update_date        DATE
);

set mapred.job.queue.name=root.dev;

INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_clientHealthSummaryByEventId
select 
                  t.eventdate                                                     AS eventdate, 
                  t.process_date                                                  AS process_date, 
                  t.category                                                      AS category,
                  t.subcategory                                                   AS subcategory,
                  t.user_id                                                       AS user_id,
                  t.activeOrSold                                                  AS activeOrSold,
                  t.event_id                                                      AS event_id,
                  t.name                                                          AS event_name,
                  t.act_primary                                                   AS primary_performer,
                  t.act_secondary                                                 AS secondary_performer,
                  t.venue_name                                                    AS venue_name,
                  count(t.price_floor)                                            AS TotalListings,
                  sum(t.price_floor)                                              AS Listings_at_floor, 
                  sum(t.stale_days)                                               AS total_stale_days,
                  min(t.update_date)                                              AS oldest_update_date,
                  max(t.update_date)                                              AS latest_update_date
                  
                  from 
                  (
                  select 
                  case
                  WHEN status = 1 OR manual_group_status = 1 THEN 1
                  ELSE                                            0   END                AS activeOrSold,
                  price_floor                                                            AS price_floor,
                  datediff(TO_DATE(process_date), TO_DATE(FROM_UNIXTIME(last_update)))   AS stale_days,
                  TO_DATE(FROM_UNIXTIME(last_update))                                    AS update_date,
                  TO_DATE(eventdatelocal)                                                AS eventdate,
                  name                                                                   AS name,
                  act_primary                                                            AS act_primary,
                  act_secondary                                                          AS act_secondary,                                    
                  TO_DATE(process_date)                                                  AS process_date,
                  category                                                               AS category,
                  subcategory                                                            AS subcategory,
                  user_id                                                                AS user_id,
                  sh_eventid                                                             AS event_id,
                  venue_name                                                             AS venue_name
                  
                  from 
                  autopricer_db.listing_criteria 
                  where TO_DATE(eventdatelocal)>=TO_DATE(process_date) OR ISNULL (eventdatelocal)
                  ) t
                  GROUP BY
                  eventdate,
                  process_date,
                  category,
                  subcategory,
                  activeOrSold,
                  user_id,
                  event_id,
                  name,
                  act_primary,
                  act_secondary,
                  venue_name
;



DROP TABLE sandbox_dev_bi.josh_last_process_date PURGE;


CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_last_process_date
(
last_process_date        DATE
);
set mapred.job.queue.name=root.dev;
INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_last_process_date
select max(TO_DATE(process_date)) from sandbox_dev_bi.josh_clientHealthSummaryByEventId AS last_process_date;







DROP TABLE sandbox_dev_bi.josh_clientHealthSummaryByEventIdLatestDate PURGE;


CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_clientHealthSummaryByEventIdLatestDate
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
latest_update_date        DATE
);

set mapred.job.queue.name=root.dev;

INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_clientHealthSummaryByEventIdLatestDate
select 
eventdate                 , 
process_date              , 
category                  ,
subcategory               ,
user_id                   ,
activeOrSold              ,
event_id                  ,
event_name                ,
primary_performer         ,
secondary_performer       ,
venue_name                ,
TotalListings             ,
Listings_at_floor         , 
total_stale_days          ,
oldest_update_date        ,
latest_update_date        

from sandbox_dev_bi.josh_clientHealthSummaryByEventId t1
join (select * from sandbox_dev_bi.josh_last_process_date) t2
on t1.process_date = t2.last_process_date
;




DROP TABLE sandbox_dev_bi.josh_stubhub_events_info_structured PURGE;


CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_stubhub_events_info_structured
(
act_primary        STRING,
event_id           STRING,
name               STRING,
venue_name         STRING,
category           STRING
);

set mapred.job.queue.name=root.dev;

INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_stubhub_events_info_structured
select 
    case when act_primary like '' then name
         else act_primary
    end  as    act_primary      ,
event_id                        ,
name                            ,
venue_name                      ,
category

from sandbox_dev_etl_dq.stubhub_events_info_structured;












DROP TABLE sandbox_dev_bi.josh_complete_clientHealthSummaryByEventIdLatestDate PURGE;


CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_complete_clientHealthSummaryByEventIdLatestDate
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
latest_update_date        DATE
);


set mapred.job.queue.name=root.dev;

INSERT OVERWRITE TABLE  sandbox_dev_bi.josh_complete_clientHealthSummaryByEventIdLatestDate
select 
t1.eventdate                 , 
t1.process_date              , 
t2.category                  ,
t1.subcategory               ,
t1.user_id                   ,
t1.activeOrSold              ,
t1.event_id                  ,
t1.event_name                ,
t2.act_primary             AS primary_performer         ,
t1.secondary_performer       ,
t2.venue_name                ,
t1.TotalListings             ,
t1.Listings_at_floor         , 
t1.total_stale_days          ,
t1.oldest_update_date        ,
t1.latest_update_date        

from sandbox_dev_bi.josh_clientHealthSummaryByEventIdLatestDate t1
join (select * from sandbox_dev_bi.josh_stubhub_events_info_structured) t2
on t1.event_id = t2.event_id 
;








add jar /home/awais_dev/elasticsearch-hadoop-5.3.0/dist/elasticsearch-hadoop-5.3.0.jar;
set mapred.job.queue.name=root.dev;
DROP TABLE sandbox_dev_bi.josh_logs_eventid_api_transation PURGE;

CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_logs_eventid_api_transation
(                         
sh_eventid         STRING,
user_api           STRING,
transaction_date   TIMESTAMP
) 
;

set mapred.job.queue.name=root.dev;

insert OVERWRITE TABLE sandbox_dev_bi.josh_logs_eventid_api_transation

SELECT
sh_eventid                      ,
user_api                        ,
Max(transaction_date)   as  transaction_date               

from 
sandbox_dev_etl_dq.client_logs 
group by 
sh_eventid, user_api
;





DROP TABLE sandbox_dev_bi.josh_logs_remove_duplicated PURGE;

CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_logs_remove_duplicated
(                         
sh_eventid         STRING,
user_api           STRING,
acc_type           STRING,
transaction_date   TIMESTAMP
) 
;


set mapred.job.queue.name=root.dev;
insert OVERWRITE TABLE sandbox_dev_bi.josh_logs_remove_duplicated
SELECT
sh_eventid                 ,
user_api                   ,
acc_type                   ,
transaction_date                      
from 
sandbox_dev_etl_dq.client_logs
group by
sh_eventid,user_api,acc_type,transaction_date;







DROP TABLE sandbox_dev_bi.josh_logs_eventid_api_transation_acc_type PURGE;

CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_logs_eventid_api_transation_acc_type
(                         
sh_eventid         STRING,
user_api           STRING,
acc_type           STRING,
transaction_date   TIMESTAMP
) 
;


set mapred.job.queue.name=root.dev;
insert OVERWRITE TABLE sandbox_dev_bi.josh_logs_eventid_api_transation_acc_type
SELECT
t1.sh_eventid              ,
t1.user_api                ,
t2.acc_type                ,
t1.transaction_date        
from 
sandbox_dev_bi.josh_logs_eventid_api_transation as t1
left join 
sandbox_dev_bi.josh_logs_remove_duplicated as t2
on t1.sh_eventid = t2.sh_eventid
AND 
t1.user_api = t2.user_api 
AND
t1.transaction_date = t2.transaction_date;









DROP TABLE sandbox_dev_bi.modefiedClientHealthSummaryByEventIdLatestDate PURGE;





CREATE TABLE IF NOT EXISTS sandbox_dev_bi.modefiedClientHealthSummaryByEventIdLatestDate
(                         
primary_performer         STRING,
totallistings             INT,
venue_name                STRING,
user_id                   STRING,
event_id                  STRING,
category                  STRING
) 
;

set mapred.job.queue.name=root.dev;


insert OVERWRITE TABLE sandbox_dev_bi.modefiedClientHealthSummaryByEventIdLatestDate

SELECT
    case when primary_performer like '' then event_name
         else primary_performer
    end  as    primary_performer,
totallistings                   ,
venue_name                      ,
user_id                         ,
event_id                        ,
category

from 
sandbox_dev_bi.josh_complete_clientHealthSummaryByEventIdLatestDate 
where activeOrSold = "1"
;




DROP TABLE sandbox_dev_bi.josh_logs_join_client PURGE;

CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_logs_join_client
(                         
sh_eventid         STRING,
user_api           STRING,
acc_type           STRING,
transaction_date   TIMESTAMP,
company_name       STRING,
client_id          STRING
) 
;

set mapred.job.queue.name=root.dev;
insert OVERWRITE TABLE sandbox_dev_bi.josh_logs_join_client
SELECT
t1.sh_eventid              ,
t1.user_api                ,
t1.acc_type                ,
t1.transaction_date        ,
t2.company_name            ,
t2.client_id

         
from 
sandbox_dev_bi.josh_logs_eventid_api_transation_acc_type as t1
left join 
sandbox_dev_etl_dq.client_information_external  as t2
on t1.user_api = t2.user_api
;



DROP TABLE sandbox_dev_bi.josh_logs_join_client_join_activeListing PURGE;

CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_logs_join_client_join_activeListing
(                         
primary_performer         STRING,
venue_name                STRING,
company_name              STRING,
acc_type                  STRING,
category                  STRING,
totalEvents               INT,
totallistings             INT
) 
;


set mapred.job.queue.name=root.dev;
insert OVERWRITE TABLE sandbox_dev_bi.josh_logs_join_client_join_activeListing
SELECT

t1.primary_performer    ,
t1.venue_name           ,
t2.company_name         ,
t2.acc_type             ,
t1.category             ,
count(*)          as  totalEvents,
sum(totallistings) as  totallistings



from 
sandbox_dev_bi.modefiedClientHealthSummaryByEventIdLatestDate as t1
left join sandbox_dev_bi.josh_logs_join_client as t2
on t1.event_id  = t2.sh_eventid
AND t1.user_id = t2.client_id
group by 
t1.primary_performer, t1.venue_name,t1.category, t2.company_name,t2.acc_type;


