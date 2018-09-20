DROP TABLE sandbox_dev_bi.josh_modefiedClientHealth_eventdatelocal PURGE;

CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_modefiedClientHealth_eventdatelocal
(                         
totallistings             INT,
user_id                   STRING,
event_id                  STRING,
eventdatelocal            STRING
) 
;


set mapred.job.queue.name=root.dev;
insert OVERWRITE TABLE sandbox_dev_bi.josh_modefiedClientHealth_eventdatelocal
SELECT

t1.totallistings                ,
t1.user_id                      ,
t1.event_id                     ,
t2.eventdatelocal               



from 
sandbox_dev_bi.josh_clientHealthSummaryByEventIdLatestDate as t1
left join sandbox_dev_etl_dq.stubhub_events_info_all as t2
on t1.event_id  = t2.id;










DROP TABLE sandbox_dev_bi.josh_active_listings_by_date PURGE;

CREATE TABLE IF NOT EXISTS sandbox_dev_bi.josh_active_listings_by_date
(      

company_name              STRING,
acc_type                  STRING,
eventdatelocal            STRING,
event_id                  STRING,
totalEvents               INT,
totallistings             INT

) 
;


set mapred.job.queue.name=root.dev;
insert OVERWRITE TABLE sandbox_dev_bi.josh_active_listings_by_date
SELECT

t2.company_name         ,
t2.acc_type             ,
t1.eventdatelocal       ,
t1.event_id              ,
count(*)          as  totalEvents,
sum(totallistings) as  totallistings



from 
sandbox_dev_bi.josh_modefiedClientHealth_eventdatelocal as t1
left join sandbox_dev_bi.josh_logs_join_client as t2
on t1.event_id  = t2.sh_eventid
AND t1.user_id = t2.client_id
group by 
t1.eventdatelocal,t1.event_id, t2.company_name,t2.acc_type;

