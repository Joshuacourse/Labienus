CREATE TABLE IF NOT EXISTS sandbox_dev_bi.daily_sales_aggregate_overwrite_uptick
(
    unique_id                                  STRING,
    user_api                                   STRING,
    event_id                                   STRING,
    event_name                                 STRING,
    total_tickets_sold                         DOUBLE,
    total_actual_sold_price                    DOUBLE,
    last_invoice_date                          TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sandbox_dev_bi.daily_previous_day_sale_aggregate_overwrite_uptick
(
    unique_id                                  STRING,
    user_api                                   STRING,
    event_id                                   STRING,
    total_tickets_sold_yesterday               DOUBLE,
    total_actual_sold_price_yesterday          DOUBLE,
    yesterday_invoice_date                     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sandbox_dev_bi.daily_past_week_sale_aggregate_overwrite_uptick
(
    unique_id                                  STRING,
    user_api                                   STRING,
    event_id                                   STRING,
    total_tickets_sold_last_week               DOUBLE,
    total_actual_sold_price_last_week          DOUBLE,
    min_last_week_invoice_date                 TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sandbox_dev_bi.sales_data_daily_aggregate_uptick
(
    unique_id                                 STRING,
    user_api                                  STRING,
    event_id                                  STRING,
    total_tickets_sold                        DOUBLE,
    total_actual_sold_price                   DOUBLE,
    last_invoice_date                         TIMESTAMP,
    total_tickets_sold_last_week              DOUBLE,
    total_actual_sold_price_last_week         DOUBLE,
    yesterday_invoice_date                    TIMESTAMP,
    total_tickets_sold_yesterday              DOUBLE,
    total_actual_sold_price_yesterday         DOUBLE,
    min_last_week_invoice_date                TIMESTAMP,
    process_datetime                          TIMESTAMP
);


set mapred.job.queue.name=root.dev;


INSERT OVERWRITE TABLE sandbox_dev_bi.daily_sales_aggregate_overwrite_uptick 
SELECT 
reflect('org.apache.commons.codec.digest.DigestUtils', 'md5Hex', concat(a.user_api,a.bg_eventid)) as unique_id,
a.user_api                                          AS user_api,
a.bg_eventid                                        AS event_id,
b.name                                              AS event_name,
sum(a.ticket_qty)                                   AS total_tickets_sold,
sum(a.actual_sold_price)                            AS total_actual_sold_price,
max(a.invoice_date)                                 AS last_invoice_date 
FROM prod_etl_dq.uptick_user_sales AS a 
JOIN 
prod_etl_dq.events_info_structured              AS b 
ON 
a.bg_eventid=b.event_id 
WHERE 
b.eventdateutc     >= to_utc_timestamp (from_unixtime(unix_timestamp()),'America/New_York') 
AND 
a.list_price       < 20000
AND 
a.list_price       > 0.5
AND
a.order_sold_price           > 0
AND
a.if_brokergenius = true
GROUP BY 
user_api,bg_eventid,name,1;



INSERT OVERWRITE TABLE sandbox_dev_bi.daily_past_week_sale_aggregate_overwrite_uptick
SELECT
reflect('org.apache.commons.codec.digest.DigestUtils', 'md5Hex', concat(a.user_api,a.bg_eventid)) as unique_id,
a.user_api                                          AS user_api,
a.bg_eventid                                        AS event_id,
sum(a.ticket_qty)                                   AS total_tickets_sold_last_week,
sum(a.actual_sold_price)                            AS total_actual_sold_price_last_week,
min(a.invoice_date)                                 AS min_last_week_invoice_date
FROM prod_etl_dq.uptick_user_sales AS a
JOIN 
prod_etl_dq.events_info_structured              AS b
ON a.bg_eventid=b.event_id
WHERE 
b.eventdateutc     >= to_utc_timestamp (from_unixtime(unix_timestamp()),'America/New_York')
AND
a.invoice_date         >=date_add(current_date,-7)
AND
a.list_price           < 20000
AND
a.list_price           > 0.5
AND
a.order_sold_price           > 0
AND
a.if_brokergenius = true
GROUP BY
user_api,bg_eventid,1;



INSERT OVERWRITE TABLE sandbox_dev_bi.daily_previous_day_sale_aggregate_overwrite_uptick
SELECT
reflect('org.apache.commons.codec.digest.DigestUtils', 'md5Hex', concat(a.user_api,a.bg_eventid)) as unique_id,
a.user_api                                          AS user_api,
a.bg_eventid                                        AS event_id,
sum(a.ticket_qty)                                   AS total_tickets_sold_yesterday,
sum(a.actual_sold_price)                            AS total_actual_sold_price_yesterday,
a.invoice_date                                      AS yesterday_invoice_date
FROM prod_etl_dq.uptick_user_sales AS a
JOIN 
prod_etl_dq.events_info_structured              AS b
ON a.bg_eventid=b.event_id
WHERE 
b.eventdateutc     >= to_utc_timestamp (from_unixtime(unix_timestamp()),'America/New_York')
AND
a.invoice_date         >=date_add(current_date,-2)
AND
a.list_price           < 20000
AND
a.list_price           > 0.5
AND
a.order_sold_price           > 0
AND
a.if_brokergenius = true
GROUP BY
user_api,bg_eventid,invoice_date,1;



















INSERT OVERWRITE TABLE sandbox_dev_bi.sales_data_daily_aggregate 
SELECT 
a.unique_id                                                       AS unique_id,
a.user_api                                                        AS user_api,
a.event_id                                                        AS event_id,
a.total_tickets_sold                                              AS total_tickets_sold,
a.total_actual_sold_price                                         AS total_actual_sold_price,
a.last_invoice_date                                               AS last_invoice_date,
b.total_tickets_sold_last_week                                    AS total_tickets_sold_last_week,
b.total_actual_sold_price_last_week                               AS total_actual_sold_price_last_week,
c.yesterday_invoice_date                                          AS yesterday_invoice_date,
c.total_tickets_sold_yesterday                                    AS total_tickets_sold_yesterday,
c.total_actual_sold_price_yesterday                               AS total_actual_sold_price_yesterday,
b.min_last_week_invoice_date                                      AS min_last_week_invoice_date,
from_unixtime(unix_timestamp())                                   AS process_datetime
FROM 
sandbox_dev_bi.daily_sales_aggregate_overwrite                AS a 
LEFT OUTER JOIN 
sandbox_dev_bi.daily_past_week_sale_aggregate_overwrite       AS b 
ON 
a.unique_id=b.unique_id 
LEFT OUTER JOIN  
sandbox_dev_bi.daily_previous_day_sale_aggregate_overwrite    AS c 
ON 
c.unique_id=a.unique_id;













































































































































































































set mapred.job.queue.name=root.dev;



DROP TABLE sandbox_dev_bi.last_process_date_uptick PURGE;


CREATE TABLE IF NOT EXISTS sandbox_dev_bi.last_process_date_uptick
(
last_process_date        DATE
);
set mapred.job.queue.name=root.dev;
INSERT OVERWRITE TABLE  sandbox_dev_bi.last_process_date_uptick
select max(TO_DATE(process_date)) from uptick_db.uptick_listings AS last_process_date;







DROP TABLE sandbox_dev_bi.clientHealthSummaryByEventId_uptick PURGE;


CREATE TABLE IF NOT EXISTS sandbox_dev_bi.clientHealthSummaryByEventId_uptick
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
sum_floor_to_cp_ratio     double,
totalTickets              INT
);



INSERT OVERWRITE TABLE  sandbox_dev_bi.clientHealthSummaryByEventId_uptick
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
                  max(t.update_date)                                              AS latest_update_date,
                  sum(t.floor_to_cp_ratio)                                        AS sum_floor_to_cp_ratio,
                  sum(t.pack)                                                     AS totalTickets
                  
                  from 
                  (
                  select 
                  active                                                                 AS activeOrSold,
                  on_floor                                                               AS price_floor,
                  datediff(TO_DATE(process_date), TO_DATE(criteria_updated_at))          AS stale_days,
                  TO_DATE(criteria_updated_at)                                           AS update_date,
                  TO_DATE(event_date)                                                    AS eventdate,
                  NULL                                                                   AS name,
                  performer_info                                                         AS act_primary,
                  NULL                                                                   AS act_secondary,                                    
                  TO_DATE(process_date)                                                  AS process_date,
                  primary_category                                                       AS category,
                  sub_category                                                           AS subcategory,
                  account_id                                                             AS user_id,
                  bg_event_id                                                            AS event_id,
                  venue_name                                                             AS venue_name,
                  floor                                                                  AS floor,
                  splits                                                                 AS pack,
                  push_price                                                             AS actual_price,
                  cp                                                                     AS changed_price,
                  case 
                  when on_floor = 1     then (floor/cp)
                  else                          0           end                          AS floor_to_cp_ratio
                  
                  from 
                  uptick_db.uptick_listings  t1
                  join (select * from sandbox_dev_bi.last_process_date_uptick) t2
                  on t1.process_date = t2.last_process_date
                  where TO_DATE(t1.event_date)>=TO_DATE(t1.process_date)
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





DROP TABLE sandbox_dev_bi.clientHealthSummaryByEventId_totalListingsTotalTickets_uptick PURGE;


CREATE TABLE IF NOT EXISTS sandbox_dev_bi.clientHealthSummaryByEventId_totalListingsTotalTickets_uptick
(
eventdate                 STRING, 
process_date              STRING, 
category                  STRING,
subcategory               STRING,
user_id                   STRING,
event_id                  STRING,
event_name                STRING,
primary_performer         STRING,
secondary_performer       STRING,
venue_name                STRING,
TotalListings             INT,
totalTickets              INT
);


INSERT OVERWRITE TABLE  sandbox_dev_bi.clientHealthSummaryByEventId_totalListingsTotalTickets_uptick
select 
                  t1.eventdate                                                     AS eventdate, 
                  t1.process_date                                                  AS process_date, 
                  t1.category                                                      AS category,
                  t1.subcategory                                                   AS subcategory,
                  t1.user_id                                                       AS user_id,
                  t1.event_id                                                      AS event_id,
                  t1.event_name                                                    AS event_name,
                  t1.primary_performer                                             AS primary_performer,
                  t1.secondary_performer                                           AS secondary_performer,
                  t1.venue_name                                                    AS venue_name,
                  sum(t1.totallistings)                                            AS TotalListings,
                  sum(t1.totalTickets)                                             AS totalTickets
                  from 
                  sandbox_dev_bi.clientHealthSummaryByEventId_uptick  t1
                  GROUP BY
                  eventdate,
                  process_date,
                  category,
                  subcategory,
                  user_id,
                  event_id,
                  event_name,
                  primary_performer,
                  secondary_performer,
                  venue_name
;




DROP TABLE sandbox_dev_bi.clientHealthSummaryByEventId_v3_uptick PURGE;


CREATE TABLE IF NOT EXISTS sandbox_dev_bi.clientHealthSummaryByEventId_v3_uptick
(
eventdate                 STRING, 
process_date              STRING, 
category                  STRING,
subcategory               STRING,
user_id                   STRING,
event_id                  STRING,
event_name                STRING,
primary_performer         STRING,
secondary_performer       STRING,
venue_name                STRING,
TotalListings             INT,
totalTickets              INT,
TotalActiveListings       INT,
Listings_at_floor         INT,
avg_stale_days            double,
oldest_update_date        DATE,
latest_update_date        DATE,
avg_floor_to_cp_ratio_for_listings_at_floor  double,
totalActiveTickets        INT
);


INSERT OVERWRITE TABLE  sandbox_dev_bi.clientHealthSummaryByEventId_v3_uptick
select 
                  t1.eventdate                                                     AS eventdate, 
                  t1.process_date                                                  AS process_date, 
                  t1.category                                                      AS category,
                  t1.subcategory                                                   AS subcategory,
                  t1.user_id                                                       AS user_id,
                  t1.event_id                                                      AS event_id,
                  t1.event_name                                                    AS event_name,
                  t1.primary_performer                                             AS primary_performer,
                  t1.secondary_performer                                           AS secondary_performer,
                  t1.venue_name                                                    AS venue_name,
                  t1.TotalListings                                                 AS TotalListings,
                  t1.totalTickets                                                  AS totalTickets,
                  t2.TotalActiveListings                                           AS TotalActiveListings,
                  t2.Listings_at_floor                                             AS Listings_at_floor, 
                  t2.avg_stale_days                                                AS avg_stale_days,
                  t2.oldest_update_date                                            AS oldest_update_date,
                  t2.latest_update_date                                            AS latest_update_date,
                  t2.avg_floor_to_cp_ratio_for_listings_at_floor                   AS avg_floor_to_cp_ratio_for_listings_at_floor,
                  t2.totalActiveTickets                                            AS totalActiveTickets
                  from 
                  sandbox_dev_bi.clientHealthSummaryByEventId_totalListingsTotalTickets_uptick  t1
                  join (
                        select 
                          process_date                                                  AS process_date,
                          user_id                                                       AS user_id,
                          event_id                                                      AS event_id,
                          TotalListings                                                 AS TotalActiveListings,
                          Listings_at_floor                                             AS Listings_at_floor, 
                          total_stale_days/TotalListings                                AS avg_stale_days,
                          oldest_update_date                                            AS oldest_update_date,
                          latest_update_date                                            AS latest_update_date,
                          sum_floor_to_cp_ratio/Listings_at_floor                       AS avg_floor_to_cp_ratio_for_listings_at_floor,
                          totalTickets                                                  AS totalActiveTickets
                        from 
                        sandbox_dev_bi.clientHealthSummaryByEventId_uptick where activeorsold >0 and totallistings>0 
                        ) t2
                  on  t1.process_date = t2.process_date
                  and t1.user_id      = t2.user_id
                  and t1.event_id     = t2.event_id
;












