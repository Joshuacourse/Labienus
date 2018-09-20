/* Some comment here */

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
group by sh_eventid,user_api,acc_type
) t2 on t1.sh_eventid = t2.sh_eventid AND t1.user_api=t2.user_api AND t1.transaction_date = t2.last_touched_date ;