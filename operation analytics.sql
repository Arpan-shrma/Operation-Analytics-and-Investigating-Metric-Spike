-- Selecting dataset schemas
use Operation_Analytics_and_Investigating_Metric_Spike;

-- Datatable for Casestudy 1
select *
FROM job_data;

-- Datatable for Casestudy 2
-- Table 1
select *
from events;
-- Table 2
select *
from email_events;
-- Table 3
select *
from users;

-- Case Study 1 (Job Data):
-- 1. The number of jobs reviewed per hour per day for November 2020
SELECT ds,
COUNT(DISTINCT job_id)*3600/SUM(time_spent)
AS daily_throughput FROM
job_data
WHERE
ds BETWEEN '2020-11-01' AND '2020-11-30' GROUP BY ds;

-- 2. 7-day rolling average of throughput
SELECT ds,
AVG(jobs_reviewed)OVER(ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS weekly_throughput FROM
(
SELECT ds, COUNT(distinct job_id) AS jobs_reviewed
FROM job_data
WHERE ds BETWEEN '2020-11-01' AND '2020-11-30' GROUP BY ds
ORDER BY ds
)a;

-- 3. Percentage share of each language for different contents
SELECT language, num_jobs,
100.0* num_jobs/total_jobs AS pct_share_jobs FROM
( SELECT language,
COUNT(job_id) AS num_jobs FROM job_data
GROUP BY language)a CROSS JOIN (
SELECT COUNT(job_id) AS total_jobs FROM job_data)b;

-- 4. Duplicate rows
-- Rows that have the same value present in them with respect to all columns 
WITH alias AS (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY ds, job_id, actors_id, event, language, time_spent, org) AS rownum
FROM temp)
SELECT * FROM alias
WHERE rownum > 1;

-- Rows that have the same value present in them with respect to job_id column
WITH alias AS (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY job_id) AS rownum
FROM temp)
SELECT * FROM alias
WHERE rownum > 1;

-- Rows that have the same value present in them with respect to ds column
WITH alias AS (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY ds) AS rownum
FROM temp)
SELECT * FROM alias
WHERE rownum > 1;

-- Rows that have the same value present in them with respect to language column
WITH alias AS (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY language) AS rownum
FROM temp)
SELECT * FROM alias
WHERE rownum > 1;

-- B) Case Study 2 (Investigating metric spike):
-- 1. Calculate the weekly user engagement
SELECT
EXTRACT(WEEK FROM occurred_at) AS weeknumber, COUNT(distinct user_id) AS no_of_active_users
FROM events
WHERE event_type = 'engagement'
GROUP BY weeknumber;

-- 2. Amount of users growing over time for a product
SELECT year, week_number,
active_users_per_week,
SUM(active_users_per_week) OVER(ORDER BY year, week_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS active_users_growth,
MIN(active_users_per_week) OVER(), MAX(active_users_per_week) OVER(), AVG(active_users_per_week) OVER(), COUNT(week_number) OVER()
FROM
(SELECT
EXTRACT(year from a.activated_at) AS year, EXTRACT(week from a.activated_at) AS week_number, COUNT(distinct user_id) AS active_users_per_week FROM users a
WHERE state='active'
GROUP BY year, week_number ORDER BY year, week_number )a;

-- 3. Users getting retained weekly after signing-up for a product
SELECT EXTRACT(WEEK FROM a.occurred_at) AS "week", AVG(a.age_at_event) AS "Average age during week",
COUNT(DISTINCT CASE WHEN a.user_age < 7 THEN a.user_age END) AS "less than a week",
COUNT(DISTINCT CASE WHEN a.user_age < 14 AND a.user_age >=7 THEN a.user_id ELSE NULL END) AS "1 week",
COUNT(DISTINCT CASE WHEN a.user_age < 21 AND a.user_age >=14 THEN a.user_id ELSE NULL END) AS "2 week",
COUNT(DISTINCT CASE WHEN a.user_age < 28 AND a.user_age >=21 THEN a.user_id ELSE NULL END) AS "3 week",
COUNT(DISTINCT CASE WHEN a.user_age < 35 AND a.user_age >=28 THEN a.user_id ELSE NULL END) AS "4 week",
COUNT(DISTINCT CASE WHEN a.user_age < 42 AND a.user_age >=35 THEN a.user_id ELSE NULL END) AS "5 week",
COUNT(DISTINCT CASE WHEN a.user_age < 49 AND a.user_age >=42 THEN a.user_id ELSE NULL END) AS "6 week",
COUNT(DISTINCT CASE WHEN a.user_age < 56 AND a.user_age >=49 THEN a.user_id ELSE NULL END) AS "7 week",
COUNT(DISTINCT CASE WHEN a.user_age < 63 AND a.user_age >=56 THEN a.user_id ELSE NULL END) AS "8 week",
COUNT(DISTINCT CASE WHEN a.user_age < 70 AND a.user_age >=63 THEN a.user_id ELSE NULL END) AS "9 week",
COUNT(DISTINCT CASE WHEN a.user_age >70 THEN a.user_id ELSE NULL END) AS "10+ week"
FROM (SELECT e.occurred_at, u.user_id, EXTRACT(WEEK FROM u.activated_at) AS activation_week,
DATEDIFF(e.occurred_at,u.activated_at) AS age_at_event, DATEDIFF('2014-09-01',u.activated_at) AS user_age
FROM users u JOIN events e
ON e.user_id = u.user_id AND e.event_type = 'engagement'
AND e.event_name= 'login' AND e.occurred_at >= '2014-05-01' AND e.occurred_at < '2014-09-01' WHERE u.activated_at IS NOT NULL ) a
GROUP BY 1
ORDER BY 1 ;

-- 4. the weekly engagement per device
SELECT distinct device, ROUND(AVG(no_of_users)OVER(PARTITION BY device),0) AS avg_active_users_per_week
FROM
( SELECT
EXTRACT(year from occurred_at) AS year,
EXTRACT(week from occurred_at) AS week_no,
device,
COUNT(distinct user_id) AS no_of_users
FROM events
WHERE event_type = 'engagement' GROUP BY 1,2,3
ORDER BY 3)a;

-- 5. the email engagement metrics
SELECT EXTRACT(YEAR FROM occurred_at) AS Year,
EXTRACT(WEEK FROM occurred_at) AS Week,
COUNT(CASE WHEN e.action = 'sent_weekly_digest' THEN e.user_id ELSE NULL END) AS weeklydigest_emails,
COUNT(CASE WHEN e.action = 'sent_reengagement_email' THEN e.user_id ELSE NULL END) AS reengagement_emails,
COUNT(CASE WHEN e.action = 'email_open' THEN e.user_id ELSE NULL END) AS email_opens,
COUNT(CASE WHEN e.action = 'email_clickthrough' THEN e.user_id ELSE NULL END) AS email_clickthroughs
FROM email_events e
GROUP BY 1,2;
