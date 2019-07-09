SELECT * FROM (WITH Unioned AS (


SELECT 'GDN' as Channel, Week, Date, Spend, Impressions, Clicks, Subscription as Sign_Ups, Log_In, NULL as Email FROM `essence-analytics-dwh.rtf.GDN_Data`

UNION ALL 

SELECT 'Google SEM' as Channel, Week, Date, Spend, Impressions, Clicks, Sign_Ups, NULL as Log_In, Email FROM `essence-analytics-dwh.rtf.Google_SEM_Data`

UNION ALL

SELECT 'Bing SEM' as Channel, Week, Date, Spend, Impressions, Clicks, Sign_Ups, NULL as Log_In, Email FROM `essence-analytics-dwh.rtf.Bing_SEM_Data`

UNION ALL

SELECT 'Brand' as Channel, Week, Date, Spend, Impressions, Clicks, Sign_Ups, NULL as Log_In, NULL as Email FROM `essence-analytics-dwh.rtf.Brand_SEM_Data`

UNION ALL

SELECT 'Non-Brand' as Channel, Week, Date, Spend, Impressions, Clicks,  Sign_Ups, NULL as Log_In, NULL as Email FROM `essence-analytics-dwh.rtf.Non_Brand_SEM_Data`

UNION ALL

SELECT 'Oath Native' as Channel, Week, Date, Spend, Impressions, Clicks, Subscriptions as Sign_Ups, Sign_In as Log_In, NULL as Email FROM `essence-analytics-dwh.rtf.Yahoo_Data`

UNION ALL

SELECT 'PLA' as Channel, Week, Date, Spend, Impressions, Clicks, Sign_Ups,  NULL as Log_In, Email  FROM `essence-analytics-dwh.rtf.PLA_Data`


UNION ALL

SELECT 'Youtube' as Channel, Week, Date, Spend, Impressions, Clicks, Conversions as Sign_Ups,  NULL as Log_In, NULL as Email  FROM `essence-analytics-dwh.rtf.Youtube_Data`

)
select *, 

(CASE WHEN WEEK LIKE '%Q1%' THEN 'Q1'
WHEN WEEK LIKE '%Q2%' THEN 'Q2'
WHEN WEEK LIKE '%Q3%' THEN 'Q3'
WHEN WEEK LIKE '%Q4%' THEN 'Q4'
END) AS Quarter,

(case 
	when SUBSTR(Week,8,10) in (" 1", " 2", " 3", " 4", " 5", " 6", " 7", " 8", " 9")
    then  concat(SUBSTR(week,1,8), "0", SUBSTR(week,9,9))
    ELSE WEEK 
 END) as Weeks_Use,
 
 (case when week like '%Q1 Week 1' then 1
 when week like '%Week 2%' then 2
 when week like '%Week 3%' then 3
 when week like '%Week 4%' then 4
 when week like '%Week 5%' then 5
 when week like '%Week 6%' then 6
 when week like '%Week 7%' then 7
 when week like '%Week 8%' then 8
 when week like '%Week 9%' then 9
 when week like '%10' then 10
 when week like '%Week 11%' then 11
 when week like '%Week 12%' then 12
 when week like '%Week 13%' then 13
 when week like '%Week 14%' then 14
 when week like '%Week 15%' then 15
 end) as week_number,
 

 


ROW_NUMBER() OVER(order by date) AS Quarter_Sorter_Do_Not_Need,


Clicks/NULLIF(Impressions,0) as CTR, Spend/NULLIF(Clicks,0) as CPC, Spend/NULLIF(Sign_Ups,0) as CPA , Sign_Ups/NULLIF(Clicks,0)*100 as CVR, (Sign_Ups/NULLIF(Clicks,0)*100)/Impressions as NRR, ifnull(Log_In,0) + ifnull(EMAIL,0)  AS Site_Log_In  from Unioned)


where Quarter NOT IN ('Q3', 'Q4')
