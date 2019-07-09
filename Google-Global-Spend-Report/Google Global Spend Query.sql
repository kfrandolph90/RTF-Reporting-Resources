(WITH CTE AS (

---- I do not use SELECT * because of issues I was running into because of the nature of CTEs. 
 SELECT Olive__Plan_Region, Olive__Campaign_Name, Olive_Plan_Brand_DR, Olive_Plan_Name, Olive_Plan_Line_Name, Olive__Plan_Line_Property, MRF_Product_Name, Olive__IO_Platform, Time_2__Month_of_Activity, MRF_MRF_ID, Olive_Campaign_Managing_Region, Segmentations_Country, Performance_And_Delivery_3__Spend_in_Client_currency , 

---- For the following condition, the created column 'Region_Plan_Region' should omit cells where Olive__Plan_Region = 'Global'
(CASE WHEN Olive__Plan_Region = 'Global' THEN NULL ELSE Olive__Plan_Region END) AS Region_Plan_Region, 
     
---- For the following conditions, Olive_Plan_Name should be transformed into regions on at the plan level. Specifically, 'Global' should yield 'Global', 'EMEA' should yield 'EMEA', 'APAC' should yield 'APAC', ('NA', 'US', 'CA', North America', 'Canada', 'United States') should yield 'NA', and 'LATAM' should yield 'LATAM'      
(CASE WHEN Olive_Plan_Line_Name LIKE '%Global%' THEN 'Global' 
      WHEN Olive_Plan_Line_Name LIKE '%EMEA%' THEN 'EMEA'
      WHEN Olive_Plan_Line_Name LIKE '%APAC%' THEN 'APAC'
      WHEN Olive_Plan_Line_Name LIKE '%LATAM%' THEN 'LATAM' 
      WHEN Olive_Plan_Line_Name LIKE '%NA%' OR Olive_Plan_Line_Name LIKE '%US%' OR Olive_Plan_Line_Name LIKE '%Canada%' OR  Olive_Plan_Line_Name LIKE '%United States%' OR Olive_Plan_Line_Name LIKE '%North America%' OR Olive_Plan_Line_Name LIKE '%CA%' THEN 'NA'
END) AS Region_Plan_Line_Name,
 
---- For the following conditions, the previous conditions are executed on Olive_Plan_Name column to create 'Region_Plan_Name'     
 (CASE WHEN Olive_Plan_Name LIKE '%Global%' THEN 'Global' 
      WHEN Olive_Plan_Name LIKE '%EMEA%' THEN 'EMEA'
      WHEN Olive_Plan_Name LIKE '%APAC%' THEN 'APAC'
      WHEN Olive_Plan_Name LIKE '%LATAM%' THEN 'LATAM' 
      WHEN Olive_Plan_Name LIKE '%NA%' OR Olive_Plan_Name LIKE '%US%' OR Olive_Plan_Name LIKE '%Canada%' OR Olive_Plan_Name LIKE '%United States%' OR Olive_Plan_Name LIKE '%North America%' OR Olive_Plan_Name LIKE '%CA%' THEN 'NA'                             
END) AS Region_Plan_Name ,

---- For the following conditions, the previous conditions are executed on Olive_Campaign_Name column to create 'Region_Campaign_Name' 
(CASE WHEN Olive__Campaign_Name LIKE '%Global%' THEN 'Global'
      WHEN Olive__Campaign_Name LIKE '%EMEA%' THEN 'EMEA'
      WHEN Olive__Campaign_Name LIKE '%APAC%' THEN 'APAC'
      WHEN Olive__Campaign_Name LIKE '%LATAM%' THEN 'LATAM' 
      WHEN Olive__Campaign_Name LIKE '%NA%' OR Olive_Plan_Line_Name LIKE '%US%' OR Olive_Plan_Line_Name LIKE '%Canada%' OR Olive_Plan_Line_Name LIKE '%United States%' OR Olive_Plan_Line_Name LIKE '%North America%' OR Olive_Plan_Line_Name LIKE '%CA%' THEN 'NA' 
END) AS Region_Campaign_Name, 
   
---- For the following conditions, Olive_Campaign_Name should be transformed into google product names. Specifically, the following product names should be extracted from Olive_Campaign_Name: Pixel, Chromebook - change to Chrome, Google Play - change to Play Store, Google play - change to Play Store, Play Store, Project Fi, Google Home, Duo, Nest,  Multi-App, Google Search App - change to GSA, Google Store, GSA - change to GSA, GStore - change to Google Store???, Google Search - change to GSA, User Education, Google Wallet, Google App,  Google Brand, Google Fiber, GApp - change to Google App, Google Express,  Google Agency Test Campaign - NULL, Android One - Android
 (CASE WHEN Olive__Campaign_Name LIKE '%YouTube%' THEN 'YouTube' 
      WHEN Olive__Campaign_Name LIKE '%Pixel%' THEN 'Pixel' 
      WHEN Olive__Campaign_Name LIKE '%Chromebook%' THEN 'Chrome' 
      WHEN Olive__Campaign_Name LIKE '%Google Play%' THEN 'Play Store'
      WHEN Olive__Campaign_Name LIKE '%Google play%' THEN 'Play Store'
      WHEN Olive__Campaign_Name LIKE '%Play Store%' THEN 'Play Store' 
      WHEN Olive__Campaign_Name LIKE '%Project Fi%' THEN 'Project Fi'
      WHEN Olive__Campaign_Name LIKE '%Google Home%' THEN 'Google Home'
      WHEN Olive__Campaign_Name LIKE '%Duo%' THEN 'Duo'
      WHEN Olive__Campaign_Name LIKE '%Nest%' THEN 'Nest'
      WHEN Olive__Campaign_Name LIKE '%Multi-App%' THEN 'Multi-App' 
      WHEN Olive__Campaign_Name LIKE '%Google Search App%' THEN 'Google Search App'
      WHEN Olive__Campaign_Name LIKE '%Google Store%' THEN 'GSA'
      WHEN Olive__Campaign_Name LIKE '%GSA%' THEN 'GSA'
      WHEN Olive__Campaign_Name LIKE '%User Education%' THEN 'User Education'
      WHEN Olive__Campaign_Name LIKE '%Google Wallet%' THEN 'Google Wallet' 
      WHEN Olive__Campaign_Name LIKE '%Google App%' THEN 'Google App'
      WHEN Olive__Campaign_Name LIKE '%GApp%' THEN 'Google App'
      WHEN Olive__Campaign_Name LIKE '%GStore%' THEN 'GSA' 
  # Asked Chris a question here about 'GStore' which is set to yield to 'Google Store' but in other places 'Google Store' yields to 'GSA' -- ## RESOLVED IT WITH CHRIS, it indeed needed to be converted to GSA 
      WHEN Olive__Campaign_Name LIKE '%Google Brand%' THEN 'Google Brand'
      WHEN Olive__Campaign_Name LIKE '%Google Fiber%' THEN 'Google Fiber' 
      WHEN Olive__Campaign_Name LIKE '%Google Express%' THEN 'Google Express' 
      WHEN Olive__Campaign_Name LIKE '%Google Agency Test Campaign' THEN NULL 
      WHEN Olive__Campaign_Name LIKE '%Android One%' THEN 'Android'
      WHEN Olive__Campaign_Name LIKE '%Stadia%' THEN 'Stadia'
      ELSE NULL 
END) AS Product_Campaign_Name, 
     
`essence-analytics-dwh.rtf_global_spend_report.MRF_PK`.string_field_1 as Product_Data_Validation_Tab, 
`essence-analytics-dwh.rtf_global_spend_report.Property_Metadata`.string_field_1 as Property,
`essence-analytics-dwh.rtf_global_spend_report.Platform_Metadata`.string_field_1 as Platform
     
FROM `essence-analytics-dwh.rtf_global_spend_report.global_spend_report_raw_data`
     
---- I use LEFT JOINs here to add 'data validation' columns. For these, I manually uploaded the tables to use in the LEFT JOIN to BigQuery. A longer-term solution would be to somehow allow the tables to be dynamically updated because with this setup I will need to RE-upload new metadata tables to accomodate new classifications. 
LEFT JOIN `essence-analytics-dwh.rtf_global_spend_report.MRF_PK` ON `essence-analytics-dwh.rtf_global_spend_report.global_spend_report_raw_data`.MRF_Product_Name = `essence-analytics-dwh.rtf_global_spend_report.MRF_PK`.string_field_0 
LEFT JOIN `essence-analytics-dwh.rtf_global_spend_report.Property_Metadata` ON `essence-analytics-dwh.rtf_global_spend_report.global_spend_report_raw_data`.Olive__Plan_Line_Property = `essence-analytics-dwh.rtf_global_spend_report.Property_Metadata`.string_field_0 
LEFT JOIN `essence-analytics-dwh.rtf_global_spend_report.Platform_Metadata` ON `essence-analytics-dwh.rtf_global_spend_report.global_spend_report_raw_data`.Olive__IO_Platform = `essence-analytics-dwh.rtf_global_spend_report.Platform_Metadata`.string_field_0 ), 
     
CTE2 AS (
---- If Region_Plan_Name and Region_Campaign_Name are both Global, the following column should yield 'Global - Managed by APAC' 
---- If Region_Plan_Name is 'Global' and Region_Campaign_Name is [some_region], the following column should yield 'Global - Managed by [some_region]' 
SELECT *, 
(CASE WHEN Region_Plan_Region IS NOT NULL THEN Region_Plan_Region
      WHEN Region_Plan_Name IS NOT NULL THEN Region_Plan_Name
      WHEN Region_Campaign_Name IS NOT NULL THEN Region_Campaign_Name
END) as Region_Prep,

---- This is the Product_Final column -- it's only adding a little bit of data but it's still important.
(CASE WHEN Product_Data_Validation_Tab IS NOT NULL THEN Product_Data_Validation_Tab
      WHEN Product_Campaign_Name IS NOT NULL THEN Product_Campaign_Name ELSE NULL 
END) AS Product_Final,
---- Adding Pubs_Final          
(CASE WHEN Property LIKE '%DBM%' THEN 'DV360'
      WHEN Property LIKE '%Bing Ads%' THEN 'Bing'
      WHEN Property LIKE '%Yahoo Inc.%' THEN 'Yahoo Search'
      WHEN Property LIKE '%Yahoo Search%' THEN 'Yahoo Search'
      WHEN Property LIKE '%Google Search%' THEN 'Google Search'
      WHEN Property LIKE '%GDN%' THEN 'GDN'
      WHEN Property LIKE '%UAC%' THEN 'UAC'
      WHEN Property LIKE '%DV360%' THEN 'DV360'
      WHEN Property LIKE '%YouTube%' THEN 'YouTube'
      WHEN Property LIKE '%Twitter%' THEN 'Twitter'
      WHEN Property LIKE '%Facebook%' THEN 'Facebook'
      WHEN Property LIKE '%Instagram%' THEN 'Instagram'
      WHEN Property LIKE '%Snapchat%' THEN 'Snapchat'
      WHEN Property LIKE '%Criteo%' THEN 'Criteo'
      ELSE Platform 
END) AS Pubs_Final 
                  
FROM CTE),
                  
                  
CTE3 AS ( 
SELECT *, 

`essence-analytics-dwh.rtf_global_spend_report.Platforms_Rolled_Up_Metadata`.string_field_1 as Platforms_Rolled_Up, 

---- Still preparing region in stages because the logic was complicated. 
(CASE WHEN Region_Prep = 'Global' THEN CONCAT(Region_Prep,' - ',Olive_Campaign_Managing_Region)
      WHEN Region_Prep != 'Global' and Region_Plan_Region IS NOT NULL THEN Region_Plan_Region
      WHEN Region_Prep != 'Global' and Region_Plan_Name IS NOT NULL THEN Region_Plan_Name
      WHEN Region_Prep != 'Global' and Region_Campaign_Name IS NOT NULL THEN Region_Campaign_Name
END) AS Region_Prep_2,

---- Finalizing Pub_Rollup. 
(CASE 
      WHEN Platform = 'unknown' AND Property IS NOT NULL THEN Property
      WHEN  Platform = 'DV360' THEN 'DV360' 
      ELSE Pubs_Final 
END) AS Pub_Rollup, 

---- There was a small formatting problem in Sheets (back when we were using the sheets) and the solution was to format the months this way. Since it's not problematic, I'm leaving them formatted like this. 
(CASE WHEN Time_2__Month_of_Activity = 1 THEN '1-Jan' 
      WHEN Time_2__Month_of_Activity = 2 THEN '2-Feb' 
      WHEN Time_2__Month_of_Activity = 3 THEN '3-Mar' 
      WHEN  Time_2__Month_of_Activity = 4 THEN '4-Apr' 
      WHEN Time_2__Month_of_Activity = 5 THEN '5-May' 
      WHEN Time_2__Month_of_Activity = 6 THEN '6-Jun' 
      WHEN Time_2__Month_of_Activity = 7 THEN '7-Jul' 
      WHEN Time_2__Month_of_Activity = 8 THEN '8-August' 
      WHEN Time_2__Month_of_Activity = 9 THEN '9-Sep' 
      WHEN Time_2__Month_of_Activity = 10 THEN '10-Oct' 
      WHEN Time_2__Month_of_Activity = 11 THEN '11-Nov' 
      WHEN Time_2__Month_of_Activity = 12 THEN '12-Dec' 
END) AS Month, 

---- Filtering out some country classifications. 
(CASE WHEN Segmentations_Country IS NOT NULL THEN Segmentations_Country ELSE NULL END) AS Country_Segmentation 

FROM CTE2 

---- LEFT JOINing some metadata pertaining to Platforms. 
LEFT JOIN `essence-analytics-dwh.rtf_global_spend_report.Platforms_Rolled_Up_Metadata` ON CTE2.Pubs_Final = `essence-analytics-dwh.rtf_global_spend_report.Platforms_Rolled_Up_Metadata`.string_field_0),

CTE4 AS (

SELECT *, 

---- Small formatting fix here, as well as adding LATAM which was an ADHOC request later on by Lauren and Reena. Finalizing Region. 
(CASE WHEN Region_Prep_2 = 'Global - North America' THEN 'Global - NA'
      WHEN Region_Prep_2 = 'NA' and Olive_Plan_Line_Name LIKE '%LATAM%' THEN 'LATAM'
      ELSE Region_Prep_2
END) AS Region_Final,

---- Classifying country names by looking left to right using OR statements. 
(CASE WHEN Olive_Plan_Line_Name LIKE '%India%' OR Olive_Plan_Name LIKE '%India%' OR Olive__Campaign_Name LIKE '%India%' THEN 'India'
WHEN Olive_Plan_Line_Name LIKE '%United States%' OR Olive_Plan_Name LIKE '%United States%' OR Olive__Campaign_Name LIKE '%United States%' THEN 'United States'
WHEN Olive_Plan_Line_Name LIKE '%United Kingdom%' OR Olive_Plan_Name LIKE '%United Kingdom%' OR Olive__Campaign_Name LIKE '%United Kingdom%' THEN 'United Kingdom'
WHEN Olive_Plan_Line_Name LIKE '%France%' OR Olive_Plan_Name LIKE '%France%' OR Olive__Campaign_Name LIKE '%France%' THEN 'France'
WHEN Olive_Plan_Line_Name LIKE '%Namibia%' OR Olive_Plan_Name LIKE '%Namibia%' OR Olive__Campaign_Name LIKE '%Namibia%' THEN 'Namibia'
WHEN Olive_Plan_Line_Name LIKE '%Germany%' OR Olive_Plan_Name LIKE '%Germany%' OR Olive__Campaign_Name LIKE '%Germany%' THEN 'Germany'
WHEN Olive_Plan_Line_Name LIKE '%Japan%' OR Olive_Plan_Name LIKE '%Japan%' OR Olive__Campaign_Name LIKE '%Japan%' THEN 'Japan'
WHEN Olive_Plan_Line_Name LIKE '%Vietnam%' OR Olive_Plan_Name LIKE '%Vietnam%' OR Olive__Campaign_Name LIKE '%Vietnam%' THEN 'Vietnam'
WHEN Olive_Plan_Line_Name LIKE '%Australia%' OR Olive_Plan_Name LIKE '%Australia%' OR Olive__Campaign_Name LIKE '%Australia%' THEN 'Australia'
WHEN Olive_Plan_Line_Name LIKE '%Canada%' OR Olive_Plan_Name LIKE '%Canada%' OR Olive__Campaign_Name LIKE '%Canada%' THEN 'Canada'
WHEN Olive_Plan_Line_Name LIKE '%Philippines%' OR Olive_Plan_Name LIKE '%Philippines%' OR Olive__Campaign_Name LIKE '%Philippines%' THEN 'Philippines'
WHEN Olive_Plan_Line_Name LIKE '%Turkey%' OR Olive_Plan_Name LIKE '%Turkey%' OR Olive__Campaign_Name LIKE '%Turkey%' THEN 'Turkey'
WHEN Olive_Plan_Line_Name LIKE '%Malaysia%' OR Olive_Plan_Name LIKE '%Malaysia%' OR Olive__Campaign_Name LIKE '%Malaysia%' THEN 'Malaysia'
WHEN Olive_Plan_Line_Name LIKE '%Thailand%' OR Olive_Plan_Name LIKE '%Thailand%' OR Olive__Campaign_Name LIKE '%Thailand%' THEN 'Thailand'
WHEN Olive_Plan_Line_Name LIKE '%Indonesia%' OR Olive_Plan_Name LIKE '%Indonesia%' OR Olive__Campaign_Name LIKE '%Indonesia%' THEN 'Indonesia'
WHEN Olive_Plan_Line_Name LIKE '%Italy%' OR Olive_Plan_Name LIKE '%Italy%' OR Olive__Campaign_Name LIKE '%Italy%' THEN 'Italy'
WHEN Olive_Plan_Line_Name LIKE '%Spain%' OR Olive_Plan_Name LIKE '%Spain%' OR Olive__Campaign_Name LIKE '%Spain%' THEN 'Spain'
WHEN Olive_Plan_Line_Name LIKE '%Russia%' OR Olive_Plan_Name LIKE '%Russia%' OR Olive__Campaign_Name LIKE '%Russia%' THEN 'Russia'
WHEN Olive_Plan_Line_Name LIKE '%Singapore%' OR Olive_Plan_Name LIKE '%Singapore%' OR Olive__Campaign_Name LIKE '%Singapore%' THEN 'Singapore'
WHEN Olive_Plan_Line_Name LIKE '%Poland%' OR Olive_Plan_Name LIKE '%Poland%' OR Olive__Campaign_Name LIKE '%Poland%' THEN 'Poland'
WHEN Olive_Plan_Line_Name LIKE '%South Africa%' OR Olive_Plan_Name LIKE '%South Africa%' OR Olive__Campaign_Name LIKE '%South Africa%' THEN 'South Africa'
WHEN Olive_Plan_Line_Name LIKE '%Pakistan%' OR Olive_Plan_Name LIKE '%Pakistan%' OR Olive__Campaign_Name LIKE '%Pakistan%' THEN 'Pakistan'
WHEN Olive_Plan_Line_Name LIKE '%Netherlands%' OR Olive_Plan_Name LIKE '%Netherlands%' OR Olive__Campaign_Name LIKE '%Netherlands%' THEN 'Netherlands'
WHEN Olive_Plan_Line_Name LIKE '%Ukraine%' OR Olive_Plan_Name LIKE '%Ukraine%' OR Olive__Campaign_Name LIKE '%Ukraine%' THEN 'Ukraine'
WHEN Olive_Plan_Line_Name LIKE '%Saudi Arabia%' OR Olive_Plan_Name LIKE '%Saudi Arabia%' OR Olive__Campaign_Name LIKE '%Saudi Arabia%' THEN 'Saudi Arabia'
WHEN Olive_Plan_Line_Name LIKE '%South Korea%' OR Olive_Plan_Name LIKE '%South Korea%' OR Olive__Campaign_Name LIKE '%South Korea%' THEN 'South Korea'
WHEN Olive_Plan_Line_Name LIKE '%Taiwan%' OR Olive_Plan_Name LIKE '%Taiwan%' OR Olive__Campaign_Name LIKE '%Taiwan%' THEN 'Taiwan'
WHEN Olive_Plan_Line_Name LIKE '%Israel%' OR Olive_Plan_Name LIKE '%Israel%' OR Olive__Campaign_Name LIKE '%Israel%' THEN 'Israel'
WHEN Olive_Plan_Line_Name LIKE '%Sweden%' OR Olive_Plan_Name LIKE '%Sweden%' OR Olive__Campaign_Name LIKE '%Sweden%' THEN 'Sweden'
WHEN Olive_Plan_Line_Name LIKE '%Egypt%' OR Olive_Plan_Name LIKE '%Egypt%' OR Olive__Campaign_Name LIKE '%Egypt%' THEN 'Egypt'
WHEN Olive_Plan_Line_Name LIKE '%Denmark%' OR Olive_Plan_Name LIKE '%Denmark%' OR Olive__Campaign_Name LIKE '%Denmark%' THEN 'Denmark'
WHEN Olive_Plan_Line_Name LIKE '%Brazil%' OR Olive_Plan_Name LIKE '%Brazil%' OR Olive__Campaign_Name LIKE '%Brazil%' THEN 'Brazil'
WHEN Olive_Plan_Line_Name LIKE '%Morocco%' OR Olive_Plan_Name LIKE '%Morocco%' OR Olive__Campaign_Name LIKE '%Morocco%' THEN 'Morocco'
WHEN Olive_Plan_Line_Name LIKE '%Nigeria%' OR Olive_Plan_Name LIKE '%Nigeria%' OR Olive__Campaign_Name LIKE '%Nigeria%' THEN 'Nigeria'
WHEN Olive_Plan_Line_Name LIKE '%Switzerland%' OR Olive_Plan_Name LIKE '%Switzerland%' OR Olive__Campaign_Name LIKE '%Switzerland%' THEN 'Switzerland'
WHEN Olive_Plan_Line_Name LIKE '%Norway%' OR Olive_Plan_Name LIKE '%Norway%' OR Olive__Campaign_Name LIKE '%Norway%' THEN 'Norway'
WHEN Olive_Plan_Line_Name LIKE '%Ireland%' OR Olive_Plan_Name LIKE '%Ireland%' OR Olive__Campaign_Name LIKE '%Ireland%' THEN 'Ireland'
WHEN Olive_Plan_Line_Name LIKE '%Belgium%' OR Olive_Plan_Name LIKE '%Belgium%' OR Olive__Campaign_Name LIKE '%Belgium%' THEN 'Belgium'
WHEN Olive_Plan_Line_Name LIKE '%United Arab Emirates%' OR Olive_Plan_Name LIKE '%United Arab Emirates%' OR Olive__Campaign_Name LIKE '%United Arab Emirates%' THEN 'United Arab Emirates'
WHEN Olive_Plan_Line_Name LIKE '%Austria%' OR Olive_Plan_Name LIKE '%Austria%' OR Olive__Campaign_Name LIKE '%Austria%' THEN 'Austria'
WHEN Olive_Plan_Line_Name LIKE '%Portugal%' OR Olive_Plan_Name LIKE '%Portugal%' OR Olive__Campaign_Name LIKE '%Portugal%' THEN 'Portugal'
WHEN Olive_Plan_Line_Name LIKE '%Hungary%' OR Olive_Plan_Name LIKE '%Hungary%' OR Olive__Campaign_Name LIKE '%Hungary%' THEN 'Hungary'
WHEN Olive_Plan_Line_Name LIKE '%Romania%' OR Olive_Plan_Name LIKE '%Romania%' OR Olive__Campaign_Name LIKE '%Romania%' THEN 'Romania'
WHEN Olive_Plan_Line_Name LIKE '%Hong Kong%' OR Olive_Plan_Name LIKE '%Hong Kong%' OR Olive__Campaign_Name LIKE '%Hong Kong%' THEN 'Hong Kong'
WHEN Olive_Plan_Line_Name LIKE '%Czech Republic%' OR Olive_Plan_Name LIKE '%Czech Republic%' OR Olive__Campaign_Name LIKE '%Czech Republic%' THEN 'Czech Republic'
WHEN Olive_Plan_Line_Name LIKE '%Sri Lanka%' OR Olive_Plan_Name LIKE '%Sri Lanka%' OR Olive__Campaign_Name LIKE '%Sri Lanka%' THEN 'Sri Lanka'
WHEN Olive_Plan_Line_Name LIKE '%New Zealand%' OR Olive_Plan_Name LIKE '%New Zealand%' OR Olive__Campaign_Name LIKE '%New Zealand%' THEN 'New Zealand'
WHEN Olive_Plan_Line_Name LIKE '%Finland%' OR Olive_Plan_Name LIKE '%Finland%' OR Olive__Campaign_Name LIKE '%Finland%' THEN 'Finland'
WHEN Olive_Plan_Line_Name LIKE '%Mexico%' OR Olive_Plan_Name LIKE '%Mexico%' OR Olive__Campaign_Name LIKE '%Mexico%' THEN 'Mexico'
WHEN Olive_Plan_Line_Name LIKE '%Greece%' OR Olive_Plan_Name LIKE '%Greece%' OR Olive__Campaign_Name LIKE '%Greece%' THEN 'Greece'
WHEN Olive_Plan_Line_Name LIKE '%Belarus%' OR Olive_Plan_Name LIKE '%Belarus%' OR Olive__Campaign_Name LIKE '%Belarus%' THEN 'Belarus'
WHEN Olive_Plan_Line_Name LIKE '%Lithuania%' OR Olive_Plan_Name LIKE '%Lithuania%' OR Olive__Campaign_Name LIKE '%Lithuania%' THEN 'Lithuania'
WHEN Olive_Plan_Line_Name LIKE '%Slovenia%' OR Olive_Plan_Name LIKE '%Slovenia%' OR Olive__Campaign_Name LIKE '%Slovenia%' THEN 'Slovenia'
WHEN Olive_Plan_Line_Name LIKE '%Slovakia%' OR Olive_Plan_Name LIKE '%Slovakia%' OR Olive__Campaign_Name LIKE '%Slovakia%' THEN 'Slovakia'
WHEN Olive_Plan_Line_Name LIKE '%Bulgaria%' OR Olive_Plan_Name LIKE '%Bulgaria%' OR Olive__Campaign_Name LIKE '%Bulgaria%' THEN 'Bulgaria'
WHEN Olive_Plan_Line_Name LIKE '%Algeria%' OR Olive_Plan_Name LIKE '%Algeria%' OR Olive__Campaign_Name LIKE '%Algeria%' THEN 'Algeria'
WHEN Olive_Plan_Line_Name LIKE '%Croatia%' OR Olive_Plan_Name LIKE '%Croatia%' OR Olive__Campaign_Name LIKE '%Croatia%' THEN 'Croatia'
WHEN Olive_Plan_Line_Name LIKE '%Tunisia%' OR Olive_Plan_Name LIKE '%Tunisia%' OR Olive__Campaign_Name LIKE '%Tunisia%' THEN 'Tunisia'
WHEN Olive_Plan_Line_Name LIKE '%Kenya%' OR Olive_Plan_Name LIKE '%Kenya%' OR Olive__Campaign_Name LIKE '%Kenya%' THEN 'Kenya'
WHEN Olive_Plan_Line_Name LIKE '%Kazakstan%' OR Olive_Plan_Name LIKE '%Kazakstan%' OR Olive__Campaign_Name LIKE '%Kazakstan%' THEN 'Kazakstan'
WHEN Olive_Plan_Line_Name LIKE '%Jordan%' OR Olive_Plan_Name LIKE '%Jordan%' OR Olive__Campaign_Name LIKE '%Jordan%' THEN 'Jordan'
WHEN Olive_Plan_Line_Name LIKE '%Estonia%' OR Olive_Plan_Name LIKE '%Estonia%' OR Olive__Campaign_Name LIKE '%Estonia%' THEN 'Estonia'
WHEN Olive_Plan_Line_Name LIKE '%Latvia%' OR Olive_Plan_Name LIKE '%Latvia%' OR Olive__Campaign_Name LIKE '%Latvia%' THEN 'Latvia'
WHEN Olive_Plan_Line_Name LIKE '%Ghana%' OR Olive_Plan_Name LIKE '%Ghana%' OR Olive__Campaign_Name LIKE '%Ghana%' THEN 'Ghana'
WHEN Olive_Plan_Line_Name LIKE '%Moldova Republic of%' OR Olive_Plan_Name LIKE '%Moldova Republic of%' OR Olive__Campaign_Name LIKE '%Moldova Republic of%' THEN 'Moldova Republic of'
WHEN Olive_Plan_Line_Name LIKE '%Cyprus%' OR Olive_Plan_Name LIKE '%Cyprus%' OR Olive__Campaign_Name LIKE '%Cyprus%' THEN 'Cyprus'
WHEN Olive_Plan_Line_Name LIKE '%Bangladesh%' OR Olive_Plan_Name LIKE '%Bangladesh%' OR Olive__Campaign_Name LIKE '%Bangladesh%' THEN 'Bangladesh'
WHEN Olive_Plan_Line_Name LIKE '%Bosnia & Herzegovina%' OR Olive_Plan_Name LIKE '%Bosnia & Herzegovina%' OR Olive__Campaign_Name LIKE '%Bosnia & Herzegovina%' THEN 'Bosnia & Herzegovina'
WHEN Olive_Plan_Line_Name LIKE '%Luxembourg%' OR Olive_Plan_Name LIKE '%Luxembourg%' OR Olive__Campaign_Name LIKE '%Luxembourg%' THEN 'Luxembourg'
WHEN Olive_Plan_Line_Name LIKE '%Uganda%' OR Olive_Plan_Name LIKE '%Uganda%' OR Olive__Campaign_Name LIKE '%Uganda%' THEN 'Uganda'
WHEN Olive_Plan_Line_Name LIKE '%Bahrain%' OR Olive_Plan_Name LIKE '%Bahrain%' OR Olive__Campaign_Name LIKE '%Bahrain%' THEN 'Bahrain'
WHEN Olive_Plan_Line_Name LIKE '%Serbia%' OR Olive_Plan_Name LIKE '%Serbia%' OR Olive__Campaign_Name LIKE '%Serbia%' THEN 'Serbia'
WHEN Olive_Plan_Line_Name LIKE '%Tanzania%' OR Olive_Plan_Name LIKE '%Tanzania%' OR Olive__Campaign_Name LIKE '%Tanzania%' THEN 'Tanzania'
WHEN Olive_Plan_Line_Name LIKE '%Zimbabwe%' OR Olive_Plan_Name LIKE '%Zimbabwe%' OR Olive__Campaign_Name LIKE '%Zimbabwe%' THEN 'Zimbabwe'
WHEN Olive_Plan_Line_Name LIKE '%Ivory Coast%' OR Olive_Plan_Name LIKE '%Ivory Coast%' OR Olive__Campaign_Name LIKE '%Ivory Coast%' THEN 'Ivory Coast'
WHEN Olive_Plan_Line_Name LIKE '%Ecuador%' OR Olive_Plan_Name LIKE '%Ecuador%' OR Olive__Campaign_Name LIKE '%Ecuador%' THEN 'Ecuador'
WHEN Olive_Plan_Line_Name LIKE '%Rwanda%' OR Olive_Plan_Name LIKE '%Rwanda%' OR Olive__Campaign_Name LIKE '%Rwanda%' THEN 'Rwanda'
WHEN Olive_Plan_Line_Name LIKE '%Dominican Republic%' OR Olive_Plan_Name LIKE '%Dominican Republic%' OR Olive__Campaign_Name LIKE '%Dominican Republic%' THEN 'Dominican Republic'
WHEN Olive_Plan_Line_Name LIKE '%Zambia%' OR Olive_Plan_Name LIKE '%Zambia%' OR Olive__Campaign_Name LIKE '%Zambia%' THEN 'Zambia'
WHEN Olive_Plan_Line_Name LIKE '%Jamaica%' OR Olive_Plan_Name LIKE '%Jamaica%' OR Olive__Campaign_Name LIKE '%Jamaica%' THEN 'Jamaica'
WHEN Olive_Plan_Line_Name LIKE '%Libya%' OR Olive_Plan_Name LIKE '%Libya%' OR Olive__Campaign_Name LIKE '%Libya%' THEN 'Libya'
WHEN Olive_Plan_Line_Name LIKE '%Malta%' OR Olive_Plan_Name LIKE '%Malta%' OR Olive__Campaign_Name LIKE '%Malta%' THEN 'Malta'
WHEN Olive_Plan_Line_Name LIKE '%Senegal%' OR Olive_Plan_Name LIKE '%Senegal%' OR Olive__Campaign_Name LIKE '%Senegal%' THEN 'Senegal'
WHEN Olive_Plan_Line_Name LIKE '%Costa Rica%' OR Olive_Plan_Name LIKE '%Costa Rica%' OR Olive__Campaign_Name LIKE '%Costa Rica%' THEN 'Costa Rica'
WHEN Olive_Plan_Line_Name LIKE '%Guatemala%' OR Olive_Plan_Name LIKE '%Guatemala%' OR Olive__Campaign_Name LIKE '%Guatemala%' THEN 'Guatemala'
WHEN Olive_Plan_Line_Name LIKE '%Bolivia%' OR Olive_Plan_Name LIKE '%Bolivia%' OR Olive__Campaign_Name LIKE '%Bolivia%' THEN 'Bolivia'
WHEN Olive_Plan_Line_Name LIKE '%Albania%' OR Olive_Plan_Name LIKE '%Albania%' OR Olive__Campaign_Name LIKE '%Albania%' THEN 'Albania'
WHEN Olive_Plan_Line_Name LIKE '%Malawi%' OR Olive_Plan_Name LIKE '%Malawi%' OR Olive__Campaign_Name LIKE '%Malawi%' THEN 'Malawi'
WHEN Olive_Plan_Line_Name LIKE '%Nicaragua%' OR Olive_Plan_Name LIKE '%Nicaragua%' OR Olive__Campaign_Name LIKE '%Nicaragua%' THEN 'Nicaragua'
WHEN Olive_Plan_Line_Name LIKE '%Uruguay%' OR Olive_Plan_Name LIKE '%Uruguay%' OR Olive__Campaign_Name LIKE '%Uruguay%' THEN 'Uruguay'
WHEN Olive_Plan_Line_Name LIKE '%Hondurus%' OR Olive_Plan_Name LIKE '%Hondurus%' OR Olive__Campaign_Name LIKE '%Hondurus%' THEN 'Hondurus'
WHEN Olive_Plan_Line_Name LIKE '%Democratic Republic of Congo%' OR Olive_Plan_Name LIKE '%Democratic Republic of Congo%' OR Olive__Campaign_Name LIKE '%Democratic Republic of Congo%' THEN 'Democratic Republic of Congo'
WHEN Olive_Plan_Line_Name LIKE '%Mauritius%' OR Olive_Plan_Name LIKE '%Mauritius%' OR Olive__Campaign_Name LIKE '%Mauritius%' THEN 'Mauritius'
WHEN Olive_Plan_Line_Name LIKE '%Botswana%' OR Olive_Plan_Name LIKE '%Botswana%' OR Olive__Campaign_Name LIKE '%Botswana%' THEN 'Botswana'
WHEN Olive_Plan_Line_Name LIKE '%Bahamas%' OR Olive_Plan_Name LIKE '%Bahamas%' OR Olive__Campaign_Name LIKE '%Bahamas%' THEN 'Bahamas'
WHEN Olive_Plan_Line_Name LIKE '%Benin%' OR Olive_Plan_Name LIKE '%Benin%' OR Olive__Campaign_Name LIKE '%Benin%' THEN 'Benin'
WHEN Olive_Plan_Line_Name LIKE '%Fiji Islands%' OR Olive_Plan_Name LIKE '%Fiji Islands%' OR Olive__Campaign_Name LIKE '%Fiji Islands%' THEN 'Fiji Islands'
WHEN Olive_Plan_Line_Name LIKE '%Guyana%' OR Olive_Plan_Name LIKE '%Guyana%' OR Olive__Campaign_Name LIKE '%Guyana%' THEN 'Guyana'
WHEN Olive_Plan_Line_Name LIKE '%Mali%' OR Olive_Plan_Name LIKE '%Mali%' OR Olive__Campaign_Name LIKE '%Mali%' THEN 'Mali'
WHEN Olive_Plan_Line_Name LIKE '%Barbados%' OR Olive_Plan_Name LIKE '%Barbados%' OR Olive__Campaign_Name LIKE '%Barbados%' THEN 'Barbados'
WHEN Olive_Plan_Line_Name LIKE '%Madagascar%' OR Olive_Plan_Name LIKE '%Madagascar%' OR Olive__Campaign_Name LIKE '%Madagascar%' THEN 'Madagascar'
WHEN Olive_Plan_Line_Name LIKE '%Guinea%' OR Olive_Plan_Name LIKE '%Guinea%' OR Olive__Campaign_Name LIKE '%Guinea%' THEN 'Guinea'
WHEN Olive_Plan_Line_Name LIKE '%Chad%' OR Olive_Plan_Name LIKE '%Chad%' OR Olive__Campaign_Name LIKE '%Chad%' THEN 'Chad'
WHEN Olive_Plan_Line_Name LIKE '%Djibouti%' OR Olive_Plan_Name LIKE '%Djibouti%' OR Olive__Campaign_Name LIKE '%Djibouti%' THEN 'Djibouti'
WHEN Olive_Plan_Line_Name LIKE '%Burundi%' OR Olive_Plan_Name LIKE '%Burundi%' OR Olive__Campaign_Name LIKE '%Burundi%' THEN 'Burundi'
WHEN Olive_Plan_Line_Name LIKE "%US%" OR Olive_Plan_Name LIKE "%US%" OR Olive__Campaign_Name LIKE "%US%" THEN 'United States'
WHEN Olive_Plan_Line_Name LIKE "%CA%"  OR Olive_Plan_Name LIKE "%CA%"  OR Olive__Campaign_Name LIKE "%CA%"  THEN 'Canada'
WHEN Olive_Plan_Line_Name LIKE "%JP %" OR Olive_Plan_Name LIKE "%JP %" OR Olive__Campaign_Name LIKE "%JP %" THEN 'Japan'
WHEN Olive_Plan_Line_Name LIKE "%[US]%" OR Olive_Plan_Name LIKE "%[US]%" OR Olive__Campaign_Name LIKE "%[US]%" THEN 'United States'
WHEN Olive_Plan_Line_Name LIKE "%[CA]%" OR Olive_Plan_Name LIKE "%[CA]%" OR Olive__Campaign_Name LIKE "%[CA]%" THEN 'Canada'
WHEN Olive_Plan_Line_Name LIKE "%JP%" OR Olive_Plan_Name LIKE "%JP%" OR Olive__Campaign_Name LIKE "%JP%" THEN 'Japan'
ELSE 'Unknown'
     
END) AS Country_Rolled

FROM CTE3 )
---- Some small changes that are best done outside of the CTE for Country. 
SELECT *,

(CASE WHEN Olive_Campaign_Managing_Region IS NULL THEN 'Unknown'
      WHEN Country_Segmentation IS NOT NULL THEN Country_Segmentation
      WHEN Country_Rolled IS NOT NULL THEN Country_Rolled
      ELSE 'Unknown'
END) AS Country_Final
     
     
FROM CTE4

---- This one specific test (literally one) was causing a NULL. Just filtering it out as per Chris's recommendation. (The spend was $1.1).
WHERE Olive__Campaign_Name != 'Google Agency Test Campaign' 
)
