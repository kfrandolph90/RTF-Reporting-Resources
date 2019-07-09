# google-global-spend-report
### Collaborators: Reena Kadakia, Chris Infanzon
### Creator: Noah Hazan (under guidance of manager, Kyle Randolph)
### Link to Datastudio: 
#### https://datastudio.google.com/u/0/reporting/1gLpGTdryiNwTqlhbdr4bHw35-GX6zK6K/page/CsHn
### Link to Datalab:
#### https://datalab.essenceglobal.com/explore/pr_google/google_cross_channel?qid=GvonBpLImRIQO64SliUW0h
### Link to SQL in BigQuery Updated as of Today, 6/21/2019
#### https://console.cloud.google.com/bigquery?sq=131786951123:17d8843c493d4473853b4a672b45202c
### Link to SQL in GitHub Update as of Today, 6/21/2019
#### https://github.com/Noah-Hazan/google-global-spend-report/blob/master/Google%20Global%20Spend%20Query.sql

## Architecture Diagram
![Global Spend Architecture Diagram](https://github.com/Noah-Hazan/google-global-spend-report/blob/master/Architecture_Global_Spend_Diagram.PNG)
### Abstract: 
The SQL Query with comments (pasted at the bottom of this README) replicates a previously-existing Google Sheet that was implementing logic on raw global spend data to extract helpfully descriptive information about Google's global spend and connecting it to Datastudio to visualize as a report for Google directly. 

### The Problem: 
Google Sheets, ironically, was becoming difficult to manage both in terms of updating logic and supporting the amount of data at hand. 

### The Solution: 
I created a SQL query via BigQuery that copies the same logic from the original Google Sheet while also connecting directly to Datastudio. This way, the report is significantly more stable, easier to edit, and quicker to load/edit. 

### The Query 
In the SQL query I used to replicate Chris's logic in Google Sheets, I used FOUR CTEs to allow for more complex querying without having to manually save and reupload data, or use big and complicated subqueries. Aside from that, there is no actual structure or order to the query. Some things are implemented 'out of order' (different than the order of the Google Sheet's columns) but the comments should explain each step of the querying process. 
