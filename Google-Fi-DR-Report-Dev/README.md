# Google-Fi-DR-Report
### Requestor: 
#### Tom Santoro <thomas.santoro@essenceglobal.com>
### Request: 
#### Implement MLMO’s feedback around functionarily and additional data cuts to the existing gSheets based Google Fi DR report.
### Proposal:
#### To implement the requested functionality changes (dynamic charts, date filtering, etc), we’re proposing rebuilding the report in DataStudio driven by a BigQuery-based backend. This report format will also allow for quicker implementation of future requests. Initially this backend will be manually fed by the existing activation data tabs with an automated approach being built in parallel.
### Existing Data Sources
#### SEM Data
* Bing
* Google
* KW Breakout
#### Non-Brand/Brand
* Non-Brand SEM
* Brand SEM
#### Display
* GDN
* YouTube
* Yahoo
* PLA
### Link to Datastudio: 
#### https://datastudio.google.com/open/12Fn_R7QyoWz3_Tq7kkgnwshWu99S1nBz
### Link to SQL in GitHub Update as of Today, 6/21/2019
#### https://github.com/Noah-Hazan/Google-Fi-DR-Report/blob/master/Google%20Fi%20%20DR%20Report%20SQL%20Query.sql
### Abstract: 
The SQL Query replicates a previously-existing Google Sheet that was implementing logic on Google Fi data to extract helpfully descriptive information about Google Fi DR Spend and connecting it to Datastudio to visualize as a report for Google directly. 

### The Problem: 
Google Sheets, ironically, was becoming difficult to manage both in terms of updating logic and supporting the amount of data at hand. 

### The Solution: 
I created a SQL query via BigQuery that copies the same logic from the original Google Sheet while also connecting directly to Datastudio. This way, the report is significantly more stable, easier to edit, and quicker to load/edit. 
