# sparkify-data-lakes
ETL pipeline that uses Spark to process extracted S3 data, and loads data back into S3 as dimensional tables

## Introduction
A (fictional) music streaming startup, Sparkify, has grown their user base and song database even more 
and wants to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

### Project Tools
Spark API: PySpark 
Cloud Services: Amazon Web Services, for Data Lake hosted on S3 (Simple Storage Service).

Procedure:
* build an ETL pipeline for a data lake 
* load data from S3
* process the data into analytics tables using PySpark
* load them back into S3
* deploy this Spark process on a cluster using AWS Redshift
