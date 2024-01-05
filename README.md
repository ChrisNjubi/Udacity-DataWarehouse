Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity.

I am buidling an ETL pipeline that extracts their data from S3, stage them in Redshift, and transform data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

 
Description

This project entails buidling an ETL pipeline for a database hosted on Redshift. To accomplish this, I need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from the staging tables.

Project Datasets

i) Song Dataset
This is a subset of real data from the Million Song Dataset.Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.

ii) Log Dataset
This dataset consists of log files in JSON format. The log files in the dataset are partitioned by year and month. Example of file paths to files in this dataset:
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json

iii) Log Json Meta Information

This dataset contains the meta information that is required by AWS to correctly load Log Data.


Schema for Song Play Analysis

I've a star schema optimized for queries on song play analysis which includes a fact table and dimension tables.

a) Fact Table
songplays- records in event data associated with song plays i.e. records with page

b) Dimension Tables
users- users in the app
songs- songs in the musci database
artists- artists in music database
time- timestamps of records in songplays broken down into specific units

Project Template 
The project template consists of:

create_table.py- This python is where I create my fact and dimension tables for the star schema in Redshift.
etl.py- This python file is where I load data from S3 into staging tables on Redshift and then process that data into my analytics tables on Redshift.
sql_queries.py- This python file is where I define my SQL statements, which will be imported into the two other files above.
README.MD- This python file is where I provide discussion on my process and decisions for this ETL pipeline.


Create Table Schema
i) Write a SQL CREATE statement for each of the fact and dimension tables in sql_queries.py
ii)Complete the logic in create_tables.py to connect to the database and create the tables
iii) Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.
iv) Launch a redshift cluster and create an IAM role that has read access to S3.
v) Add redshift database and IAM role info to dwh.cfg.
vi) Test by running create_tables.py and checking the table schemas in my redshift database.

Build ETL Pipeline
i) Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
ii) Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
iii) Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
iv) Finally, I delete my redshift cluster when finished.