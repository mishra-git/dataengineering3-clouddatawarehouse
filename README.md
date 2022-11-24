# PROJECT: DATA WAREHOUSE - ANALYTICS USING S3 AND REDSHIFT ON SONG DATA FOR SPARKIFY 

## BACKGROUND
Due to growing user base , a music startup company wants to move their processes and data into cloud. The data lives in AWS S3 in a directory of JSON logs. They want  a data engineer to help this project

As a data engineer, I have built an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team  so they can extract meaningful insights into what songs their users are listening to

*** 
## DATASETS 
Dataset for this project was provided by Udacity in two public S3 buckets as folows 

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
Log data json path: s3://udacity-dend/log_json_path.json

## DIFFERENT TOOLS AND SERVICES USED 
- Python is used as the main programming language . 
- AWS Redshift service is used to injest data from files and laod them into staging tables in postgress database 
- Copy Command has been used to copy the file contents into the staging tables 


## CORE PROJECT STEPS 
The project has been completed with the following steps 

#### 1: DESIGNING DATABASE SCHEMA 
Based on the data as seen in the data and log files, following tables have been designed in the schema 

**Staging Tables** 
Two staging tables to hold the data from log files and data files from S3 
- staging_events
- staging_songs

**Fact tables** 
One fact table has been desined to record the event data involving song plays 
- songplays 

**Dimension Tables**
Following Dimension tables have been designed 
- users - application users 
- songs - songs in music database
- artists - Different artists that have sung various songs 
- time - timestamps of records corresponding to various songs 

#### 2: DATA WAREHOUSE SETUP 
AWS Redshift has been created with appropriate iAM user under the student learning account provided by Udacity. The user has been given administrative access and attach the policies.

Other details have been filled in the config file. The cluster end point have been used to feed the ETL process where to load the data 

#### 3:  ETL PIPELINE 
Python process have been created with the following files 
- create_tables.py to create all required tables per the schema desgin 
- sql_queries.py to hold the DDL and insert queies for the ETL process 
- etl.py to injest data into Redshift from S3 
- dhw.cfg - Configuration file used that contains info about Redshift, IAM and S3
How to Run
***

## HOW TO RUN THE PROJECT FILES 
- Run create_tables.py 
This will drop tables if they exists already and craete the needed tables 
- Run etl.py 
This script reaches out to S3, injest the data into Redshift postress tables 
***

## CREDIT / REFERENCE 
- Project starter files all belong to Udacity
- Data source used in the project as well has been provided by Udacity 
- https://pypi.org/project/pep8/
https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
https://www.postgresql.org/docs/current/datatype.html