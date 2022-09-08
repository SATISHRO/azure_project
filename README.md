sales2019 dataset Data Engineering With AWS/Azure(cloud service)

In this Project, I will get data from AWS dataset and send azure data lake storage

Note: I'm going to use following AWS Services for getting job Done..

1) AWS IAM
2) AWS VPC
3) AWS S3
4) Azure Data Lake storage
5) Azure data Factory
6) Azure databricks
7)Azure sql data warehouse
8)github(Reso)

Agenda Of Our Project...
============================================================ Phase-1 (Extract)
Find sales data for data analysis from aws dataset website : ("https://aws.amazon.com/covid-19-data-lake/")
Upload that data in s3 bucket named "Roy".
after azure data factory use copy for data lake
use databricks in mount data lake from analysis

============================================================ Phase-2 (Transform)
load data in databricks notebook.
change columns names & change column data type
remove duplicates & drop null values
after use save command from data lake store data

=========================================================== Phase-3 (Load)

use azure data factory  copy for azure sql data warehouse create
load data in sql 
sql in basic qury run



