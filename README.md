# DEC Project 1 - Chicago Crime

## Project Context and Goals

The goal of this project was to create an end-to-end pipeline that will extract data from a constantly-updating (live) dataset and successfully load it to a relational database, imitating a real-life data warehouse. Transformations on the data would be done within the pipeline prior to loading (ETL), and also after loading (ELT), to simulate potential data manipulation requirements of real-time applications.

The data should be loaded in an easy-to-use format so as to allow the data analysts/data scientists to immediately begin working with it without having to commit a sizeable amount of time to data manipulations. This means that the data should be loaded onto several distinct tables that can easily be merged with each other and that contains intuitive column names and data types for each distinct record.

The DA/DS person should not be concerned with the accuracy of each data record and the process by which each data record is inserted or updated within the database. The pipeline should abstract and automate all of these processes as much as possible.

## Business Objective

The objective of our project is to provide analytical datasets from our Chicago crime API and supporting police, ward and holiday databases. The datasets are clean, regularly updated, and supported with several custom table views generated from these datasets.

## Consumers

The users of our data are Chicago city budget and policy analysts and officials. Our data is useful for getting up-to-date information on crimes that occur throughout Chicago. Our users would want to access the data primarily through sql queries and the custom sql views from the database. This data can also be used to create dashboards with maps and metrics.

## Questions

There are three general question categories our data addresses: broad questions about crime in the city of Chicago, questions about crime incidents during holidays, and crime incidents within police districts.

Examples of these questions include but are not limited to:

- What day of the week seems to have the highest number of crimes?
- What month of the year has the most crimes?
- What time of the day (morning, afternoon, evening, night) seems to have the highest number of crimes?
- Which holidays have the highest incidences of crime and may require additional staffing or resources?
- Which beats have the most violent crimes?
- Which police districuts may need additional resources, specialized training, etc. based on the type and quantity of crime?

Our data will allow our users to make resourcing and policy decisions based on historical and newly generated data.

## Source datasets

The chosen live dataset for the project was the [Chicago Crimes One Year Prior to Present](https://data.cityofchicago.org/Public-Safety/Crimes-One-year-prior-to-present/x2n5-8w5q/about_data) which stores all reported crime incidents in the city of Chicago, Illinois. This public dataset provides the following advantages:

- The Chicago Data Portal provides an API endpoint which allows us to make unlimited HTTP requests to extract the dataset.
- The dataset contains a significant amount of distinct columns (fields) that can then be joined separately with a different dataset to facilitate data aggregations.
- The dataset also contains system fields that hold metadata information for each record (such as when the row was created, last updated, and a unique identifier for the row).
- The dataset can be queried prior to extraction based on a SQL-like query language in order to specify which exact data you need (e.g. data created after a specific date).

In addition to the Chicago Crimes data, we used static datasets for 2023 and 2024 holidays, police stations and ward offices.

## Solution Architecture Diagram

Below is the solution architecture description and diagram, illustrating the key components and their interactions in our project.

![DEC Project 1 Architecture](images/DEC-Project1-architecture.jpg)

- **Python** was used for:
  - Extracting the crime data, via an API, from the city of chicago website.
  - Reading .csv files into pandas dataframes. This is for Holidays, police stations, and wards data.
  - Transforming data -> drop unnecessary columns, rename columns, and create a calendar (dates) dataframe that merges all the dates in 2023 and 2024 with the holidays information from the csv file.
  - Load data to our postgres database.

- **PostgreSQL DBMS** was used for:
  - Storing all our data: crime data, dates, police stations, and ward offices.

- **AWS RDS** was used for:
  - hosting and managing our postgres database.

- **SQL** was used for:
  - creating views off of the data that is loaded

- Other programs used:
  - **Docker** was used to containerize our pipeline
  - **ECR** was used to host our docker container
  - **ECS** was used to run the docker container
  - **S3** was used to store the `.env` file.
  

## ELT/ETL Techniques Applied

We employed Extract, Transform, Load (ETL) and Extract, Load, Transform (ELT) techniques to efficiently process and transform raw data into a format suitable for analysis.

### Extraction Pattern

We are using a live dataset that updates periodically (6 days/week). Our pipeline first checks if the database exists. If the database doesn't exit, for the first time the code runs, the pipeline extracts the data two weeks at a time, based on the `date_of_occurrence` field, until all data has completed the ETL process and has been loaded into our database, which is hosted and managed on AWS RDS. This serves as a backfill of the database. If the database does exist, the pipeline identifies the max `updated_at` field in the database and extracts data starting from that date to today's date. The extraction pipeline is scheduled to run daily to check if data has been updated.

### Data Transformation Patterns

#### ETL

There are two places we use data transformation patterns. The first is after the extracion of the crime data and import of the .csv data. We used Pandas to drop columns, change column names and generate the holiday dataframe.

#### ELT

The second set of transformations happens after the data has been loaded into the database. We use sql templates to generate views in the database. These transformation include CTEs, joining, grouping, sorting, and aggregation function. The SQL transformations result in several table views in the database. Our ERD diagram for the tables and views can be seen below:

![DEC Project 1 Architecture](images/chicago-crimes-erd-diagram.jpg)

### Data Loading Patterns

Our pipeline extracts, transforms and loads two weeks of data at a time until the database has been completely backfilled.

## Data Flow Chart

For more details on project data flow, please see the [Chicago Crime Project Flowchart](images/DEC-Project1-Flowchart.pdf) pdf.

## Limitations and Lessons Learned

Throughout the course of the project, we encountered various challenges and gained valuable insights. Here are some key lessons learnt during the project:

- It was very important to us as a group to be able to practice each step of the process. This was extremely valuable in contributing to our learning but did slow us down and may have added additional work or stress toward the end.
- We had hoped to eliminate some of the technical debt in the code by making it more modular or object oriented. This is something that can be a follow on task.
- We started by using Trello to manage tasks but found it worked better for our group to orangize tasks in the issues section of github. We were able to add code, comments and have discussion for each issue.

## Installation and Running Instructions

