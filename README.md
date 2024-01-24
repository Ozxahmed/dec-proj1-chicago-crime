# DEC Project 1 - Chicago Crime

## Objective

The objective of our project is to provide analytical datasets from our Chicago crime API and supporting police, ward and holiday databases.

## Consumers

The users of our data are Chicago city budget and policy analysts and officials. Our data is useful for getting up-to-date information on crimes that occur throughout Chicago.  

What users would find your data useful? How do they want to access the data?

## Questions

The three general question catagories our data addresses are: broad questions about crime in the city of Chicago, questions about crime incidents and holidays, and crime incidents and police districts. 

Examples of these questions include but are not limited to:
- What day of the week seems to have the highest number of crimes?
- What month of the year has the most crimes?
- What time of the day (morning, afternoon, evening, night) seems to have the highest number of crimes?
- Which beats have the most violent crimes? 
- Which police districuts may need additional resources, specialized training, etc. based on the type and quantity of crime?

Our data will allow our users to make resourcing and policy decisions based on historial and newly generated data. 

## Source datasets

What datasets are you sourcing from? How frequently are the source datasets updating?

Example:
Source name 	Source type 	Source documentation
Customers database 	PostgreSQL database 	-
Orders API 	REST API 	-


## Solution architecture

How are we going to get data flowing from source to serving? What components and services will we combine to implement the solution? How do we automate the entire running of the solution?

**Extraction Pattern:**

We are using a live dataset that updating periodically (weekly). If this is the first time the code has been run, the pipeline extracts the data two weeks at a time. If the database exists, the pipeline identifies the max data updated field in the database and extracts from that date to today's date. We have scheduled the extraction pipeline to run daily to check if data has been updated. 

**Data Loading Patterns:**

For the first data load, the pipeline extracts, transforms and loads the two weeks of data until the database has been completely backfilled. If the database exists, the newly updated   
    
**Data Transformation Patterns:** 

(include diagram image)
We recommend using a diagramming tool like draw.io to create your architecture diagram.


## Project Outline
1. Extraction 
   Code to extract incrementally 
2. Transformations
   Pandas Transformation 
    - Drop columns
    - Change column names
    - simple merge 
3. Scheduling 
4. Loading data to postgres
5. Write Unit tests
6. Transforming using SQL
   SQL Transformation 
    - Aggregations (weekends vs weekdays)
    - grouping 
    - having/where
    - window functions
7. Docker
8. AWS 
9. Presentation 


**IMPORTANT**: Once we are done with the project, please renew your APP TOKENs on the chicago crime data website.
### To do later:
- Divide code into modules (pipelines.py, assets.py, connectors.py)
- Write README.md
- Write project_plan.md
#### OPTIONAL:
- OOP (Create classes + objects)
