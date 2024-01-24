## Project Context and Goals

The goal of this project was to create an end-to-end pipeline that will extract data from a constantly-updating (live) dataset and sucessfully load it to a relational database, imitating a real-life data warehouse. Transformations on the data would be done in between and post-loading (ETL and ELT processes) to also simulate potential data manipulation requirements of real-time applications. 

The data should be loaded in an easy-to-use format so as to allow the data analysts/data scientists to immediately begin working with it without having to commit a sizeable amount of manipulation techniques. This means that the data should be loaded onto several distinct tables that can easily be merged with each other and that contains intuitive column names and data types for each distinct record.

The DA/DS person should not be concerned with the accuracy of each data record and the process by which each data record is inserted or updated within the database. The pipeline should abstract and automate all of these processes as much as possible.

## Datasets Selected

The chosen live dataset for the project was the [Chicago Crimes One Year Prior to Present](https://data.cityofchicago.org/Public-Safety/Crimes-One-year-prior-to-present/x2n5-8w5q/about_data) which stores all reported crime incidents in the city of Chicago, Illinois. This public dataset provides the following advantages:
- The Chicago Data Portal provides an API endpoint which allows us to make unlimited HTTP requests to extract the dataset.
- The dataset contains a significant amount of distinct columns (fields) that can then be joined separately with a different dataset to facilitate data aggregations.
- The dataset also contains system fields that hold metadata information for each record (such as when the row was created, last updated, and a unique identifier for the row).
- The dataset can be queried prior to extraction based on a SQL-like query language in order to specify which exact data you need (e.g. data created after a specific date).



## Solution Architecture Diagram

Below is the solution architecture diagram, illustrating the key components and their interactions in our project.

![Solution Architecture Diagram](path/to/diagram.png)

## ELT/ETL Techniques Applied

We employed Extract, Load, Transform (ELT) and Extract, Transform, Load (ETL) techniques to efficiently process and transform raw data into a format suitable for analysis. This involved [mention specific tools or techniques used].

## Final Dataset and Demo Run (if possible)

The culmination of our efforts resulted in a refined dataset ready for analysis. We conducted a demo run to showcase the capabilities of our solution. Here are some highlights:

- **Dataset Overview:** Brief description of the final dataset.
- **Demo Run:** Steps and outcomes of the demonstration.

## Lessons Learnt

Throughout the course of the project, we encountered various challenges and gained valuable insights. Here are some key lessons learnt during the project:

- [List of lessons learnt]

