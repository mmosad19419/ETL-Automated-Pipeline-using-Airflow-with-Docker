# Data Analytics, Data Integration, and Automated ETL using Airflow with Docker

## Dataset
UK Accidents Dataset for the year 1983

## 1- Data Exploration(EDA) and Visualization:
- Load the dataset
- Explore the dataset and ask questions to give a better understanding of the data provided to you.
- Use data visualization to answer these questions.

## 2- Data Cleaning:
- Observe missing data, and data missing strategy(MCAR, MAR, or MNAR).
- Observe duplicate data.
- Observe outliers.
- After observing outliers, missing data, and duplicates, handle any unclean data.
- techniques used, and how has it affected the data(by showing the change in the data i.e: change in the number of rows/columns, change in distribution, etc., and commenting on it).

## 3- Data Transformation and Feature Engineering:
- Add a new column named 'Week number' and discretize the data into weeks according to the dates.
- Encode any categorical feature(s) and comment on why I used this technique and how the data has changed.
- Identify feature(s) which need normalization and show the reasoning. Then choose a technique to normalize the feature(s) and comment on why I used this technique.
- Add at least two more columns that add more info to the dataset by evaluating specific feature(s). i.e: A column indicating whether the accident was on the weekend or not).

## 4 - Build ETL pipeline 
process the data and load it to a Relational database to be ready for further analysis Or machine learning 

### Using:
#### Docker
To Setup Airflow environment and build a network to connect Airflow with Postgres SQL Database

#### Postgresql RDBMS
As the Storage Platform to Store Data for Analytics and Preprocessed Data for Machine Learning

#### Airflow
To schedule and Automated The pipeline, For reproducibility and Automation


## Files
#### 1. Data Analytics
Contain Data Analysis and Data Integration part of the Project
- DataSets: Contain the datasets used
- Data Analysis Notebook
- Data Integration Notebook

#### 2. Automated ETL using Airflow with Docker
contain docker image files and setup 
- docker-compose.yml
- Dockerfile
- requirements.txt

##### 3. dags folder
contain the main script file and dag file
- etl-dag.py, contain the dag building blocks of the pipeline
- etl_script.py, contain the function definitions for functions used in the dag
