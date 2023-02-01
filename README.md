# Data Analytics, Data Integeration and Automated ETL using Airflow with Docker

## Dataset
UK Accidents Data for year 1983

## 1- Data Exploration(EDA) and Visualization:
- Load the dataset
- Explore the dataset and ask questions to give a better understanding of the data provided to you.
- Use data visualization to answer these questions.

## 2- Data Cleaning:
- Observe missing data, and data missing strategy(MCAR, MAR, or MNAR).
- Observe duplicate data.
- Observe outliers.
- After observing outliers, missing data and duplicates, handle any unclean data.
- Techiques used, and how has it affected the data(by both showing the change in the data i.e: change in number of rows/columns, change in distribution,etc. and commenting on it).

## 3- Data Transformation and Feature Engineering:
- Add a new column named 'Week number' and discretize the data into weeks according to the dates.
- Encode any categorical feature(s) and comment on why I used this technique and how the data has changed.
- Identify feature(s) which need normalization and show the reasoning. Then choose a technique to normalize the feature(s) and comment on why I used this technique.
- Add at least two more columns that add more info to the dataset by evaluating specific feature(s). i.e: A column indicating whether the accident was on a weekend or not).

## 4 - Build ETL pipeline 
process the data and load it to Relational database to be ready for further analysis Or machine learning 

### Using:
#### Docker
To Setup Airflow Enviroment and build a network to connct Airflow with Postgres Sql Database

#### Postgresql RDBMS
As the Storage Platform to Store Data for Analytics and Preprocessed Data for Machine Learning

#### Airflow
To schudle and Automated The pipeline, For reproducability and Automation


## Files
#### 1. Data Analytics
Contain Data Analysis and Data Integration part of the Project
- DataSets : Contain the datasets used
- Data Analysis Notebook
- Data Integration Notebook

#### 2.Automated ETL using Airflow with Docker
contain docker image files and setup 
- docker-compose.yml
- Dockerfile
- requirements.txt

###### dags folder
contain main script file, and dag file
- etl-dag.py, contain the dag building blocks of the pipeline
- etl_script.py, contain the function definitions for functions used in the dag