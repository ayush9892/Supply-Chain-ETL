# Supply-Chain-ETL

## Purpose

The primary purpose of this ETL pipeline is to reinforce the knowledge acquired through self-study. Drawing on the knowledge and skills developed, the project aims to apply the real-world scenario of building an ETL pipeline.

## Scope and Goals

The scope of the project involves creating an ETL (Extract, Transform, Load) pipeline. This pipeline will extract data from a SQL Server through a control table and then perform transformations using a medallion architecture. The objective is to design the pipeline in such a way that it can handle both full load data and incremental load data. This means that every 24 hours, when the pipeline is executed, it will only load and transform the newly updated data. This approach will not only save costs but also enhance performance.


## Used Technologies
- Azure Data Factory
- Azure Databricks
- Azure Key Vault
- SQL Servers
- Azure Data Lake Gen 2

## Source Data: 📤
- [Download these Datasets](https://github.com/ayush9892/Supply-Chain-ETL/tree/main/Datasets)

## Architecture Diagram

![Architecture Diagram](https://github.com/ayush9892/Supply-Chain-ETL/assets/85745368/baae07e1-b55c-48df-893c-6431fa7bda3c)

## Data Extraction

There is one control table in sql server, that basically contains all the information about all other table, like Source_Object_Name, Target_Location, Load_Type, Creation_Time, Indicator, ...

![image](https://github.com/ayush9892/Supply-Chain-ETL/assets/85745368/30f19975-cc2d-46d8-b542-ce51b3cdcd32)
![image](https://github.com/ayush9892/Supply-Chain-ETL/assets/85745368/17f9dc19-9b16-43e2-9e7f-8b8e040ad08c)


#### 1. Lookup Control Table

- To get all the tables information from control table whose indicator is 1.

#### 2. Iterate each tables

- To iterate over each tables inside the control table.

#### 3. Check Load Type

- Inside ForEach, check whether it is full load or incremental load.

#### 4. Copy activity for Incremental Load

- Use LookUp activity to get the max watermark value from the table itself.
- Use Copy activity to copy only the incremented data to raw container in ADLS Gen 2, which is greater then the mentioned watermark value in control table.
- After that use, Stored Procedure activity, to update the latest watermark value in control table.

#### 5. Copy activity for Full Load

- Use Copy activity to copy all the data to raw container in ADLS Gen 2.
- After that use, Stored Procedure activity, to update the Indicator value to 1 in control table. So, that in next run, it will not again fetch that data.


## Data Transformation

- [View the complete Notebooks here](https://github.com/ayush9892/Supply-Chain-ETL/tree/main/pyspark_notebooks)

#### 1. Bronze Layer Transformation: (Raw to Refined)

- Check Null Values.
- Validate data type and Handling null values.
- Check Duplicates data.

#### 2. Silver Layer Transformation: (Business Rule Application)

- Categorizing supplier negotiation score and defect quality.
- Prioritizing transportation modes.
- Categorizing product prices.
- Calculating total costs for purchase orders.

#### 3. Gold Layer Transformation

- Ranking of suppliers based on the total number of services provided.
- Recommending supplier based on their supplying data.

## Pipeline Execution Result
- Here it shows, it only process the new incremented data.
![image](https://github.com/ayush9892/Supply-Chain-ETL/assets/85745368/eef300ef-195e-4902-9e8a-c9f10e74965b)
