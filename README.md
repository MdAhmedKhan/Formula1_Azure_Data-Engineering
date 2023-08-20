# Formula1_Azure_Data-Engineering

In this project, we will use the Formula One dataset from this page: http://ergast.com/mrd/ (you can find the same dataset in Kaggle as well). 
The data source is available through API JSON/XML format or downloaded CSV files. We will be using Azure Data Factory to read the data from Rest API (JSON) and write to Azure SQL/DataLake.

Architecture:

Raw data will be read from Ergast via API method using Azure Data Factory and imported into ADLS Raw containers in JSON format.
Then Databricks will be used to perform data ingestion to ADLS Processed Layer, and transformation to the ADLS Presentation layer. Later data will be analyzed and visualized through Databricks and Power BI.
Azure Data Factory will be an orchestration tool to monitor and schedule the pipeline.
In this project, 3 different layers are used and processed through DataBricks notebooks described as Bronze (where raw data is loaded - RAW folder), Silver (where data is filtered and cleaned are stored - PROCESSED), and Gold (where data is transformed through business logic - TRANSFORMED).

![Architecture](https://github.com/MdAhmedKhan/Formula1_Azure_Data-Engineering/assets/47691372/e5fc36da-2621-4f6f-9e77-61ee23913eb2)
