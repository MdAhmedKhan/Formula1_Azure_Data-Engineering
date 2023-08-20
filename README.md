# Formula1_Azure_Data-Engineering
The Formula1 Data engineering project is a comprehensive data engineering solution that utilizes various Azure services, including Azure Data Lake, Azure Data Factory, Azure Databricks, Azure Hive metastore, and PowerBI. 
This project aims to collect complex semi-structured JSON data, process, analyze and visualize data related to Formula1 racing games. 


In this project, we will use the Formula One dataset from this page: http://ergast.com/mrd/ (you can find the same dataset in Kaggle as well). 
The data source is available through API JSON/XML format or downloaded CSV files. We will be using Azure Data Factory to read the data from Rest API (JSON) and write to Azure SQL/DataLake.

Architecture:

![Architecture](https://github.com/MdAhmedKhan/Formula1_Azure_Data-Engineering/assets/47691372/e5fc36da-2621-4f6f-9e77-61ee23913eb2)

Raw data will be read from Ergast via API method using Azure Data Factory and imported into ADLS Raw containers in JSON format.
Then Databricks will be used to perform data ingestion to ADLS Processed Layer, and transformation to the ADLS Presentation layer. Later data will be analyzed and visualized through Databricks and Power BI.

Azure Data Factory will be an orchestration tool to monitor and schedule the pipeline.

![Data Pipeline](https://github.com/MdAhmedKhan/Formula1_Azure_Data-Engineering/assets/47691372/bc3dfff1-4947-471a-8dc1-f5d53f298416)


In this project, 3 different layers are used and processed through DataBricks notebooks described as Bronze (where raw data is loaded - RAW folder),
Silver (where data is filtered and cleaned are stored - PROCESSED), and Gold (where data is transformed through business logic - TRANSFORMED).

![Azure Data Lake Snapshot](https://github.com/MdAhmedKhan/Formula1_Azure_Data-Engineering/assets/47691372/a2b08355-ce32-4350-82c1-aa7e530323f2)

Overall, the Formula1 Data Engineering Project demonstrates the integration of Azure services to build a robust and scalable data engineering solution. It showcases the capabilities of Azure Data Factory for data integration, Azure Databricks for data processing, and Power BI for data visualization. By leveraging these services, the project enables efficient data management, analysis, and visualization, ultimately enhancing decision-making and providing valuable insights into the Formula1 racing.





