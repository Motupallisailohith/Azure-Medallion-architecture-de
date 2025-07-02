
# Car Sales Data Engineering Project: End-to-End Analytics Platform

## 📊 Project Overview

This project demonstrates an end-to-end data engineering solution for analyzing car sales performance. It encompasses the entire data lifecycle, from raw data ingestion and robust cloud-based ETL processes to advanced data modeling and interactive business intelligence dashboards. The platform is designed for scalability, incremental processing, and delivering actionable insights into sales trends, revenue, and inventory.

## ✨ Key Features & Capabilities

  * Automated Incremental Data Ingestion: Implemented a robust data pipeline on Azure Data Factory (ADF) for dynamic, incremental loading of sales data, utilizing Change Data Capture (CDC) principles for efficiency.
  * Medallion Architecture Implementation: Structured data processing in Azure Databricks (PySpark, Python) into Bronze (raw), Silver (cleaned/transformed), and Gold (conformed for analytics) layers using Delta Lake for ACID transactions and schema evolution.
  * Optimized Data Warehousing: Designed a performant star schema data model (Fact and Dimension tables with SCD Type 2) in Azure SQL Database, facilitating complex analytical queries and historical data tracking.
  * Interactive Business Intelligence Dashboards: Developed dynamic and insightful dashboards in Power BI, offering drill-through capabilities and advanced DAX measures for real-time visualization of sales performance, regional trends, and product analysis.
  * End-to-End Data Lifecycle Management: Showcases proficiency in cloud data platform architecture, distributed data processing, data modeling, and business intelligence tool integration.

## 🏗️ Architecture

The solution follows a modern cloud data architecture, leveraging Azure services and a Medallion Lakehouse approach:

1.  Data Ingestion (Bronze Layer):

      * Source: Mock car sales data (e.g., CSV).
      * Azure Data Factory (ADF): Orchestrates data ingestion, employing dynamic and incremental copy activities.
      * Azure Data Lake Storage Gen2: Stores raw (Bronze) data.
      * Azure SQL Database: Manages metadata for incremental loading (e.g., watermark tables).

2.  Data Transformation & Conformance (Silver & Gold Layers):

      * Azure Databricks: Used for scalable data transformation and quality checks.
      * Delta Lake: Built on top of Data Lake Storage, providing a reliable storage layer for all medallion layers (ACID properties, schema enforcement, time travel).
      * Silver Layer: Cleansed, normalized, and integrated data from Bronze.
      * Gold Layer: Conformed dimensional model (star schema) optimized for analytical queries, ready for consumption.

3.  Data Warehousing:

      * Azure SQL Database: The Gold layer data is loaded into Azure SQL Database as the central analytical data store, optimized for Power BI connectivity.

4.  Business Intelligence & Visualization:

      * Power BI: Connects to the Gold layer in Azure SQL Database to create interactive reports and dashboards.

<!-- end list -->

```
+--------------+     +----------+     +-------------------+     +---------------------+     +-----------+
| Source Data  | --> | Azure    | --> | Azure Data Lake   | --> | Azure Databricks    | --> | Azure SQL | --> Power BI
| (CSV/API)    |     | Data     |     | Storage (ADLS Gen2)|     | (PySpark, Delta Lake) |     | Database  |
+--------------+     | Factory  |     |   (Bronze Layer)  |     | (Silver & Gold Layers)|     | (Gold Layer)|
                     +----------+     +-------------------+     +---------------------+     +-----------+
                                             ^
                                             |
                                         Incremental
                                           Metadata
                                         (Azure SQL DB)
```

## 🛠️ Technologies Used

  * Cloud Platform: Microsoft Azure
      * Azure Data Factory (ADF)
      * Azure Databricks
      * Azure Data Lake Storage Gen2 (ADLS Gen2)
      * Azure SQL Database
  * Programming Languages: Python, SQL, PySpark
  * Data Technologies:
      * Delta Lake
      * Change Data Capture (CDC)
      * Star Schema (Fact & Dimension Modeling, SCD Type 2)
      
  * Business Intelligence: Power BI (Desktop, Service)
  * Version Control: Git, GitHub
  * Tools/Libraries: Streamlit, `python-dotenv`, `requests`, `langchain`, `langchain-community`, `langchain-core`, `langchain-ollama`, `langchain-text-splitters`, `chromadb`, `pypdf`, `unstructured`, `huggingface-hub`, `pydantic`, `tiktoken`, `rich`, `diskcache`, `transformers`.

## 🚀 Setup & Deployment

### Local Development Setup

1.  Clone the Repository:
    ```bash
    git clone https://github.com/Motupallisailohith/quiverai-local.git
    cd quiverai-local
    ```
2.  Create and Activate Virtual Environment (Python 3.12 Recommended):
    ```bash
    # Ensure Python 3.12 is installed on your system
    py -3.12 -m venv .venv
    .\.venv\Scripts\Activate.ps1 # On Windows PowerShell
    # source .venv/bin/activate # On Linux/macOS
    ```

    ```
    Your application should open in your browser at `http://localhost:8501`.


## 📚 Project Highlights & Learnings

  * Full Data Engineering Lifecycle: Demonstrated proficiency in designing, building, and deploying scalable data pipelines from source to consumption.
  * Cloud-Native Technologies: Gained hands-on experience with core Azure services (ADF, Databricks, SQL DB, ADLS Gen2) and open-source frameworks (Delta Lake, FAISS, Ollama).
  * Complex Dependency Management: Successfully navigated intricate Python dependency conflicts (Pydantic, `huggingface_hub`, `sentence-transformers`, `faiss-cpu`) to achieve a stable application environment.
  * Performance Optimization & Resource Management: Addressed challenges related to LLM model size (2.6GB) within free-tier cloud resource constraints, optimizing model pulling and runtime environment.
  * Data Modeling & BI Integration: Applied principles of dimensional modeling and built interactive Power BI dashboards for intuitive data exploration and decision support.



-----
