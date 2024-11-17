# DAM and ARCOS Log Processing Pipeline

## Table of Contents
1. [Introduction](#introduction)
2. [Technologies Used](#technologies-used)
3. [Project Workflow](#project-workflow)
4. [Setup Instructions](#setup-instructions)
5. [Detailed Steps of the Pipeline](#detailed-steps-of-the-pipeline)
6. [Challenges Faced](#challenges-faced)
7. [Outcome](#outcome)
8. [Real-World Usefulness](#real-world-usefulness)
9. [Future Improvements](#future-improvements)
10. [Error Handling and Logging](#error-handling-and-logging)
11. [Running the Pipeline](#running-the-pipeline)

---

## 1. Introduction
Monitoring user behavior, database activities, and system interactions is crucial for security and compliance in organizations. The **DAM (Database Activity Monitoring)** and **ARCOS (Access Control System)** Log Processing Pipeline is designed to process logs from these two sources, merge them efficiently, and produce an integrated dataset ready for further analysis, auditing, or compliance reporting. This pipeline ensures the seamless processing of potentially large volumes of logs, integrates data from different sources, and manages missing data effectively.

---

## 2. Technologies Used
- **Apache Spark**: For parallel and distributed data processing.
- **Python**: For scripting the pipeline logic and orchestration.
- **PySpark**: To leverage Spark's distributed processing capabilities.
- **Cloud Storage** (e.g., GCS, AWS S3): For storing input and output data.
- **Logging Libraries**: To log events and issues during execution.
- **Jupyter Notebooks** (Optional): For interactive development and testing.

---

## 3. Project Workflow
The pipeline workflow can be divided into the following steps:

1. **Data Ingestion**: Load DAM and ARCOS logs from specified input paths.
2. **Data Transformation**: Parse and preprocess the logs for consistency and readiness for merging.
3. **Data Merging**: Match and merge DAM and ARCOS logs using user IDs (CAN_ID) and timestamps, with a defined time window to handle discrepancies.
4. **Error Handling**: Log any missing or incomplete data, handle errors gracefully without halting the pipeline.
5. **Data Archiving**: Save the processed and merged logs to a designated output location for downstream use.

---

## 4. Setup Instructions

### Step 1: Install Dependencies
Ensure that the required packages are installed:

bash
'''
pip install pyspark
pip install google-cloud-storage
pip install pandas
'''

### Step 2: Configure Input and Output Paths
Set up the paths for where your DAM and ARCOS logs are stored and where the processed data will be saved. These paths can point to a cloud bucket (e.g., GCS) or an on-premise location.

### Step 3: Run the Pipeline
Execute the Python script with the required parameters, such as date and CAN_ID:


python dam_arcos_pipeline.py --date 20240101 --can_id ABC123

## 5. Detailed Steps of the Pipeline
--- 

**1. Data Ingestion**
Load the DAM logs from the input path.
Load the ARCOS logs from the input path.
Validate the data to ensure that the necessary columns (CAN_ID, timestamps, actions) are present.
**2. Data Transformation**
Convert timestamps in both datasets to a common format for time-based merging.
Filter and normalize relevant columns (e.g., action types, database names).
Standardize CAN_ID formats to ensure uniformity during merging.
**3. Data Merging**
Perform a join between DAM and ARCOS logs based on CAN_ID and a defined time window.
Use a time-tolerance window to match logs within a certain range if exact timestamps are not aligned.
If no matching ARCOS logs are found for a given CAN_ID, log this event and skip the processing for that ID:

if arcos_df.count() == 0:
    print(f"No ARCOS logs found for CAN_ID: {can_id}. Skipping processing.")
    return
**4. Error Handling and Skipping Missing Data**
The pipeline logs an error if the DAM or ARCOS logs for a specific date or CAN_ID are missing and proceeds to process the next available data. This ensures the pipeline does not halt due to missing or incomplete data, maintaining data continuity for available logs.

**5. Archiving Processed Data**
Write the processed and merged dataset to the specified output location (e.g., cloud storage, a database, or a local file system). Ensure that data is stored in a structured format such as Parquet for efficient querying and further processing.

## 6. Challenges Faced
Handling Missing Data: Missing or incomplete data was a significant challenge. The pipeline was designed to handle these scenarios gracefully by logging the issue and continuing with other available data, ensuring uninterrupted processing.
Time Discrepancies Between Logs: The timestamps in DAM and ARCOS logs often had slight discrepancies. To mitigate this, a time window was used for merging, allowing logs to be matched within a specified range.
Scalability and Performance: Given the high volume of log data, performance optimization was necessary. Using Apache Spark allowed for parallel data processing, ensuring scalability and preventing memory issues.
## 7. Outcome
The pipeline achieved its objectives by providing:

A robust mechanism for processing DAM and ARCOS logs efficiently.
The ability to manage missing data without stopping the entire pipeline.
A scalable architecture capable of handling large datasets.
Processed data that can be used for analysis, auditing, security monitoring, and compliance reporting.
---

## 8. Real-World Usefulness
This pipeline is particularly valuable for industries such as:

- **Banking and Finance**: To monitor database activities for security audits and compliance.
- **Healthcare**: Ensuring data privacy by auditing database access and activities.
- **E-commerce**: Monitoring user interactions with databases for anomaly detection.
- **General IT Security**: For tracking suspicious activities and protecting sensitive data.
## 9. Future Improvements
- **Real-Time Processing**: Integrating Apache Kafka or other streaming tools to enable real-time log processing.
- **Enhanced Error Handling**: Implementing advanced error recovery mechanisms, such as retries for failed processes and notifications for critical data issues.
- **Cloud Optimization**: Further optimizing the pipeline for cloud-native environments, leveraging services such as AWS Glue, Google BigQuery, and Azure Data Lake for better performance and reliability.

## 10. Error Handling and Logging
The pipeline incorporates robust error-handling mechanisms:

- **Missing DAM Logs**: Logs an error message and skips processing for that specific date.
- **Missing ARCOS Logs**: Logs an error message for the CAN_ID and skips further processing for that ID.
- **Data Mismatches**: Skips mismatched or incomplete entries and logs the discrepancies for future review.
Example:
'''
if dam_df.count() == 0:
    print(f"No DAM logs found for the date {date}. Skipping to next.")
    return
'''
'''
if arcos_df.count() == 0:
    print(f"No ARCOS logs found for CAN_ID: {can_id}. Skipping processing.")
    return
'''  

## 11. Running the Pipeline
**Step 1**: Install Required Packages
Ensure all necessary packages are installed as shown in the Setup Instructions.

**Step 2**: Prepare Input Data
Place your DAM and ARCOS log files in the specified input locations.

**Step 3**: Execute the Pipeline
Run the script using the command line:
'''
python dam_arcos_pipeline.py --date 20240101 --can_id ABC123
'''
--

## Step 4: Review Output
The processed data will be stored in the output directory. Check the log file for any issues or warnings logged during execution.
