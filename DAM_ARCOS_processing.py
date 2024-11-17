from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import broadcast, col, unix_timestamp, split
import os
import shutil
from datetime import datetime, timedelta

def main(date, can_id):
    # Initialize Spark session
    spark = SparkSession.builder.appName("DAM_ARCOS_Processing").getOrCreate()

    # Generalized date variable (format as 'yymmdd')
    date_str = (datetime.strptime(date, "%y%m%d") + timedelta(days=1)).strftime("%y%m%d")

    # File paths
    dam_logs_path = f"gs://your-bucket/DAM_logs/{date_str}.csv"
    app_dump_path = "gs://your-bucket/application_dump.csv"
    app_server_mapping_path = "gs://your-bucket/application_server_mapping.csv"
    arcos_base_path = f"gs://your-bucket/ARCOS_logs/{date_str}/"
    output_path = "gs://your-bucket/output_DAM_ARCOS/"
    archive_path = "gs://your-bucket/archive/"

    # Read the CSV files
    dam_logs_df = spark.read.format("csv").option("header", "true").load(dam_logs_path)
    app_dump_df = spark.read.format("csv").option("header", "true").load(app_dump_path)
    app_server_mapping_df = spark.read.format("csv").option("header", "true").load(app_server_mapping_path)

    # Step 1: Join DAM logs with application dump and server mapping, adding only specific columns
    joined_dam_df = dam_logs_df \
        .join(broadcast(app_dump_df.select("application_name", "CAN_id")), "application_name", "left") \
        .join(broadcast(app_server_mapping_df.select(
            col("ip").alias("source_ip"), col("application_name").alias("Source_application_name"))),
            dam_logs_df["source_ip"] == app_server_mapping_df["source_ip"], "left") \
        .join(broadcast(app_server_mapping_df.select(
            col("ip").alias("destination_ip"), col("application_name").alias("Destination_application_name"))),
            dam_logs_df["destination_ip"] == app_server_mapping_df["destination_ip"], "left")

    # Step 2: Partition by CAN_ID and write DAM logs
    dam_output_path = f"gs://your-bucket/output_DAM/{date_str}"
    joined_dam_df.write.partitionBy("CAN_id").mode("overwrite").csv(dam_output_path)

    # Step 3: Process ARCOS logs for the given CAN_ID
    arcos_path = f"{arcos_base_path}/{can_id}/*.csv"
    arcos_df = spark.read.format("csv").option("header", "true").load(arcos_path)

    # Rename columns in ARCOS for join compatibility
    arcos_df = arcos_df.withColumnRenamed("service_description", "service_ip")

    # Step 4: Split the 'service_description' column in arcos_df to extract 'generic_user' and 'DB_server_IP'
    split_col = split(col("service_ip"), "@")
    arcos_df = arcos_df.withColumn("generic_user", split_col.getItem(1)) \
                       .withColumn("DB_server_IP", split_col.getItem(0))

    # Step 5: Join DAM with ARCOS data on matching IP and command execution time
    merged_df = joined_dam_df.filter(col("CAN_id") == can_id).join(
        arcos_df,
        (joined_dam_df["destination_ip"] == arcos_df["DB_server_IP"]) &
        (joined_dam_df["generic_user"] == arcos_df["generic_user"]) &
        (unix_timestamp(joined_dam_df["command_execution_time"]).between(
            unix_timestamp(arcos_df["cmd_login_time"]),
            unix_timestamp(arcos_df["cmd_logout_time"])
        )),
        "inner"
    )

    # Filter out entries with "ARCOS" as the source application name
    merged_df = merged_df.filter(col("Source_application_name") == "ARCOS")

    # Step 6: Write the final joined DataFrame to an output folder by CAN_ID
    final_output_path = f"{output_path}/{can_id}/{date_str}"
    merged_df.write.mode("overwrite").csv(final_output_path)

    # Step 7: Archive the processed DAM and ARCOS files
    archive_dam_path = f"{archive_path}/DAM/{date_str}/{can_id}_{date_str}.csv"
    archive_arcos_path = f"{archive_path}/ARCOS/{date_str}/{can_id}/"

    if merged_df.count() > 0:
        local_dam_path = f"/local/path/to/DAM/{can_id}_{date_str}.csv"
        local_arcos_path = f"/local/path/to/ARCOS/{can_id}/"

        # Archive DAM
        if os.path.exists(local_dam_path):
            shutil.move(local_dam_path, archive_dam_path)

        # Archive ARCOS
        if os.path.exists(local_arcos_path):
            shutil.move(local_arcos_path, archive_arcos_path)

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process date and can_id arguments')
    parser.add_argument('--date', type=str, required=True, help='Date in yyyymmdd format')
    parser.add_argument('--can_id', type=str, required=True, help='CAN ID to process')
    args = parser.parse_args()

    main(args.date, args.can_id)
