import pandas as pd
import boto3
from io import StringIO
from botocore.exceptions import ClientError
from datetime import datetime,timedelta
from datatype import dtypes_order_details, dtypes_payment_details, reduction_cds, card_types, data_cols,date_cols,dtypes_all
from data_transformer import DataTransformer

BUCKET_NAME = "otg-prod-fraud-data"
s3_bucket = 'otg-fraud-dataml-prod'
s3_prefix = 'acm/daily/temporary'
s3 = boto3.client("s3")

SEP = ","

def read_s3_csv(s3_path, sep="\t", dtype=None,parse_dates=None):
    s3 = boto3.client("s3")
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')), sep=sep, dtype=dtype,parse_dates=parse_dates)
        print(f"Successfully read {s3_path}")
        return df
    except ClientError as e:
        print(f"Error reading {s3_path}: {e}")
        return None

def process_and_merge_data(order_details_path, payment_details_path):
    # Load data from S3
    order_details_daily = read_s3_csv(order_details_path, sep="\t", dtype=dtypes_all,parse_dates=date_cols)
    payment_details_daily = read_s3_csv(payment_details_path, sep="\t", dtype=dtypes_all,parse_dates=['bus_date'])

    if order_details_daily is None or payment_details_daily is None:
        print("Failed to read one or both of the input files.")
        return None  # Handle case where S3 files are not found

    # Call the DataTransformer methods to process data
   # order_details_processed = DataTransformer.process_order_details(order_details_daily)
   # payment_details_processed = DataTransformer.process_payment_details(payment_details_daily)

    # Drop columns not in data_cols after processing
    order_details_processed = order_details_daily.drop(order_details_daily.columns.difference(data_cols), axis='columns')
    print(order_details_processed.dtypes)
    payment_details_processed = payment_details_daily.drop(payment_details_daily.columns.difference(data_cols), axis='columns')
    print(payment_details_processed.dtypes)
    # order_details_processed=order_details_processed.astype(order_dtypes)
    
    # payment_details_processed=payment_details_processed.astype(payment_dtypes)
    # Drop specific columns from payment_details and order_details after processing
   # payment_details_processed = payment_details_processed.drop(["bus_date"], axis=1)
    order_details_processed = order_details_processed.drop(
        [
            "pos_venue_id",
            "transaction_id",
            "src_sys_id",
            "vendor_id",
            "active_fl",
        ],
        axis=1,
    )

    # Example transformation (daily merge) 
   # acm = order_details_processed.merge(payment_details_processed, on="order_id", how="left")
    order_details_processed = order_details_processed.drop_duplicates()
    payment_details_processed = payment_details_processed.drop_duplicates()
    acm = order_details_processed.merge(payment_details_processed, on=["order_id", "bus_date"], how="left")

    # Call process_acm_details to replace values in 'reduction_cd' and 'card_type'
    acm_processed = DataTransformer.process_acm_details(acm, reduction_cds, card_types)

    return acm_processed

def run_acm_daily_pipeline(v_order_details_daily, v_payment_details_daily):
    acm_daily_processed = process_and_merge_data(v_order_details_daily, v_payment_details_daily)
    
    if acm_daily_processed is not None:
        print("v_order_details",v_order_details_daily)
        date_part = v_order_details_daily.split('/') # ['2024', '10', '2']
        timestamp = date_part[-1].split('_')[0]
        # Extract year, month, and day from the file path
        date_part = v_order_details_daily.split('/')[-4:-1]  # ['2024', '10', '2']
        year, month, day = date_part[0], date_part[1], date_part[2]
        
        # Construct the S3 path
        s3_output_path = f"s3://otg-fraud-dataml-prod/acm/daily/temporary/{year}/{month}/{day}/{timestamp}_acm_output.csv"
        
        # Save results to S3
        save_to_s3(acm_daily_processed, s3_output_path)
    
    print("Daily data processing complete")
    return s3_output_path

def run_acm_monthly_pipeline(v_order_details_monthly, v_payment_details_monthly):
    acm_monthly_processed = process_and_merge_data(v_order_details_monthly, v_payment_details_monthly)
    
    if acm_monthly_processed is not None:
        date_part = v_order_details_monthly.split('/') # ['2024', '10', '2']
        timestamp = date_part[-1].split('_')[0]
        # Extract year, month, and day from the file path
        date_part = v_order_details_monthly.split('/')[-4:-1]  # ['2024', '10', '2']
        
        year, month, day = date_part[0], date_part[1], date_part[2]
        
        # Construct the S3 path
        s3_output_path = f"s3://otg-fraud-dataml-prod/acm/monthly/temporary/{year}/{month}/{day}/{timestamp}_acm_output.csv"
        
        # Save results to S3
        save_to_s3(acm_monthly_processed, s3_output_path)
    
    print("Monthly data processing complete")
    return s3_output_path

def list_files_in_last_30_days(end_date):
    """Retrieve the latest .csv file path for each day in the last 30 days from the end_date."""
    file_paths = []
    # end_date= datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S.%f')
    current_date = end_date - timedelta(days=30)  # Set to 30 days; adjust if needed

    # Loop from the start of the 30-day range up to the end_date
    while current_date < end_date:
        year, month, day = current_date.year, current_date.month, current_date.day
        date_prefix = f"{s3_prefix}/year={year}/month={str(month).zfill(2)}/day={str(day)}/"
        print(date_prefix)
        
        # Fetch all files within the current date prefix
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=date_prefix)
        
        if 'Contents' in response:
            # Filter to include only .csv files
            csv_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.csv')]
            
            if csv_files:
                # Sort .csv files by LastModified timestamp (descending) to get the latest one first
                sorted_csv_files = sorted(csv_files, key=lambda obj: obj['LastModified'], reverse=True)
                
                # Get the path of the latest .csv file
                latest_csv_file_path = f"s3://{s3_bucket}/{sorted_csv_files[0]['Key']}"
                file_paths.append(latest_csv_file_path)
        
        current_date += timedelta(days=1)
    
    return file_paths

def load_and_concat_files(end_date):
    """Load CSV files from S3 and concatenate them into a single DataFrame."""
    dataframes = []
    # Set the S3 parameters and end_date
    s3_bucket = 'otg-fraud-dataml-prod'
    s3_prefix = 'acm/daily/temporary'
    # Convert to datetime object using the full format
        # Convert to datetime object using the full format
    end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    # Retrieve file paths
    file_paths = list_files_in_last_30_days(end_date)

    for file_path in file_paths:
        # Read CSV from S3
        df = pd.read_csv(file_path)
        
        # Append to the list of DataFrames
        dataframes.append(df)
    
    # Concatenate all data into a single DataFrame
    if dataframes is not None:
        combined_df = pd.concat(dataframes, ignore_index=True)
        # Get the current date and time components
        now = datetime.now()
        curr_year = now.year
        curr_month = now.month
        curr_day = now.day


        # Generate a timestamp_global for the file name
        timestamp_global = now.strftime('%Y%m%d%H%M%S')  # Format: YYYYMMDDHHMMSS
        s3_path = (f's3://otg-fraud-dataml-prod/acm/concat/temporary/year={curr_year}/month={curr_month}/day={curr_day}/{timestamp_global}_acm_output.csv')

        # Save results to S3
        save_to_s3(combined_df, s3_path)
    
    print("concat data processing complete")
    return s3_path

def save_to_s3(df, s3_path):
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    print(f"Successfully saved to {s3_path}")


