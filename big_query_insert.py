from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
from datetime import datetime

# pip install pandas openpyxl

# Google BigQuery Configuration
GOOGLE_APPLICATION_CREDENTIALS = os.get('GOOGLE_APPLICATION_CREDENTIALS')
GBQPROJECT_ID = os.get('GBQPROJECT_ID')
GBQPROJECT_DATASET = os.get('GBQPROJECT_DATASET')

def safe_convert_for_parquet(df):
    """
    Convert all columns to types compatible with Parquet/PyArrow.
    """
    for column in df.columns:
        try:
            # Get sample of non-null values
            sample = df[column].dropna().head(1)
            if len(sample) == 0:
                df[column] = df[column].astype(str)
                continue

            # Get column data type
            dtype = df[column].dtype
            sample_value = sample.iloc[0]

            # Handle different data types
            if pd.api.types.is_float_dtype(dtype):
                if df[column].dropna().apply(lambda x: x.is_integer()).all():
                    df[column] = df[column].astype('Int64')  # Nullable integer
                else:
                    df[column] = df[column].astype(str)  # Convert float to string

            elif isinstance(dtype, pd.CategoricalDtype):  # FIXED: Use isinstance()
                df[column] = df[column].astype(str)

            elif pd.api.types.is_object_dtype(dtype):
                # Handle lists, dicts, sets, tuples
                if isinstance(sample_value, (list, dict, set, tuple)):
                    df[column] = df[column].apply(lambda x: str(x) if x is not None else None)

                # Convert to datetime if applicable
                try:
                    converted_col = pd.to_datetime(df[column], format="%Y-%m-%d %H:%M:%S", errors='coerce')
                    if converted_col.notna().sum() > 0:  # Check if valid datetime values exist
                        df[column] = converted_col.dt.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        df[column] = df[column].astype(str)  # Keep as string if all conversions failed
                except Exception:
                    df[column] = df[column].astype(str)

            elif pd.api.types.is_datetime64_dtype(dtype):
                df[column] = df[column].dt.strftime('%Y-%m-%d %H:%M:%S')

            elif pd.api.types.is_timedelta64_dtype(dtype):
                df[column] = df[column].apply(lambda x: str(x.total_seconds()) if pd.notnull(x) else None)

            elif pd.api.types.is_bool_dtype(dtype):
                pass  # Keep boolean as is

            elif pd.api.types.is_integer_dtype(dtype):
                pass  # Keep integer as is

            else:
                df[column] = df[column].astype(str)

        except Exception as e:
            print(f"Warning: Error converting column {column}. Converting to string. Error: {str(e)}")
            df[column] = df[column].astype(str)

    return df


def get_bigquery_type(pandas_dtype):
    """
    Map pandas dtypes to BigQuery data types.
    """
    if pd.api.types.is_datetime64_dtype(pandas_dtype):
        return "TIMESTAMP"
    elif pd.api.types.is_bool_dtype(pandas_dtype):
        return "BOOLEAN"
    elif pd.api.types.is_integer_dtype(pandas_dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(pandas_dtype):
        return "FLOAT"
    else:
        return "STRING"


def insert_database(table_name, data_frame):
    try:
        # Create a copy to avoid modifying the original DataFrame
        df = data_frame.copy()

        # Clean unnamed columns
        df.drop(
            df.columns[df.columns.str.contains('unnamed', case=False)],
            axis=1, inplace=True
        )

        # Convert all columns to Parquet-compatible types
        df = safe_convert_for_parquet(df)

        # Create BigQuery client
        bq_client = bigquery.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)

        # Construct full table ID
        table_id = f"{GBQPROJECT_ID}.{GBQPROJECT_DATASET}.{table_name}"

        try:
            # Check if the table exists
            bq_client.get_table(table_id)
        except Exception:
            # Table doesn't exist, create it with schema
            schema = [
                bigquery.SchemaField(col, get_bigquery_type(df[col].dtype))
                for col in df.columns
            ]

            # Add an auto-incrementing ID column
            schema.insert(0, bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"))

            # Add created_at timestamp column
            schema.append(bigquery.SchemaField(
                "created_at", "TIMESTAMP", mode="REQUIRED",
                default_value_expression="CURRENT_TIMESTAMP()"
            ))

            # Create the table
            table = bigquery.Table(table_id, schema=schema)
            bq_client.create_table(table)

        # Fetch the current max ID
        query = f"SELECT IFNULL(MAX(id), 0) FROM `{table_id}`"
        query_job = bq_client.query(query)
        max_id = list(query_job.result())[0][0]  # Extract the max id
        df.insert(0, "id", range(max_id + 1, max_id + 1 + len(df)))  # Generate sequential IDs

        # Sort DataFrame before inserting (ensures sequential order)
        df = df.sort_values(by="id").reset_index(drop=True)
        # Configure load job
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.PARQUET
        )

        # Load DataFrame to BigQuery
        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for completion

        return f"Successfully inserted {len(df)} rows into {table_name}"

    except Exception as e:
        print(f"Error: {str(e)}")
        return e
    
    
    
# Read Excel file
print("Current Working Directory:", os.getcwd())

def batch_excel_to_bigquery(excel_files):
    try:
        
        for excel_file in excel_files:
            table_name = os.path.splitext(excel_file)[0].replace('.xlsx', '')

            data_frame = pd.read_excel(excel_file)
            insert_database(table_name, data_frame)
    except Exception as e:
        print(e)
        
# Excel files list
excels = [
    'Example.xlsx'
]

csv_file_specific = [
    'Example.csv'
]
# Execute batch import
# batch_excel_to_bigquery(excels)

def batch_csv_to_bigquery(csv_files):
    try:
        for csv_file in csv_files:
            table_name = 'hospital_courses'
            csv_name = os.path.splitext(csv_file)[0].replace('.csv', '')
            if csv_name == 'Kensington_hospital':
                facility_name = 'Kensington'
            else:
                facility_name = 'Gateway'
            data_frame = pd.read_csv(csv_file)
            data_frame['facility_name'] = facility_name
            insert_database(table_name, data_frame)
    except Exception as e:
        print(e)

batch_csv_to_bigquery(csv_file_specific)