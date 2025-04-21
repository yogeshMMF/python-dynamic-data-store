import pandas as pd
import os
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# pip install pandas openpyxl mysql-connector-python


MYSQL_CONFIG = {
    'host': os.get('MYSQL_HOST'),
    'user': os.get('MYSQL_USER'),
    'password': os.get('MYSQL_PASSWORD'),
    'database': os.get('MYSQL_DATABASE')
}

def safe_convert_for_mysql(df):
    """
    Convert all columns to types compatible with MySQL.
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
                    # Keep as float for MySQL DOUBLE type
                    pass

            elif isinstance(dtype, pd.CategoricalDtype):
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
                # MySQL uses TINYINT(1) for boolean
                pass

            elif pd.api.types.is_integer_dtype(dtype):
                pass  # Keep integer as is

            else:
                df[column] = df[column].astype(str)

        except Exception as e:
            print(f"Warning: Error converting column {column}. Converting to string. Error: {str(e)}")
            df[column] = df[column].astype(str)

    return df


def get_mysql_type(pandas_dtype, column_values):
    """
    Map pandas dtypes to MySQL data types.
    """
    if pd.api.types.is_datetime64_dtype(pandas_dtype):
        return "DATETIME"
    elif pd.api.types.is_bool_dtype(pandas_dtype):
        return "TINYINT(1)"
    elif pd.api.types.is_integer_dtype(pandas_dtype):
        max_val = column_values.max() if not column_values.empty else 0
        # Choose appropriate integer type based on size
        if max_val <= 127:
            return "TINYINT"
        elif max_val <= 32767:
            return "SMALLINT"
        elif max_val <= 8388607:
            return "MEDIUMINT"
        elif max_val <= 2147483647:
            return "INT"
        else:
            return "BIGINT"
    elif pd.api.types.is_float_dtype(pandas_dtype):
        return "DOUBLE"
    else:
        # For strings, determine max length to choose VARCHAR or TEXT
        non_null_values = column_values.dropna()
        if len(non_null_values) > 0:
            max_length = non_null_values.astype(str).str.len().max()
            if max_length <= 65535:
                return "TEXT"
            elif max_length <= 16777215:
                return "MEDIUMTEXT"
            else:
                return "LONGTEXT"
        return "VARCHAR(255)"  # Default


def create_mysql_connection():
    """
    Create and return a MySQL connection.
    """
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


def insert_database(table_name, data_frame):
    """
    Insert dataframe into MySQL table with dynamic schema creation.
    """
    try:
        # Create a copy to avoid modifying the original DataFrame
        df = data_frame.copy()

        # Clean unnamed columns
        df.drop(
            df.columns[df.columns.str.contains('unnamed', case=False)],
            axis=1, inplace=True
        )

        # Convert all columns to MySQL-compatible types
        df = safe_convert_for_mysql(df)

        # Create MySQL connection
        connection = create_mysql_connection()
        if not connection:
            return "Failed to connect to MySQL database"

        cursor = connection.cursor()

        # Check if table exists
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        table_exists = cursor.fetchone()

        if not table_exists:
            # Create table with schema
            column_definitions = []
            for col in df.columns:
                mysql_type = get_mysql_type(df[col].dtype, df[col])
                # MySQL doesn't like certain characters in column names
                col_name = col.replace(' ', '_').replace('-', '_').replace('.', '_')
                column_definitions.append(f"`{col_name}` {mysql_type}")

            # Add an auto-incrementing ID column and created_at timestamp
            create_table_sql = f"""
            CREATE TABLE `{table_name}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                {', '.join(column_definitions)},
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_sql)
            connection.commit()
        else:
            # Table exists, check if we need to add new columns
            cursor.execute(f"DESCRIBE `{table_name}`")
            existing_columns = {row[0] for row in cursor.fetchall()}
            
            for col in df.columns:
                # MySQL doesn't like certain characters in column names
                col_name = col.replace(' ', '_').replace('-', '_').replace('.', '_')
                if col_name not in existing_columns and col_name != 'id' and col_name != 'created_at':
                    mysql_type = get_mysql_type(df[col].dtype, df[col])
                    alter_table_sql = f"ALTER TABLE `{table_name}` ADD COLUMN `{col_name}` {mysql_type}"
                    cursor.execute(alter_table_sql)
                    connection.commit()

        # Fetch the current max ID
        cursor.execute(f"SELECT IFNULL(MAX(id), 0) FROM `{table_name}`")
        max_id = cursor.fetchone()[0]  # Extract the max id
        
        # Insert data in batches
        batch_size = 1000  # Adjust based on your needs
        for start_idx in range(0, len(df), batch_size):
            end_idx = min(start_idx + batch_size, len(df))
            batch_df = df.iloc[start_idx:end_idx]
            
            # Generate placeholders and values for the insert query
            placeholders = ', '.join(['%s'] * len(batch_df.columns))
            columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in batch_df.columns]
            columns_str = ', '.join([f'`{col}`' for col in columns])
            
            insert_sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
            
            # Prepare data for insertion
            data_values = [tuple(row) for row in batch_df.values]
            
            # Execute insert
            cursor.executemany(insert_sql, data_values)
            connection.commit()

        cursor.close()
        connection.close()
        return f"Successfully inserted {len(df)} rows into {table_name}"

    except Error as e:
        print(f"MySQL Error: {str(e)}")
        return str(e)
    except Exception as e:
        print(f"Error: {str(e)}")
        return str(e)


# Read Excel file
print("Current Working Directory:", os.getcwd())

def batch_excel_to_mysql(excel_files):
    """
    Batch import Excel files to MySQL.
    """
    try:
        for excel_file in excel_files:
            table_name = os.path.splitext(excel_file)[0].replace('.xlsx', '')
            # Clean table name for MySQL (remove invalid characters)
            table_name = table_name.replace('.', '_').replace('-', '_').replace(' ', '_')
            
            print(f"Processing {excel_file} into table {table_name}...")
            data_frame = pd.read_excel(excel_file)
            result = insert_database(table_name, data_frame)
            print(result)
    except Exception as e:
        print(f"Error in batch Excel import: {e}")
        
        
        
def batch_csv_to_mysql(csv_files):
    """
    Batch import CSV files to MySQL with facility name.
    """
    try:
        for csv_file in csv_files:
            table_name = 'payout_summary'
            # csv_name = os.path.splitext(csv_file)[0].replace('.csv', '')
            # if csv_name == 'Kensington_hospital':
            #     facility_name = 'Kensington'
            # else:
            #     facility_name = 'Gateway'
            
            # print(f"Processing {csv_file} with facility {facility_name}...")
            data_frame = pd.read_csv(csv_file)
            # data_frame['facility_name'] = facility_name
            result = insert_database(table_name, data_frame)
            print(result)
    except Exception as e:
        print(f"Error in batch CSV import: {e}")


# Excel files list
excels = [
    'Example.xlsx'
]

csv_file_specific = [
    'Example.csv'
]

# Uncomment these lines to run the import
# batch_excel_to_mysql(excels)
# batch_csv_to_mysql(csv_file_specific)


# Example usage:
if __name__ == "__main__":
    print("Excel/CSV to MySQL Converter")
    print("1. Import Excel files")
    print("2. Import CSV files")
    print("3. Import both Excel and CSV files")
    choice = input("Enter your choice (1-3): ")
    
    if choice == '1':
        batch_excel_to_mysql(excels)
    elif choice == '2':
        batch_csv_to_mysql(csv_file_specific)
    elif choice == '3':
        batch_excel_to_mysql(excels)
        batch_csv_to_mysql(csv_file_specific)
    else:
        print("Invalid choice")