# Databricks notebook source
import psycopg2
import pandas as pd
import os

# COMMAND ----------

from get_api_key import import_json as read_db_secret

# COMMAND ----------

db_info = read_db_secret("db_config/database_config.json")

host = db_info["host"]
user = db_info["user"]
password = db_info["password"]
db_port = db_info["port"]
db_name = db_info["db_name"]


# COMMAND ----------

conn_string = f"host={host} user={user} dbname={db_name} password={password}"
conn = psycopg2.connect(conn_string)
print("Connection established")

# COMMAND ----------

# See https://www.psycopg.org/docs/usage.html for usage of psycopg2
# Open a cursor to the database - remember to close this after you have done everything!

cur = conn.cursor()
# If something goes wrong (you get transactionerror)- use the command below
cur.execute("rollback") 

# COMMAND ----------

def get_number_type(num):
    try:
        num = float(num)
        result = isinstance(num, (int, float)) and num.is_integer()
    except ValueError:
        result = False

    if result:
        num_type = "int" if num.is_integer() else "float"
        return num_type
    else:
        return False


# COMMAND ----------

from datetime import datetime
#returns if a string is a date or not
def is_date(s, date_formats=['%d-%m-%Y', '%m-%d-%Y', '%Y-%d-%m', '%Y-%m-%d']):
    for date_format in date_formats:
        try:
            datetime.strptime(s, date_format)
            return True
        except ValueError:
            pass
    return False


# COMMAND ----------

def remove_extension(file_path):
    # Find the last occurrence of '.' in the file path
    dot_index = file_path.rfind('.')

    # If '.' is found, slice the string to remove the extension
    if dot_index != -1:
        file_name_without_extension = file_path[:dot_index]
    else:
        # If '.' is not found, keep the original string
        file_name_without_extension = file_path

    return file_name_without_extension

# COMMAND ----------

# Function for converting datetime to date
def convert_datetime_columns_to_date(df):
    for column in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[column]):
            df[column] = df[column].dt.date

# COMMAND ----------

def estimate_column_size(column, df, debug_print=False):
    max_value = df[column].max()
    if(debug_print):print(f"Maxvalue was {max_value} for {column}")

    if pd.api.types.is_string_dtype(df[column]):  # Check if the column contains strings
        max_length = df[column].str.len().max()
        return f"{column} VARCHAR({max_length})"
    elif pd.api.types.is_float_dtype(df[column]):  # Check if the column contains floats
        if max_value < 1e-3:
            return f"{column} FLOAT"
        elif max_value < 1e38:
            return f"{column} REAL"
        else:
            return f"{column} DOUBLE"
    elif pd.api.types.is_integer_dtype(df[column]):  # Check if the column contains integers
        if max_value < 256:
            return f"{column} TINYINT"
        elif max_value < 32768:
            return f"{column} SMALLINT"
        elif max_value < 2147483648:
            return f"{column} INT"
        else:
            return f"{column} BIGINT"
    elif pd.api.types.is_datetime64_any_dtype(df[column]):  # Check if the column contains datetime values
        return f"{column} DATE"
    #If for some reason the formatting is neither of these, set it as text as a failsafe

    return f"{column} TEXT"


# COMMAND ----------

# #Read files from the silver directory, and create tables with column name from the first row and populate each row later in the csv file
def create_and_populate_tables(directory_path, db_name, user, password, host, port, table_prefix, debug_print=False):
    # Connect to PostgreSQL database
    conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=host, port=port)
    cursor = conn.cursor()
    # Iterate through CSV files in the specified directory
    for filename in os.listdir(directory_path):
        if filename.endswith(".json"): #For every CSV file
            json_path = os.path.join(directory_path, filename)
            
            #Remove the filename, will be used to create the column names
            filename_no_ext = remove_extension(filename)
            df = pd.read_json(json_path)

            #Convert datetime to date, since pd always adds a timestamp for some reason
            # convert_datetime_columns_to_date(df)

            # Generate SQL query string
            table_name = filename_no_ext # uses the table name that is the filename but with no file extension
            table_name_full = table_prefix + table_name

            #remove this later
            cursor.execute(f"DROP TABLE IF EXISTS {table_name_full};")

            sql_query = f"CREATE TABLE IF NOT EXISTS {table_name_full} (\n"

            # FUNCTION FOR ESTIMATING HOW LARGE EACH DATA TYPE NEEDS TO BE
            # Loop through all values in all tables, and store the largest value for text as varchar, float and int types
            # Add max values for text variable, float, int for each column. Where the float type corresponds with the largest value of that type for that column
            # 

            
            data_types = df.dtypes # set the type to be the types from the dataframe
                    
            for column in df.columns:
                column_definition = estimate_column_size(column, df, debug_print)
                if column_definition is not None:
                    sql_query += "    " + column_definition + ",\n"

            #We assume all the dates we get are in datetime
            convert_datetime_columns_to_date(df)


            # Remove the trailing comma and newline character
            sql_query = sql_query.rstrip(",\n")

            # Close the CREATE TABLE statement
            sql_query += "\n);"
            if(debug_print):print("SQL creation:", sql_query)
            cursor.execute(sql_query)
            # Populating the database
            columns = df.columns # set the columns to be the columns from the dataframe
            for index, row in df.iterrows():
                #row is a list, for value in row joins the values in the row with a comma
                # row1[1,John,25]
                #values 1, John, 25
                values = ", ".join([f"'{str(value)}'" for value in row])

                insert_query = f"INSERT INTO {table_name_full} ({', '.join(columns)}) VALUES ({values});"
                if(debug_print):print("Inserting ",insert_query)
                cursor.execute(insert_query)

    # Commit and close the connection
    conn.commit()
    conn.close()

# COMMAND ----------

directory_path = 'silver/'
table_prefix = 'emissions_'
debug_print = False
create_and_populate_tables(directory_path, db_name, user, password, host, db_port, table_prefix, debug_print)