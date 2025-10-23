from io import StringIO
import requests

import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import numpy as np

import re
import json
import time
import os

import psycopg2
import geopandas as gpd
import folium
import tqdm
from tqdm import tqdm
import requests

from functools import wraps

from sqlalchemy import create_engine, MetaData, Table, Column, String, JSON, text
from sqlalchemy.exc import ProgrammingError
from shapely.geometry import shape
from sqlalchemy import MetaData, Table
from sqlalchemy.exc import ProgrammingError


from google.colab import userdata

AIVEN_URL = userdata.get('AIVEN')
conn_string = "postgresql://" + AIVEN_URL
engine = create_engine(conn_string)

def timer(func):
    """Print the runtime of the decorated function.

    Args:
        func: The function to time.

    Returns:
        The decorated function with timing capabilities.
    """
    @wraps(func)
    def wrapper_timer(*args, **kwargs):
        """A wrapper function that measures the execution time of the decorated function."""
        start_time = time.perf_counter()    # 1: Record start time using a precise time counter.
        value = func(*args, **kwargs)       # 2: Execute the decorated function.
        end_time = time.perf_counter()      # 3: Record end time.
        run_time = end_time - start_time    # 4: Calculate the duration of execution.
        print(f"Finished {func.__name__!r} in {run_time:.4f} secs") # 5: Print the function name and execution time.
        return value                        # 6: Return the value returned by the decorated function.
    return wrapper_timer

def download_file(url, save_path):
    """
    Downloads a file from a given URL and saves it to disk.

    Args:
        url (str): The URL of the file to download.
        save_path (str): The full path where the downloaded file will be saved.
    """
    # Sends a GET request to the specified URL
    response = requests.get(url)

    # Checks if the request was successful (status code 200)
    if response.status_code == 200:
        # Opens the file in binary write mode ('wb')
        with open(save_path, 'wb') as f:
            # Writes the binary content of the response to the file
            f.write(response.content)
        # Prints a success message
        print(f"File downloaded successfully and saved to {save_path}")
    else:
        # Prints an error message if the download failed, along with the status code
        print(f"Failed to download file. Status code: {response.status_code}")


# Assuming 'engine' is already defined elsewhere in your code,
# for example:
# engine = create_engine('postgresql://user:password@host:port/database')

def query_table(table_name):
    """
    Execute a SELECT * query on a specified database table and return the result as a pandas DataFrame.

    This function connects to the database using a pre-configured engine,
    executes a query to select all data from the given table name,
    fetches all results, and converts them into a pandas DataFrame.
    Finally, it closes the database connection.

    Args:
        table_name (str): The name of the table to query.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the data from the specified table.
                      Returns an empty DataFrame if the table is empty or doesn't exist.
    """
    try:
        conn = engine.connect() # 1: Establish a connection to the database using the global or pre-defined 'engine'.
        # 2: Construct the SQL query string. Using text() is recommended for literal SQL strings
        # to prevent potential SQL injection issues and allow SQLAlchemy to properly handle the query.
        query = text("SELECT * FROM " + table_name + "")
        result = conn.execute(query) # 3: Execute the SQL query.
        result_list = result.fetchall() # 4: Fetch all rows from the query result.
        # 5: Create a pandas DataFrame from the fetched rows and use the column names from the result keys.
        df = pd.DataFrame(result_list, columns=result.keys())
    except Exception as e:
        print(f"An error occurred while querying table {table_name}: {e}")
        df = pd.DataFrame() # 6: Return an empty DataFrame in case of an error.
    finally:
        if 'conn' in locals() and conn: # 7: Ensure the connection is closed even if an error occurs.
            conn.close()
    return df # 8: Return the resulting pandas DataFrame.

def query_sample_table(table_name):
    """
    Execute a SELECT * LIMIT 10 query on a specified database table and return the result as a pandas DataFrame.

    This function connects to the database using a pre-configured engine,
    executes a query to select the first 10 rows of all data from the given table name,
    fetches these results, and converts them into a pandas DataFrame.
    Finally, it closes the database connection. This is useful for quickly
    inspecting a table's structure and a small sample of its data.

    Args:
        table_name (str): The name of the table to query.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the first 10 rows of data
                      from the specified table. Returns an empty DataFrame if
                      the table is empty, doesn't exist, or has less than 10 rows.
    """
    try:
        conn = engine.connect() # 1: Establish a connection to the database using the global or pre-defined 'engine'.
        # 2: Construct the SQL query string using text(). The LIMIT 10 clause
        # restricts the number of rows returned to a maximum of 10.
        query = text("SELECT * FROM " + table_name + " LIMIT 10")
        result = conn.execute(query) # 3: Execute the SQL query.
        result_list = result.fetchall() # 4: Fetch all (up to 10) rows from the query result.
        # 5: Create a pandas DataFrame from the fetched rows and use the column names from the result keys.
        df = pd.DataFrame(result_list, columns=result.keys())
    except Exception as e:
        print(f"An error occurred while querying table {table_name}: {e}")
        df = pd.DataFrame() # 6: Return an empty DataFrame in case of an error.
    finally:
        if 'conn' in locals() and conn: # 7: Ensure the connection is closed even if an error occurs.
            conn.close()
    return df # 8: Return the resulting pandas DataFrame (up to 10 rows).

def query(query):
  """
  Executes a SQL query and returns the result as a pandas DataFrame.

  Args:
    query: A string containing the SQL query to execute.

  Returns:
    A pandas DataFrame containing the results of the query.
  """
  conn = engine.connect()
  query = text(query) # Wrap the query string in text()
  result = conn.execute(query)
  result_list = result.fetchall()
  df = pd.DataFrame(result_list, columns=result.keys())
  conn.close()
  return df


suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'] # 1: Define a list of standard size suffixes.

def humansize(nbytes):
    """
    Convert a size in bytes into a human-readable string representation
    using standard suffixes (B, KB, MB, GB, TB, PB).

    The function repeatedly divides the number of bytes by 1024 until
    it is less than 1024 or the largest defined suffix (PB) is reached.
    The result is formatted to two decimal places, with trailing zeros
    and decimal points removed if unnecessary, and appended with the
    appropriate suffix.

    Args:
        nbytes (int or float): The size in bytes to convert.

    Returns:
        str: A human-readable string representing the size,
             e.g., "1.23 MB", "500 B", "2.50 GB".
    """
    if nbytes == 0: # 2: Handle the edge case where the size is 0 bytes.
        return "0 B"

    i = 0 # 3: Initialize an index for the suffixes list.
    # 4: Loop as long as the number of bytes is 1024 or greater AND
    #    we haven't reached the largest suffix (PB).
    while nbytes >= 1024 and i < len(suffixes)-1:
        nbytes /= 1024. # 5: Divide the number of bytes by 1024.0 to move to the next larger unit (KB, MB, etc.).
        i += 1 # 6: Increment the suffix index to point to the next unit.

    # 7: Format the remaining number of bytes to two decimal places.
    #    rstrip('0') removes any trailing zeros (e.g., "1.50" becomes "1.5").
    #    rstrip('.') removes the decimal point if it's the last character
    #    after removing trailing zeros (e.g., "2.0" becomes "2").
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')

    # 8: Construct the final human-readable string by combining the formatted
    #    number and the appropriate suffix from the suffixes list using the index i.
    return '%s %s' % (f, suffixes[i])

def export_parquet_chunk(destination_folder, chunk_size, df):
    """
    Splits a pandas DataFrame into smaller chunks and exports each chunk
    to a separate Parquet file in a specified destination folder.

    This function is useful for handling large DataFrames that might not
    fit entirely into memory or for creating partitioned datasets. It
    iterates through the DataFrame in specified chunk sizes, saves each
    chunk as a Parquet file, and reports the completion.

    Args:
        destination_folder (str): The path to the directory where the Parquet
                                  files will be saved.
                                  NOTE: The function *overwrites* this argument
                                  internally to 'parquet_files'. Consider
                                  removing the parameter or the internal
                                  reassignment if you want to use the passed argument.
        chunk_size (int): The number of rows for each chunk.
                          NOTE: The function *overwrites* this argument
                          internally to 15000. Consider removing the
                          parameter or the internal reassignment if you
                          want to use the passed argument.
        df (pd.DataFrame): The pandas DataFrame to be split and exported.

    Raises:
        ImportError: If the 'pyarrow' engine is not installed.
        Exception: For any other errors that occur during directory creation or file writing.
    """
    # NOTE: The function ignores the passed 'destination_folder' argument
    # and hardcodes the destination folder to 'parquet_files'.
    # If you want to use the passed argument, remove the line below.
    destination_folder = 'parquet_files' # 1: Define the local variable for the destination folder path.

    # 2: Create the destination directory if it doesn't already exist.
    #    exist_ok=True prevents an error if the directory is already present.
    os.makedirs(destination_folder, exist_ok=True)

    # NOTE: The function ignores the passed 'chunk_size' argument
    # and hardcodes the chunk size to 15000.
    # If you want to use the passed argument, remove the line below.
    chunk_size = 15000 # 3: Define the local variable for the chunk size (number of rows per file).

    # 4: Iterate through the DataFrame in steps of 'chunk_size'.
    #    tqdm(range(...)) adds a progress bar to the loop, useful for large DataFrames.
    for i in tqdm(range(0, len(df), chunk_size)):
        # 5: Slice the DataFrame to get the current chunk.
        #    The slice goes from index i up to i + chunk_size.
        chunk = df[i:i + chunk_size]

        # 6: Construct the file path for the current chunk's Parquet file.
        #    os.path.join correctly handles path separators across different operating systems.
        #    f'chunk_{i//chunk_size}.parquet' creates a filename like 'chunk_0.parquet',
        #    'chunk_1.parquet', etc., based on the chunk index (i // chunk_size performs integer division).
        file_path = os.path.join(destination_folder, f'chunk_{i//chunk_size}.parquet')

        # 7: Save the current chunk to a Parquet file.
        #    to_parquet() is a pandas method for saving DataFrames in Parquet format.
        #    engine='pyarrow' specifies using the pyarrow library for writing the file.
        try:
            chunk.to_parquet(file_path, engine='pyarrow')
        except ImportError:
            print("Error: pyarrow engine is required to write Parquet files. Please install it using `!pip install pyarrow`.")
            # Depending on desired behavior, you might want to raise the error or exit the function
            raise
        except Exception as e:
             print(f"An error occurred while writing chunk {i//chunk_size} to {file_path}: {e}")
             # Handle other potential file writing errors

    # 8: Print a confirmation message indicating where the files were saved.
    print(f"DataFrame has been split into Parquet files and saved to {destination_folder}")

def export_to_postgres_chunk_replace_first(df, table_name, chunk_size=10000):
    """
    Exports a pandas DataFrame to a PostgreSQL table in chunks,
    replacing the table with the first chunk and appending subsequent chunks.

    Args:
        df (pd.DataFrame): The DataFrame to export.
        table_name (str): The name of the target PostgreSQL table.
        engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine object.
        chunk_size (int): The number of rows per chunk.
    """
    for i in tqdm(range(0, len(df), chunk_size)):
        chunk = df[i:i + chunk_size]
        if i == 0:
            # For the first chunk, replace the table if it exists
            chunk.to_sql(table_name, engine, if_exists='replace', index=False)
        else:
            # For subsequent chunks, append to the existing table
            chunk.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"DataFrame has been exported to {table_name} in chunks.")

def drop_table(table_name, id_column_name):
    """
    Drops a specific table from the database if it exists.

    This function defines a SQLAlchemy Table object using the provided table name.
    It then attempts to drop this table from the database using the global
    SQLAlchemy engine. It handles the case where the table might not exist
    gracefully by catching the ProgrammingError that typically occurs in such scenarios.

    Args:
        table_name (str): The name of the table to drop.
        id_column_name (str): The name of the primary or a key column for the table definition.
                              Note: Defining columns here is often not strictly necessary
                              just for dropping, but it completes the Table object definition.
    """
    # 1: Create a MetaData object. MetaData is a container object that keeps
    #    together many different features of a database schema (tables, etc.).
    metadata = MetaData()

    # 2: Define a Table object. This object represents the table structure
    #    in terms of SQLAlchemy's schema definition.
    #    We pass the table_name, the metadata object, and at least one Column definition.
    #    Defining columns accurately here isn't always essential just for dropping
    #    unless there are complex dependencies, but it's good practice to define
    #    at least a key column if known.
    table = Table(
        table_name, metadata,
        Column(id_column_name, String), # Define a column; 'id_column_name' is used as the column name, String as its type.
        # Add other Column definitions here if you know the table structure
        # and want a more complete Table object representation.
    )

    try:
        # 3: Attempt to drop the table (or tables defined in the metadata).
        #    metadata.drop_all(engine) attempts to drop *all* tables defined
        #    within this specific metadata object using the given engine.
        #    Since only one table is defined in this metadata object, it will
        #    attempt to drop that single table.
        table.drop(engine, checkfirst=True) # Using table.drop() is often more direct for a single table and includes a check.
        print(f"Table '{table_name}' dropped successfully or did not exist.")

    except ProgrammingError as e:
        # 4: Catch a ProgrammingError. This error often occurs in databases
        #    when trying to drop a table that does not exist, or due to permissions issues.
        #    We print a message indicating a potential issue, which is expected
        #    if the table wasn't there to begin with.
        print(f"An error occurred while dropping table '{table_name}': {e}")
        # You might want to check the error message content to distinguish
        # between "table not found" and other programming errors if needed.

    # Note: In this specific function, there's no connection object explicitly
    # created with `engine.connect()` that needs to be closed with `conn.close()`.
    # The `drop_all` or `drop` methods of MetaData/Table handle their own
    # connection management internally based on the engine.


def drop_table2(table_name):
    """
    Drops a specific table from the database if it exists.

    Args:
        table_name (str): The name of the table to drop.
    """
    # Create a MetaData object
    metadata = MetaData()

    # Define a Table object without specifying columns
    table = Table(table_name, metadata)

    try:
        # Attempt to drop the table
        table.drop(engine, checkfirst=True)
        print(f"Table '{table_name}' dropped successfully or did not exist.")
    except ProgrammingError as e:
        # Catch a ProgrammingError
        print(f"An error occurred while dropping table '{table_name}': {e}")
   
def tables_list():
    """
    Connects to the database using a SQLAlchemy engine and lists all
    user-defined tables.

    It queries the database's catalog (specifically pg_catalog for PostgreSQL,
    though the query structure might vary slightly for other database types)
    to retrieve information about all tables, excluding system tables from
    'pg_catalog' and 'information_schema'. The names of the tables are
    then printed to the console.

    Assumes a PostgreSQL database based on the query structure (`pg_catalog.pg_tables`).
    """
    connection = None # Initialize connection to None

    try:
        # 1: Establish a connection to the database using the pre-configured 'engine'.
        connection = engine.connect()

        # 2: Define the SQL query using text() for robustness.
        #    This query selects all columns from the pg_catalog.pg_tables view.
        #    It filters out tables from the system schemas 'pg_catalog' and
        #    'information_schema' to list only user-created tables.
        #    The results are ordered by table name.
        #    NOTE: This query is specific to PostgreSQL databases.
        #          For other databases (like SQLite, MySQL, etc.), you would
        #          need a different query to list tables.
        query = text("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema' ORDER BY tablename")

        # 3: Execute the SQL query against the database connection.
        result = connection.execute(query)

        # 4: Fetch all rows returned by the query. Each row typically represents
        #    information about a table (like table name, schema name, etc.).
        result_list = result.fetchall()

        # 5: Iterate through the fetched rows and print each one.
        #    The exact format of the printed output will depend on the columns
        #    selected by the '*' in the query and the database system.
        #    For a simple list of names, you might prefer to select just the
        #    'tablename' column and print `row.tablename`.
        print("Tables in the database (excluding system tables):")
        if result_list:
            for row in result_list:
                print(row)
        else:
            print("No user tables found.")

    except Exception as e:
        # 6: Handle any errors that occur during the connection or query execution.
        print(f"An error occurred while listing tables: {e}")

    finally:
        # 7: Ensure the database connection is closed, regardless of whether
        #    an error occurred. Check if the connection was successfully established.
        if connection is not None:
            connection.close()

def return_tables_list():
    """
    Connects to the database using a SQLAlchemy engine and lists all
    user-defined tables.

    It queries the database's catalog (specifically pg_catalog for PostgreSQL,
    though the query structure might vary slightly for other database types)
    to retrieve information about all tables, excluding system tables from
    'pg_catalog' and 'information_schema'. The names of the tables are
    then printed to the console.

    Assumes a PostgreSQL database based on the query structure (`pg_catalog.pg_tables`).
    """
    connection = None # Initialize connection to None

    try:
        # 1: Establish a connection to the database using the pre-configured 'engine'.
        connection = engine.connect()

        # 2: Define the SQL query using text() for robustness.
        #    This query selects all columns from the pg_catalog.pg_tables view.
        #    It filters out tables from the system schemas 'pg_catalog' and
        #    'information_schema' to list only user-created tables.
        #    The results are ordered by table name.
        #    NOTE: This query is specific to PostgreSQL databases.
        #          For other databases (like SQLite, MySQL, etc.), you would
        #          need a different query to list tables.
        query = text("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema' ORDER BY tablename")

        # 3: Execute the SQL query against the database connection.
        result = connection.execute(query)

        # 4: Fetch all rows returned by the query. Each row typically represents
        #    information about a table (like table name, schema name, etc.).
        result_list = result.fetchall()

        # 5: Iterate through the fetched rows and print each one.
        #    The exact format of the printed output will depend on the columns
        #    selected by the '*' in the query and the database system.
        #    For a simple list of names, you might prefer to select just the
        #    'tablename' column and print `row.tablename`.

        if result_list:
          return result_list
        else:
            print("No user tables found.")

    except Exception as e:
        # 6: Handle any errors that occur during the connection or query execution.
        print(f"An error occurred while listing tables: {e}")

    finally:
        # 7: Ensure the database connection is closed, regardless of whether
        #    an error occurred. Check if the connection was successfully established.
        if connection is not None:
            connection.close()

def export_to_postgre(df, table_name):
    """
    Exports a pandas DataFrame to a specified database table using a SQLAlchemy engine.

    If the table already exists, it will be replaced with the data from the DataFrame.
    The DataFrame index is not included in the table.

    Args:
        df (pd.DataFrame): The pandas DataFrame to export.
        table_name (str): The name of the table in the database where the data
                          will be saved.

    Raises:
        Exception: If any error occurs during the database export process
                   (e.g., connection issues, permissions, data type mismatches).
    """
    try:
        # 1: Use the pandas to_sql method to write records stored in a DataFrame
        #    to a SQL database.
        #    - table_name: The name of the target SQL table.
        #    - engine: The SQLAlchemy engine object that manages the database connection.
        #    - if_exists='replace': Specifies the behavior if the table already exists.
        #      'replace' means the existing table will be dropped and a new one
        #      created with the DataFrame's data. Other options include 'fail'
        #      (raise an error) and 'append' (add rows to the existing table).
        #    - index=False: Instructs pandas *not* to write the DataFrame's index
        #      as a column in the SQL table.
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        # 2: Print a success message confirming that the data was saved.
        print(f"Données sauvegardées dans la table '{table_name}'") # Using f-string for clarity

    except Exception as e:
        # 3: Catch any general exceptions that might occur during the export process.
        #    This could be due to database connection problems, lack of write permissions,
        #    incompatible data types between the DataFrame and potential existing table
        #    schema (if 'replace' still encounters issues or if 'append' was used), etc.
        print(f"Une erreur s'est produite lors de la sauvegarde des données dans la table '{table_name}': {e}")
        # Depending on the requirement, you might want to re-raise the exception
        # or handle it differently (e.g., log the error).
        # raise e


# Edition de carte
def map_generation(df,id,lib,geo):
  m = folium.Map(location=[48.858885,2.34694], zoom_start=6, tiles="CartoDB positron")
  df = df[[id,lib,geo]].drop_duplicates()

  for index, row in tqdm(df.iterrows()):
      sim_geo = gpd.GeoSeries(row[geo]).simplify(tolerance=0.001)
      geo_j = sim_geo.to_json()

      geo_j = folium.GeoJson(data=geo_j,
                            style_function = lambda x: {"fillColor": "blue"}
                            #,highlight_function= lambda feat: {'fillColor': 'red'}
                            )

      folium.Popup(row[id] + " - " + row[lib]).add_to(geo_j)

      geo_j.add_to(m)

  display(m)

def return_tables_list():
    """
    Connects to the database using a SQLAlchemy engine and lists all
    user-defined tables.

    It queries the database's catalog (specifically pg_catalog for PostgreSQL,
    though the query structure might vary slightly for other database types)
    to retrieve information about all tables, excluding system tables from
    'pg_catalog' and 'information_schema'. The names of the tables are
    then printed to the console.

    Assumes a PostgreSQL database based on the query structure (`pg_catalog.pg_tables`).
    """
    connection = None # Initialize connection to None

    try:
        # 1: Establish a connection to the database using the pre-configured 'engine'.
        connection = engine.connect()

        # 2: Define the SQL query using text() for robustness.
        #    This query selects all columns from the pg_catalog.pg_tables view.
        #    It filters out tables from the system schemas 'pg_catalog' and
        #    'information_schema' to list only user-created tables.
        #    The results are ordered by table name.
        #    NOTE: This query is specific to PostgreSQL databases.
        #          For other databases (like SQLite, MySQL, etc.), you would
        #          need a different query to list tables.
        query = text("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema' ORDER BY tablename")

        # 3: Execute the SQL query against the database connection.
        result = connection.execute(query)

        # 4: Fetch all rows returned by the query. Each row typically represents
        #    information about a table (like table name, schema name, etc.).
        result_list = result.fetchall()

        # 5: Iterate through the fetched rows and print each one.
        #    The exact format of the printed output will depend on the columns
        #    selected by the '*' in the query and the database system.
        #    For a simple list of names, you might prefer to select just the
        #    'tablename' column and print `row.tablename`.

        if result_list:
          return result_list
        else:
            print("No user tables found.")

    except Exception as e:
        # 6: Handle any errors that occur during the connection or query execution.
        print(f"An error occurred while listing tables: {e}")

    finally:
        # 7: Ensure the database connection is closed, regardless of whether
        #    an error occurred. Check if the connection was successfully established.
        if connection is not None:
            connection.close()
