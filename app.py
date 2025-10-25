
#streamlit run /workspaces/stats2/app.py

from io import StringIO
import requests

import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import numpy as np
import streamlit as st

import re
import json
import time
import os

import psycopg2
import geopandas as gpd
import folium
import requests

from functools import wraps

from sqlalchemy import create_engine, MetaData, Table, Column, String, JSON, text
from sqlalchemy.exc import ProgrammingError
from shapely.geometry import shape
from sqlalchemy import MetaData, Table
from sqlalchemy.exc import ProgrammingError

import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore", category=pd.errors.SettingWithCopyWarning)

AIVEN_URL ='avnadmin:AVNS_8Nfkstx4GWwAGOxp7OB@pg-11490ac3-jeancabouat-2aa9.j.aivencloud.com:23133/defaultdb?sslmode=require'
conn_string = "postgresql://" + AIVEN_URL
engine = create_engine(conn_string)

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

# Edition de carte
def map_generation(df,id,lib,geo):
  m = folium.Map(location=[48.858885,2.34694], zoom_start=6, tiles="CartoDB positron")
  df = df[[id,lib,geo]].drop_duplicates()

  for index, row in df.iterrows():
      sim_geo = gpd.GeoSeries(row[geo]).simplify(tolerance=0.001)
      geo_j = sim_geo.to_json()

      geo_j = folium.GeoJson(data=geo_j,
                            style_function = lambda x: {"fillColor": "blue"}
                            #,highlight_function= lambda feat: {'fillColor': 'red'}
                            )

      folium.Popup(row[id] + " - " + row[lib]).add_to(geo_j)

      geo_j.add_to(m)

  display(m)



# ---------- 1️⃣ Load / cache the raw data ----------
@st.cache_data(show_spinner=False)
def load_insee_ref() -> pd.DataFrame:
    """Pull the full table once and keep it in Streamlit's cache."""
    df = query("SELECT * FROM insee_ref WHERE id_dep ='78'")  # fetch only the needed cols
    df = df.drop_duplicates()                                   # tiny safety net
    return df.sort_values(['libReg', 'libDep', 'libCom','id_com'],
                          ascending=[True, True, True, True])         # one single sort

def load_geo(nom_commune) -> pd.DataFrame:
    """Pull the full table once and keep it in Streamlit's cache."""
    query_str = """SELECT * FROM com_geo WHERE "libCom" = '""" + nom_commune + """'"""
    print(query_str)
    df = query("SELECT * FROM com_geo WHERE """"libCom"""" = '" + nom_commune + "'")  # fetch only the needed cols
    df = df.drop_duplicates()                                   # tiny safety net
    return df.sort_values(['id_com','libCom','geoCom'],
                          ascending=[True, True, True])         # one single sort

df_insee_ref = load_insee_ref()

# ---------- 2️⃣ Build unique, *sorted* option lists ----------
regions = df_insee_ref['libReg'].unique().tolist()

with st.sidebar:
    st.header("Sélection géographique")
    # ---------- 3️⃣ Region selectbox ----------
    option_reg = st.selectbox(
        "Région:",
        options=regions,
        index=0,
        placeholder="Sélectionner une région",
        key="reg_selectbox"
    )

    # ---------- 4️⃣ Department selectbox (filtered) ----------
    # Filter once (no extra sort)
    filtered_dep = df_insee_ref[df_insee_ref['libReg'] == option_reg]
    dep_options = filtered_dep['libDep'].unique().tolist()

    option_dep = st.selectbox(
        "Département:",
        options=dep_options,
        index=0,
        placeholder="Sélectionner un département",
        key="dep_selectbox"
    )

    # ---------- 5️⃣ Commune selectbox (filtered) ----------
    filtered_com = filtered_dep[filtered_dep['libDep'] == option_dep]
    com_options = filtered_com['libCom'].unique().tolist()

    option_com = st.selectbox(
        "Commune:",
        options=com_options,
        index=0,
        placeholder="Sélectionner une commune",
        key="com_selectbox"
    )
df_com = filtered_com[filtered_com['libCom'] == option_com]

lib_com = df_com['libCom'].values[0]
df_geo_com = load_geo(lib_com)    
st.write(f"Vous avez sélectionné la commune de **{option_com}**, dans le département de **{option_dep}**, en région **{option_reg}**.")

print(df_geo_com.head())
# Affichage de la carte
#map_generation(df_geo_com,'id_com','libCom','geoCom')