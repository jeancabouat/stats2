
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

st.set_page_config(layout="wide")

# Load the HTML file
def read_html_file(filename):
    with open(filename, 'r') as f:
        return f.read()

AIVEN_URL ='avnadmin:AVNS_8Nfkstx4GWwAGOxp7OB@pg-11490ac3-jeancabouat-2aa9.j.aivencloud.com:23133/defaultdb?sslmode=require'
conn_string = "postgresql://" + AIVEN_URL
engine = create_engine(conn_string)

# Assuming 'engine' is already defined elsewhere in your code,
# for example:
# engine = create_engine('postgresql://user:password@host:port/database')

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

# ---------- 1️⃣ Load / cache the raw data ----------
@st.cache_data(show_spinner=False)
def load_insee_ref() -> pd.DataFrame:
    """Pull the full table once and keep it in Streamlit's cache."""
    df = query("SELECT * FROM insee_ref WHERE id_dep ='78'")  # fetch only the needed cols
    df = df.drop_duplicates()                                   # tiny safety net
    return df.sort_values(['lib_reg', 'lib_dep', 'lib_com','id_com'],
                          ascending=[True, True, True, True])         # one single sort

def load_geo(nom_commune) -> pd.DataFrame:
    """Pull the full table once and keep it in Streamlit's cache."""
    df = query("SELECT * FROM com_geo WHERE """"lib_com"""" = '" + nom_commune + "' AND id_dep ='78'")  # fetch only the needed cols
    df = df.drop_duplicates()                                   # tiny safety net
    return df.sort_values(['id_com','lib_com','geo_com'],
                          ascending=[True, True, True])         # one single sort

df_insee_ref = load_insee_ref()

# ---------- 2️⃣ Build unique, *sorted* option lists ----------
regions = df_insee_ref['lib_reg'].unique().tolist()

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
    filtered_dep = df_insee_ref[df_insee_ref['lib_reg'] == option_reg]
    dep_options = filtered_dep['lib_dep'].unique().tolist()

    option_dep = st.selectbox(
        "Département:",
        options=dep_options,
        index=0,
        placeholder="Sélectionner un département",
        key="dep_selectbox"
    )

    # ---------- 5️⃣ Commune selectbox (filtered) ----------
    filtered_com = filtered_dep[filtered_dep['lib_dep'] == option_dep]
    com_options = filtered_com['lib_com'].unique().tolist()

    option_com = st.selectbox(
        "Commune:",
        options=com_options,
        index=0,
        placeholder="Sélectionner une commune",
        key="com_selectbox"
    )
df_com = filtered_com[filtered_com['lib_com'] == option_com]

lib_com = df_com['lib_com'].values[0]
df_geo_com = load_geo(lib_com)    
st.write(f"Vous avez sélectionné la commune de **{option_com}**, dans le département de **{option_dep}**, en région **{option_reg}**.")

# a.Carte
#Read the HTML content from the file
html_content = read_html_file('cartes/map_' + df_geo_com['id_com'][0] + '.html')
# Display the HTML content in Streamlit
map_container = st.container()
     
with map_container:
    st.components.v1.html(html_content,height=800)