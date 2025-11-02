
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

st.set_page_config(layout="wide")

# Load the HTML file
def read_html_file(filename):
    with open(filename, 'r') as f:
        return f.read()

# ---------- Function to run SQL queries ----------
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
    df = query("SELECT * FROM insee_ref WHERE id_dep in ('43','78','85')")  # fetch only the needed cols
    df = df.drop_duplicates()                                   # tiny safety net
    return df.sort_values(['lib_reg', 'lib_dep', 'lib_com','id_com'],
                          ascending=[True, True, True, True])         # one single sort

def load_geo(id_commune) -> pd.DataFrame:
    """Pull the full table once and keep it in Streamlit's cache."""
    df = query("SELECT * FROM com_geo WHERE """"id_com"""" = '" + id_commune  + "'")# fetch only the needed cols
    df = df.drop_duplicates()                                   # tiny safety net
    return df.sort_values(['id_com','lib_com','geo_com'],
                          ascending=[True, True, True])         # one single sort

df_insee_ref = load_insee_ref()

# ---------- 2️⃣ Build unique, *sorted* option lists ----------
regions = df_insee_ref['lib_reg'].unique().tolist()
default_value = "Île-de-France"
default_index = regions.index(default_value)

with st.sidebar:
    st.header("Sélection géographique")
    # ---------- 3️⃣ Region selectbox ----------
    selected_reg = st.selectbox(
        "Région:",
        options=regions,
        index=default_index,
        placeholder="Sélectionner une région",
        key="reg_selectbox"
    )

    # ---------- 4️⃣ Department selectbox (filtered) ----------
    # Filter once (no extra sort)
    filtered_dep = df_insee_ref[df_insee_ref['lib_reg'] == selected_reg]
    dep_options = filtered_dep['lib_dep'].unique().tolist()

    selected_dep = st.selectbox(
        "Département:",
        options=dep_options,
        index=0,
        placeholder="Sélectionner un département",
        key="dep_selectbox"
    )

    # ---------- 5️⃣ Commune selectbox (filtered) ----------
    filtered_com = filtered_dep[filtered_dep['lib_dep'] == selected_dep]
    com_options = filtered_com['lib_com'].unique().tolist()

    selected_com = st.selectbox(
        "Commune:",
        options=com_options,
        index=0,
        placeholder="Sélectionner une commune",
        key="com_selectbox"
    )
df_com = filtered_com[filtered_com['lib_com'] == selected_com]

lib_com = df_com['lib_com'].values[0]
id_com = df_com['id_com'].values[0]
df_geo_com = load_geo(id_com)    

# a.Carte
st.header("Carte")
st.write(f"Vous avez sélectionné la commune de **{selected_com}**, dans le département de **{selected_dep}**, en région **{selected_reg}**.")

#Read the HTML content from the file
html_content = read_html_file('cartes/map_' + id_com + '.html')
# Display the HTML content in Streamlit

map_container = st.container()
with map_container:
    st.components.v1.html(html_content,height=800)

# b.Comparateur INSEE
query_com = "SELECT * FROM insee_comparateur_sample WHERE """"id_com"""" = '" + id_com  + "'"
print(query_com)
df_comp = query(query_com)
st.header("Comparateur INSEE")

st.write(f"Superficie: **{df_comp['SUPERF'].values[0]}** km²")
df_comp_pop = df_comp[['P22_POP','P16_POP','NAIS1621','DECE1621','P22_MEN','NAISD24','DECESD24']]
df_comp_log = df_comp[['P22_LOG','P22_RP','P22_RSECOCC','P22_LOGVAC']]
df_comp_fisc = df_comp[['NBMENFISC21','PIMP21','MED21','TP6021']]
df_comp_emp = df_comp[['P22_EMPLT','P22_EMPLT_SAL','P16_EMPLT','P22_POP1564','P22_CHOM1564','P22_ACT1564']]
df_comp_eco = df_comp[['ETTOT23','ETAZ23','ETBE23','ETFZ23','ETGU23','ETOQ23','ETTEF123','ETTEFP1023']]

st.dataframe(df_comp_pop,
             column_config={
            'P22_POP': 'Pop. 2022',
            'P16_POP': 'Pop. 2016',
            'NAIS1621': 'Nb naissances 2016-2021',
            'DECE1621': 'NB décès 2016-2021',
            'P22_MEN': 'NB ménages 2022',
            'NAISD24': 'NB naissances 2024',
            'DECESD24': 'NB décès 2024'},
            use_container_width=False,hide_index=True)
st.dataframe(df_comp_log,
             column_config={
            'P22_LOG': 'NB logements 2022',
            'P22_RP': 'Nb rés.pal. 2022',
            'P22_RSECOCC': 'Nb rés.sec et occas. 2022',
            'P22_LOGVAC': 'Nb logements vacants 2022'},
             use_container_width=False,hide_index=True)
st.dataframe(df_comp_fisc,
             column_config={
            'NBMENFISC21': 'Nb foyers fisc. 2022',
            'PIMP21': 'Part des foyers fisc. imposés 2021',
            'MED21': 'Médiane du niveau de vie 2021',
            'TP6021': 'Tx de pauvreté 2021'},
             use_container_width=False,hide_index=True)
st.dataframe(df_comp_emp,
             column_config={
            'P22_EMPLT': 'Nb emplois 2022',
            'P22_EMPLT_SAL': 'Nb emplois salariés 2022',
            'P16_EMPLT': 'Nb emplos 2016',
            'P22_POP1564': 'Nb pers. 15-64 ans 2022',
            'P22_CHOM1564': 'Nb chômeurs 15-64 ans 2022',
            'P22_ACT1564': 'Nb pers. actives 15-64 ans 2022'
             },
             use_container_width=False,hide_index=True)
st.dataframe(df_comp_eco,
            column_config={
            'ETTOT23': 'Nb établissements 2023',
            'ETAZ23': 'Nb étab. agri.sylvi.pêche 2023',
            'ETBE23': 'Nb étab. industriels 2023',
            'ETFZ23': 'Nb étab. construction 2023',
            'ETGU23': 'Nb étab. comm. transports services fin. 2023',
            'ETOQ23': 'Nb étab.publ. enseignement santé/social',
            'ETTEF123': 'Nb étab. 1-9 salariés 2023',
            'ETTEFP1023': 'Nb étab. +10 salariés 2023'
            },
             use_container_width=False,hide_index=True)

# C.Evolution du vote
st.header("Evolution du vote")