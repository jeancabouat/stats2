
import sys
sys.path.append('/content/drive/MyDrive/Colab_Notebooks')

import functions
from functions import *

import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore", category=pd.errors.SettingWithCopyWarning)

import streamlit as st
import pandas as pd
import numpy as np

# ref INSEE
df_insee_ref = query(f"SELECT * FROM insee_ref")

option = st.selectbox(
    "Région:",
    df_insee_ref['libReg'].unique().tolist(), # Use the 'Email' column as options
    index=None,
    placeholder="Selectionner une région",
)

st.write("Région sélectionnée:", option)
