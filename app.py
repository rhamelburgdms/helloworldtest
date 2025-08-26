import os, io, posixpath
import pandas as pd
import streamlit as st
from functools import lru_cache
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import numpy as np 
import re, json
from azure.storage.blob import BlobServiceClient

st.set_page_config(page_title="Candidates", page_icon="🧩", layout="wide")

pages = {
    "Your account": [
        st.Page("candidates.py", title="View candidates"),
        #st.Page("summary_editor.py", title="Edit Summaries"),
        #st.Page("agent.py", title='Agent Help')
    ],
}

pg = st.navigation(pages)
pg.run()
