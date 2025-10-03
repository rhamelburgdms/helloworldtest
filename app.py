# app.py
import streamlit as st

st.set_page_config(page_title="Candidates", page_icon="🧩", layout="wide")

pages = {
    "Your account": [
        st.Page("uploads.py",   title="Upload candidates"),     # 👈 new
        st.Page("candidates.py", title="View processed candidates"), # existing
    ],
}

pg = st.navigation(pages)
pg.run()
