import streamlit as st

st.write("Hello World")

blob_name = "Hamelburg Supervisory Hiring_athena.json"
data = load_json(blob_name)

st.subheader("Raw JSON from Blob")
st.json(data)  # shows pretty JSON in the app
