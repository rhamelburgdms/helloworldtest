# app.py — minimal Blob -> Streamlit JSON viewer

import os, json
import streamlit as st
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

CONTAINER = os.getenv("CONTAINER", "processed")

def make_bsc() -> BlobServiceClient:
    # Local dev: use connection string (set in your .env)
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str)
    # Deployed on Azure: use Managed Identity + account name from App Settings
    acct = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    if not acct:
        raise RuntimeError(
            "Storage not configured. Set AZURE_STORAGE_CONNECTION_STRING locally, "
            "or AZURE_STORAGE_ACCOUNT_NAME in App Settings (with Managed Identity)."
        )
    cred = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    return BlobServiceClient(account_url=f"https://{acct}.blob.core.windows.net", credential=cred)

def load_json(blob_path: str):
    cc = make_bsc().get_container_client(CONTAINER)
    return json.loads(cc.download_blob(blob_path).readall())

st.title("Hello World + Blob JSON")

# This must be the exact path inside the container, e.g.
# processed/software_engineer_ii/AB123/clean.json
blob_name = "processed/athena/Hamelburg Supervisory Hiring_athena.json"  # <-- change me

try:
    data = load_json(blob_name)
    st.subheader(f"Raw JSON from Blob: {blob_name}")
    st.json(data)
except Exception as e:
    st.error(f"Error loading '{blob_name}': {e}")
