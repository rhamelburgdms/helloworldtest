# app.py — minimal Blob -> Streamlit JSON viewer

import os, json
import streamlit as st
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import pandas as pd

'''
st.title("Expandable Buttons Demo")

with st.expander("Show Details for Candidate A"):
    st.write("Name: Alice Johnson")
    st.write("Role: Data Scientist")
    st.write("Status: Shortlisted")

with st.expander("Show Details for Candidate B"):
    st.write("Name: Bob Smith")
    st.write("Role: Product Manager")
    st.write("Status: Pending Review")

with st.expander("Advanced Settings"):
    st.write("Here you can configure additional options.")
    option = st.checkbox("Enable debug mode")
    st.write("Debug mode:", option)

'''
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

st.title("JSON → Table demo")

# Pick a blob (you can hardcode or list via list_blobs)
blob_path = st.text_input("Blob path inside the container", "athena/Hamelburg Supervisory Hiring_athena.json")

if st.button("Load"):
    try:
        data = load_json(blob_path)

        st.markdown("#### Raw JSON preview")
        st.json(data)

        # --- Flexible conversion rules ---
        df = None
        if isinstance(data, list):
            # JSON is a list of records
            df = pd.DataFrame(data)

        elif isinstance(data, dict):
            # 1) common case: records live under a key (e.g., "measures")
            if "measures" in data and isinstance(data["measures"], list):
                df = pd.DataFrame(data["measures"])
            else:
                # 2) try flattening nested dicts/lists generically
                df = pd.json_normalize(
                    data,
                    max_level=1,                # bump this if you need deeper flattening
                    sep="."
                )

        if df is not None and not df.empty:
            st.markdown("#### Table view")
            st.dataframe(df, use_container_width=True)

            # Optional: allow CSV download (no Function App needed)
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download as CSV",
                data=csv,
                file_name=f"{blob_path.split('/')[-1].rsplit('.',1)[0]}.csv",
                mime="text/csv",
            )
        else:
            st.info("Loaded JSON but couldn't form a table. You may need a custom flattening rule.")

    except Exception as e:
        st.error(f"Failed to load/parse: {e}")

