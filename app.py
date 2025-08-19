import os
import io
import pandas as pd
import streamlit as st
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

# ---- Config ----
CONTAINER = os.getenv("CONTAINER", "dashboard")

def make_bsc() -> BlobServiceClient:
    # Local: connection string
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str)
    # Azure: Managed Identity + account name
    acct = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    if not acct:
        raise RuntimeError(
            "Storage not configured. Set AZURE_STORAGE_CONNECTION_STRING locally, "
            "or AZURE_STORAGE_ACCOUNT_NAME in App Settings (with Managed Identity)."
        )
    cred = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    return BlobServiceClient(account_url=f"https://{acct}.blob.core.windows.net", credential=cred)

def load_csv(blob_path: str, *, encoding="utf-8", delimiter=None) -> pd.DataFrame:
    """
    Download a CSV blob and return a DataFrame.
    - `encoding`: try 'utf-8' (or 'utf-8-sig' if you see BOM issues)
    - `delimiter`: set to ',' ';' '\t' etc. If None, pandas will infer.
    """
    cc = make_bsc().get_container_client(CONTAINER)
    blob_bytes = cc.download_blob(blob_path).readall()

    # Use StringIO for text CSV
    text_buf = io.StringIO(blob_bytes.decode(encoding, errors="replace"))
    read_kwargs = {}
    if delimiter:
        read_kwargs["sep"] = delimiter

    # If you have large CSVs, you can add engine="pyarrow" (requires pyarrow)
    # read_kwargs["engine"] = "pyarrow"
    return pd.read_csv(text_buf, **read_kwargs)

# ------------------- Streamlit UI -------------------
st.title("Blob CSV → Table")

# Example: "athena/Hamelburg Supervisory Hiring_athena.csv"
blob_path = st.text_input("CSV blob path inside the container", "athena/sample.csv")

# Optional controls
col1, col2 = st.columns(2)
with col1:
    encoding = st.text_input("Encoding", value="utf-8")
with col2:
    delimiter = st.text_input("Delimiter (blank = auto)", value="")

if st.button("Load CSV"):
    try:
        df = load_csv(blob_path, encoding=encoding, delimiter=(delimiter or None))

        if df is not None and not df.empty:
            st.markdown("#### Table view")
            st.dataframe(df, use_container_width=True)

            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download as CSV",
                data=csv,
                file_name=os.path.basename(blob_path),
                mime="text/csv",
            )
        else:
            st.info("Loaded CSV but the DataFrame is empty.")
    except Exception as e:
        st.error(f"Failed to load CSV: {e}")
