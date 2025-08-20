import os
import streamlit as st
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.identity import DefaultAzureCredential

st.set_page_config(page_title="Summary Editor", page_icon="✏️", layout="wide")

CONTAINER = os.getenv("CONTAINER", "dashboard")  # same as candidates.py

@st.cache_resource
def make_bsc():
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str)
    acct = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    cred = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    return BlobServiceClient(account_url=f"https://{acct}.blob.core.windows.net", credential=cred)

def load_summary_text(cand: str) -> str:
    if not cand:
        return ""
    cc = make_bsc().get_container_client(CONTAINER)
    try:
        return cc.download_blob(f"{cand}/summary.txt").readall().decode("utf-8", errors="replace")
    except Exception:
        return ""

def save_summary_text(cand: str, text: str):
    cc = make_bsc().get_container_client(CONTAINER)
    cc.upload_blob(
        f"{cand}/summary.txt",
        text.encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="text/plain"),
    )

# Accept candidate from session OR URL (?candidate=slug)
cand = (
    st.session_state.get("selected_candidate")
    or st.query_params.get("candidate", "")
    or st.experimental_get_query_params().get("candidate", [""])[0]
)

if not cand:
    st.error("No candidate selected. Go back to the dashboard and click 'Go to Editor'.")
    st.page_link("candidates.py", label="Back to Candidates", icon="↩️")
    st.stop()

st.title(f"Edit Summary — {cand}")

current = load_summary_text(cand)
edited = st.text_area("Summary text", value=current, height=300, placeholder="No summary yet.")

col1, col2 = st.columns([1, 1])
with col1:
    if st.button("Save"):
        save_summary_text(cand, edited)
        st.success("Saved.")
with col2:
    st.page_link("candidates.py", label="Back to Candidates", icon="↩️")
