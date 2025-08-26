# send_back.py
import os
import streamlit as st
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.identity import DefaultAzureCredential


@st.cache_resource(show_spinner=False)
def _make_bsc() -> BlobServiceClient:
    conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if conn:
        return BlobServiceClient.from_connection_string(conn)
    acct = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    cred = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    return BlobServiceClient(account_url=f"https://{acct}.blob.core.windows.net", credential=cred)

def _archive_cc():
    CONTAINER = os.getenv("CONTAINER", "finished")
    return _make_bsc().get_container_client(CONTAINER)

def upload_text(path: str, text: str, *, content_type="text/html"):
    _archive_cc().upload_blob(
        name=path.strip("/"),
        data=text.encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type=content_type),
    )

def render_candidate_download(cand: str, solo_html: str):
    archive_path = f"{cand}_summary.html"  # fixed name, no timestamp
    try:
        upload_text(archive_path, solo_html, content_type="text/html")
        st.toast(f"Archived to Blob: {archive_path}", icon="✅")
        st.session_state.setdefault("removed_candidates", set()).add(cand)
    except Exception as e:
        st.warning(f"Downloaded locally, but failed to archive to Blob: {e}")
