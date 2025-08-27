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

def render_comparison_download(cand: str, other: str, html: str):
    archive_path = f"{cand}_vs_{other}_cohesive_summary.html"
    try:
        upload_text(archive_path, html, content_type="text/html")
        st.toast(f"Comparison archived to Blob: {archive_path}", icon="✅")
    except Exception as e:
        st.warning(f"Downloaded locally, but failed to archive comparison: {e}")
import re
from html import unescape as _unescape

import re
from html import unescape as _unescape

def load_summary_only(blob_name: str) -> str:
    """
    Loads an HTML file from Finished and extracts only the cohesive summary text,
    excluding the comparison table and other sections.
    """
    cc = _archive_cc()
    try:
        html_doc = cc.download_blob(blob_name).readall().decode("utf-8", errors="replace")

        # --- Step 1: Locate the section before the table ---
        # Grab everything from <h2> down to <h3>Comparison Table</h3>
        match = re.search(r"<h2.*?</h2>(.*?)<h3>Comparison Table", html_doc, flags=re.S | re.I)
        if match:
            summary_html = match.group(1)
        else:
            # Fallback: if table isn't found, grab inside <body>
            body_match = re.search(r"<body[^>]*>(.*?)</body>", html_doc, flags=re.S | re.I)
            summary_html = body_match.group(1) if body_match else html_doc

        # --- Step 2: Remove HTML tags but preserve newlines ---
        summary_html = re.sub(r"<br\s*/?>", "\n", summary_html)  # convert <br> to newlines
        summary_text = re.sub(r"<[^>]+>", "", summary_html)      # strip all other HTML
        return _unescape(summary_text).strip()

    except Exception:
        return ""

def delete_candidate_from_dashboard(cand: str, container: str | None = None) -> tuple[int, list[str]]:
    """
    Permanently remove all blobs for a candidate from the dashboard container.
    Returns (count_deleted, list_of_deleted_blob_names).
    """
    # Use send_back's own client to avoid circular import
    container = container or os.getenv("CONTAINER", "dashboard")
    cc = _make_bsc().get_container_client(container)

    prefix = cand.rstrip("/") + "/"
    blobs_to_delete = [b.name for b in cc.list_blobs(name_starts_with=prefix)]

    deleted = 0
    for blob_name in blobs_to_delete:
        try:
            cc.delete_blob(blob_name)
            deleted += 1
        except Exception as e:
            st.warning(f"Failed to delete {blob_name}: {e}")

    return deleted, blobs_to_delete
