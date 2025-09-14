# send_back.py
import os
import streamlit as st
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.identity import DefaultAzureCredential
import re
from pathlib import Path
from html import unescape as _unescape

@st.cache_resource(show_spinner=False)
def _make_bsc() -> BlobServiceClient:
    conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if conn:
        return BlobServiceClient.from_connection_string(conn)
    acct = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    cred = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    return BlobServiceClient(account_url=f"https://{acct}.blob.core.windows.net", credential=cred)

def _dash_cc():
    """Client for the editable dashboard container."""
    CONTAINER = os.getenv("CONTAINER", "dashboard")
    return _make_bsc().get_container_client(CONTAINER)

def _archive_cc():
    """Client for the finished/archive container."""
    CONTAINER = os.getenv("FINISHED_CONTAINER", "finished")
    return _make_bsc().get_container_client(CONTAINER)

def upload_text(path: str, text: str, *, content_type="text/html"):
    _archive_cc().upload_blob(
        name=path.strip("/"),
        data=text.encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type=content_type),
    )

def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.strip().lower()).strip("-")

def _safe_join(prefix: str, name: str) -> str:
    prefix = prefix.strip("/")
    name = Path(name).name
    return f"{prefix}/{name}" if prefix else name

def _resolve_by_basename(basename: str) -> str | None:
    cc = _archive_cc()
    target = Path(basename).name.lower()
    for item in cc.walk_blobs(name_starts_with=""):
        n = getattr(item, "name", "") or ""
        if n and Path(n).name.lower() == target:
            return n
    return None

def render_candidate_download(cand: str, solo_html: str):
    folder = f"{cand}/exports"
    file_name = f"{_slug(cand)}_summary.html"
    archive_path = _safe_join(folder, file_name)
    try:
        upload_text(archive_path, solo_html, content_type="text/html")
        st.toast(f"Archived to Blob: {archive_path}", icon="✅")
        # Do NOT auto-hide; keep candidate visible after save+download
        # st.session_state.setdefault("removed_candidates", set()).add(cand)
    except Exception as e:
        st.warning(f"Downloaded locally, but failed to archive to Blob: {e}")

def render_comparison_download(cand: str, other: str, html: str):
    base_name = f"{_slug(cand)}-vs-{_slug(other)}.html"
    cand_path  = _safe_join(f"{cand}/comparisons", base_name)
    other_path = _safe_join(f"{other}/comparisons", base_name)
    try:
        upload_text(cand_path, html, content_type="text/html")
        upload_text(other_path, html, content_type="text/html")
        st.toast("Comparison archived")
    except Exception as e:
        st.warning(f"Downloaded locally, but failed to archive comparison: {e}")

def load_summary_only(blob_name: str) -> str:
    cc = _archive_cc()
    path = blob_name.strip("/")
    if "/" not in path:
        resolved = _resolve_by_basename(path)
        if not resolved:
            return ""
        path = resolved
    try:
        html = cc.download_blob(path).readall().decode("utf-8", "replace")
        m = re.search(r"<!--\s*SUMMARY_START\s*-->(.*?)<!--\s*SUMMARY_END\s*-->", html, re.S|re.I)
        if not m:
            m = re.search(r'<div[^>]+id=["\']summary-text["\'][^>]*>(.*?)</div>', html, re.S|re.I)
        if m:
            frag = m.group(1)
            frag = re.sub(r"<br\s*/?>", "\n", frag, flags=re.I)
            frag = re.sub(r"<[^>]+>", "", frag)
            return _unescape(frag).strip()
        head = re.split(r"<h3", html, maxsplit=1, flags=re.I)[0]
        head = re.sub(r"<[^>]+>", "", head)
        return _unescape(head).strip()
    except Exception:
        return ""

def delete_candidate_from_dashboard(cand: str) -> tuple[int, list[str]]:
    """Remove all blobs for this candidate from the dashboard container."""
    cc = _dash_cc()  # fixed: use dashboard container + correct helper
    prefix = f"{cand.rstrip('/')}/"
    names = [b.name for b in cc.list_blobs(name_starts_with=prefix)]
    deleted, errors = 0, []
    for name in names:
        try:
            cc.get_blob_client(name).delete_blob(delete_snapshots="include")
            deleted += 1
        except Exception as e:
            errors.append(f"{name}: {e}")
    return deleted, errors
