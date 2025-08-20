import os, io, posixpath
import pandas as pd
import streamlit as st
from functools import lru_cache
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

# ---- Config ----
CONTAINER = os.getenv("CONTAINER", "dashboard")
CSV_EXTS = {".csv"}  # add ".tsv" etc. if needed

# ---------- Blob client ----------
@lru_cache(maxsize=1)
def make_bsc() -> BlobServiceClient:
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str)
    acct = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    if not acct:
        raise RuntimeError(
            "Storage not configured. Set AZURE_STORAGE_CONNECTION_STRING locally, "
            "or AZURE_STORAGE_ACCOUNT_NAME in App Settings (with Managed Identity)."
        )
    cred = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    return BlobServiceClient(account_url=f"https://{acct}.blob.core.windows.net", credential=cred)

def get_cc():
    return make_bsc().get_container_client(CONTAINER)

# ---------- Helpers ----------
def _is_csv(name: str) -> bool:
    return os.path.splitext(name.lower())[1] in CSV_EXTS

@st.cache_data(ttl=30)
def list_candidate_prefixes() -> list[str]:
    """
    Returns top-level 'prefixes' (first path segment) inside the container.
    Works whether or not hierarchical namespace is enabled.
    """
    cc = get_cc()
    prefixes = set()
    # Use 'walk_blobs' if available (hierarchical listing), otherwise fall back.
    try:
        for item in cc.walk_blobs(delimiter="/"):
            if hasattr(item, "name") and item.name:  # BlobPrefix
                p = item.name.strip("/")
                if p:
                    prefixes.add(p)
    except TypeError:
        # Older SDKs may not support delimiter; fallback to list_blobs
        for blob in cc.list_blobs():
            parts = blob.name.split("/", 1)
            if len(parts) == 2:
                prefixes.add(parts[0])
    return sorted(prefixes)

@st.cache_data(ttl=30)
def list_csvs_under(prefix: str) -> list[str]:
    """List CSV blob paths under a given top-level prefix (candidate folder)."""
    cc = get_cc()
    start = prefix.rstrip("/") + "/"
    paths = []
    for blob in cc.list_blobs(name_starts_with=start):
        if _is_csv(blob.name):
            paths.append(blob.name)
    return sorted(paths)

@st.cache_data(ttl=300)
def load_csv(blob_path: str, *, encoding="utf-8", delimiter=None) -> pd.DataFrame:
    """Download a CSV blob and return a DataFrame."""
    cc = get_cc()
    data = cc.download_blob(blob_path).readall()
    buf = io.StringIO(data.decode(encoding, errors="replace"))
    kwargs = {"sep": delimiter} if delimiter else {}
    return pd.read_csv(buf, **kwargs)

# ---------- UI ----------
st.title("Candidates")
st.caption("Each candidate is a folder under the 'dashboard' container. Expand to view their tables.")

# Optional controls
with st.expander("Options"):
    encoding = st.text_input("CSV encoding", "utf-8", help="Try 'utf-8-sig' if you see weird characters.")
    delimiter = st.text_input("CSV delimiter (blank = auto)", "")

# List candidates
try:
    candidates = list_candidate_prefixes()
    if not candidates:
        st.info("No candidate folders found yet.")
    else:
        for cand in candidates:
            with st.expander(cand, expanded=False):
                csvs = list_csvs_under(cand)
                if not csvs:
                    st.write("_No CSVs in this folder._")
                    continue

                for path in csvs:
                    file_name = posixpath.basename(path)
                    st.markdown(f"**{file_name}**")
                    try:
                        df = load_csv(path, encoding=encoding or "utf-8", delimiter=(delimiter or None))
                        if df is not None and not df.empty:
                            st.dataframe(df, use_container_width=True)
                            st.download_button(
                                label="Download CSV",
                                data=df.to_csv(index=False).encode("utf-8"),
                                file_name=file_name,
                                mime="text/csv",
                                key=f"dl-{path}",
                            )
                        else:
                            st.info("Loaded but DataFrame is empty.")
                    except Exception as e:
                        st.error(f"Failed to load {file_name}: {e}")
except Exception as e:
    st.error(f"Error listing candidates: {e}")
