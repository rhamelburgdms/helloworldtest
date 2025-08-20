import os, io, posixpath
import pandas as pd
import streamlit as st
from functools import lru_cache
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import numpy as np
import re, json
st.set_page_config(page_title="Candidates", page_icon="🧩", layout="wide")

# we pull the csvs that are formatted in our dashboard container
CONTAINER = os.getenv("CONTAINER", "dashboard")
CSV_EXTS = {".csv"} 

# we use a blob service client, so that our blob storage becomes a remote file system for our app.
#@lru_cache(maxsize=1)
def make_bsc() -> BlobServiceClient:
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str)
    acct = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    cred = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    return BlobServiceClient(account_url=f"https://{acct}.blob.core.windows.net", credential=cred)

def get_cc():
    return make_bsc().get_container_client(CONTAINER)

# this helper will title the csvs
def _is_csv(name: str) -> bool:
    return os.path.splitext(name.lower())[1] in CSV_EXTS

@st.cache_data(ttl=30)
def list_candidate_prefixes() -> list[str]:
    """
    Returns top-level 'prefixes' (first path segment) inside the container.
    """
    cc = get_cc() 
    prefixes = set() 
    # Use 'walk_blobs' if available (hierarchical listing), otherwise fall back.
    try:
        for item in cc.walk_blobs(delimiter="/"):
            if hasattr(item, "name") and item.name:  # this is how we're storing candidates by their filenames
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

@st.cache_data(ttl=30) # This returns paths to the csvs so they can be found for each candidate.
def list_csvs_for_candidate(cand: str) -> list[str]:
    """
    List CSV blob paths under a candidate folder.
    """
    cc = make_bsc().get_container_client(CONTAINER)
    start = cand.rstrip("/") + "/"
    paths = []
    for blob in cc.list_blobs(name_starts_with=start):
        if blob.name.lower().endswith(".csv"):
            paths.append(blob.name)
    return sorted(paths)
    
# Returns dataframes from the files.
def load_csv(blob_path: str) -> pd.DataFrame | None:
    try:
        cc = make_bsc().get_container_client(CONTAINER)
        blob_bytes = cc.download_blob(blob_path).readall()
        return pd.read_csv(io.StringIO(blob_bytes.decode("utf-8")))
    except Exception:
        return None
        
def load_summary(cand: str) -> str | None:
    """
    Downloads summary.txt for a given candidate from the processed container.
    Returns the summary text, or None if not found.
    """
    cc = make_bsc().get_container_client(CONTAINER)
    path = f"{cand}/summary.txt"
    try:
        blob_bytes = cc.download_blob(path).readall()
        return blob_bytes.decode("utf-8", errors="replace")
    except Exception:
        return None

# actual UI
st.title("Candidates")
st.caption("Each candidate is a folder under the 'dashboard' container. Expand to view their tables.")

# Optional controls
with st.expander("Options"):
    encoding = st.text_input("CSV encoding", "utf-8", help="Try 'utf-8-sig' if you see weird characters.")
    delimiter = st.text_input("CSV delimiter (blank = auto)", "")

# List candidates
try:
    candidates = list_candidate_prefixes()
except Exception as e:
    st.error(f"Failed to list candidates: {e}")
    candidates = []

if not candidates:
    st.info("No candidates are pending approval.")
else:
    for cand in candidates:
        with st.expander(cand, expanded=False):
            summary = load_summary(cand)
            if summary:
                st.markdown("### Candidate Summary")
                st.write(summary)
            else:
                st.info("_No summary available for this candidate._")

            # list the csvs for that candidate
            
            try:
                csvs = list_csvs_for_candidate(cand)

                for path in csvs:
                    df = load_csv(path)
                    if df is not None:
                        st.markdown(f"**{path.split('/')[-1]}**")
                        st.dataframe(df, use_container_width=True)
                    else:
                        st.warning(f"Failed to load `{path}`.")

            except Exception as e:
                st.error(f"Failed to list CSVs for {cand}: {e}")
                continue

            if not csvs:
                st.write("_No CSVs found for this candidate._")
                continue
    
            # Try to find Athena & Genos CSVs by name
            athena_path = next((p for p in csvs if "athena" in p.lower()), None)
            genos_path  = next((p for p in csvs if "genos"  in p.lower()), None)

            # make sure you load the appropriate csvs - we're just loading them, we're not displaying them here. 
            athena_df = load_csv(athena_path) if athena_path else None
            genos_df  = load_csv(genos_path)  if genos_path  else None  # reserved for later

            # Here's the fit determination piece

            def _col(df, target: str):
                """Find a column name case/space-insensitively."""
                if df is None:
                    return None
                tl = target.strip().lower()
                for c in df.columns:
                    if c.strip().lower() == tl:
                        return c
                return None

            def _to_set(v):
                """Parse a cell to a set of lowercase tokens."""
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return set()
                if isinstance(v, str):
                    parts = re.split(r"[;,/|]+", v)
                elif isinstance(v, (list, tuple, set)):
                    parts = list(v)
                else:
                    parts = [str(v)]
                return {p.strip().lower() for p in parts if p and p.strip()}

            def athena_fit_from_flags(df: pd.DataFrame):
                """
                Returns (fit_0to1, matches, top_performers, candidate_flags)
                Fit = |matches| / |top_performers|  (0 if no top performers)
                Unions values across rows if multiple rows exist.
                """
                if df is None or df.empty:
                    return 0.0, set(), set(), set()

                tp_col = _col(df, "Top Performers")
                cf_col = _col(df, "Candidate Value")
                if not tp_col or not cf_col:
                    return 0.0, set(), set(), set()

                tp, cf = set(), set()
                for _, row in df.iterrows():
                    tp |= _to_set(row.get(tp_col))
                    cf |= _to_set(row.get(cf_col))

                matches = tp & cf
                fit = len(matches) / len(tp) if tp else 0.0
                return float(fit), matches, tp, cf

            athena_fit, matches, tp_all, cf_all = athena_fit_from_flags(athena_df)

            # For now, overall fit = Athena fit only (Genos integration later)
            fit = athena_fit
            a = athena_fit
            g = float("nan")

            # show what matched 
            with st.expander("Athena flag comparison", expanded=False):
                if tp_all:
                    st.markdown(
                        f"**Matches ({len(matches)}/{len(tp_all)}):** "
                        + (", ".join(sorted(matches)) if matches else "—")
                    )
                    missing = tp_all - matches
                    st.markdown("**Missing:** " + (", ".join(sorted(missing)) if missing else "—"))
                    extra = cf_all - tp_all
                    if extra:
                        st.markdown("**Candidate-only flags:** " + ", ".join(sorted(extra)))
                else:
                    st.markdown("_No Top Performers listed in Athena._")

            # The progress bar part
            row = pd.DataFrame([{
                "candidate": cand,
                "fit": fit,        # 0..1 fraction
                "athena": a,       # 0..1 fraction
                "genos":  g,       # NaN for now
                "status": "pending",
                "notes": "",
            }])


            row["fit"]    = (row["fit"]    * 100).astype(float).round(1)
            row["athena"] = (row["athena"] * 100).astype(float).round(1)
            row["genos"]  = (row["genos"]  * 100).astype(float)  # keep NaN if any

            edited = st.data_editor(
                row,
                hide_index=True,
                use_container_width=True,
                disabled=["candidate", "fit", "athena", "genos"],  # only status/notes editable
                column_config={
                   "fit": st.column_config.ProgressColumn(
                        "Fit (Athena)", min_value=0, max_value=100, format="%.1f%%"
                    ),
                    "athena": st.column_config.NumberColumn("Athena", format="%.1f%%"),
                    "genos":  st.column_config.NumberColumn("Genos",  format="%.1f%%"),

                    "status": st.column_config.SelectboxColumn(
                        "Status", options=["pending", "approved", "on hold", "rejected"]
                    ),
                    "notes":  st.column_config.TextColumn("Notes"),
                },
            )
            
            # Optional save button → write review.json back to this candidate folder
            if st.button("Save", key=f"save-{cand}"):
                try:
                    review = {
                        "status": str(edited.loc[0, "status"]),
                        "notes":  str(edited.loc[0, "notes"]),
                        "fit":    float(edited.loc[0, "fit"]),
                        "athena": float(edited.loc[0, "athena"]) if pd.notna(edited.loc[0, "athena"]) else None,
                        "genos":  float(edited.loc[0, "genos"])  if pd.notna(edited.loc[0, "genos"])  else None,
                    }
                    path = f"{cand}/review.json"
                    cc = make_bsc().get_container_client(CONTAINER)
                    cc.upload_blob(path, json.dumps(review).encode("utf-8"), overwrite=True)
                    st.success("Saved review.")
                except Exception as e:
                    st.error(f"Failed to save review: {e}")
            
