import os, io, re
from typing import Dict, Tuple, List
import pandas as pd
import streamlit as st
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.identity import DefaultAzureCredential, AzureCliCredential

DASHBOARD = os.getenv("DASHBOARD_CONTAINER", "dashboard")

# Caching storage client
@st.cache_resource(show_spinner=False)
def _bsc() -> BlobServiceClient:
    conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if conn:
        return BlobServiceClient.from_connection_string(conn)
    acct = os.getenv("AZURE_STORAGE_ACCOUNT_NAME") or os.getenv("AZURE_STORAGE_ACCOUNT")
    if not acct:
        raise RuntimeError("Set AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_NAME.")
    cred = AzureCliCredential() if os.getenv("PREFER_AZ_CLI","1")=="1" else DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    return BlobServiceClient(f"https://{acct}.blob.core.windows.net", credential=cred)
    
# Creating container client
def _cc():
    return _bsc().get_container_client(DASHBOARD)

# Blob helpers (streaming the data to Streamlit). Will return none if blob doesn't exist.
def _download_blob_text(path: str) -> str | None:
    try:
        return _cc().download_blob(path).readall().decode("utf-8", errors="replace")
    except Exception:
        return None
    
# Loading the AI generated summary
@st.cache_data(ttl=15, show_spinner=False)
def load_summary_text(slug: str) -> str:
    return _download_blob_text(f"{slug.rstrip('/')}/summary.txt") or ""
    
# This is for when the user makes an edit - it'll write the summary to streamlit so that it updates for the user
def save_summary_text(slug: str, text: str, filename: str = "summary.txt") -> None:
    _cc().upload_blob(
        f"{slug.rstrip('/')}/{filename}",
        text.encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="text/plain"),
    )


# Combined summary editor  
def draft_combined_summary(a_name: str, a_text: str, b_name: str, b_text: str) -> str:
    a_text = (a_text or "").strip()
    b_text = (b_text or "").strip()
    return (
        f"{a_text if a_text else '(no summary yet)'}\n\n"
        f"{b_name} summary:** {b_text if b_text else '(no summary yet)'}\n\n"
        "— Compare strengths/risks/culture fit. Add hiring leaning and 3–5 probe questions. —"
    )

def render_combined_editor(current_slug: str, other_slug: str, context_key: str = "compare") -> None:
    
    a_name, b_name = current_slug, other_slug
    a_text = load_summary_text(current_slug)
    b_text = load_summary_text(other_slug)

    sess_key = f"combined-draft-{current_slug}-{other_slug}-{context_key}"
    if sess_key not in st.session_state:
        st.session_state[sess_key] = draft_combined_summary(a_name, a_text, b_name, b_text)
    
    st.markdown(f"### Combined summary editor ({a_name} ⇄ {b_name})")
    edited = st.text_area(
        "Edit the combined comparison",
        value=st.session_state[sess_key],
        height=300,
        key=f"combined-ta-{current_slug}-{other_slug}-{context_key}",
    )
    
    c1, c2 = st.columns([1,1])

    with c1:
        if st.button("Save", key=f"save-combined-{current_slug}-{other_slug}-{context_key}"):
            try:
                save_summary_text(current_slug, edited, filename="combined_summary.txt")
                st.success(f"Saved to {current_slug}/summary_combined.txt")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Save failed: {e}")

# CSV listing + parsers (Athena/Genos) — used by the comparison table
@st.cache_data(ttl=30, show_spinner=False)
def _list_csvs_for_candidate(cand: str) -> list[str]:
    start = cand.rstrip("/") + "/"
    paths = []
    for blob in _cc().list_blobs(name_starts_with=start):
        if blob.name.lower().endswith(".csv"):
            paths.append(blob.name)
    return sorted(paths)

def _find_col(df: pd.DataFrame, *cands) -> str | None:
    cols = {c.strip().lower(): c for c in df.columns if isinstance(c, str)}
    for c in cands:
        if c and c.strip().lower() in cols:
            return cols[c.strip().lower()]
    norm = {re.sub(r"[^a-z0-9]", "", k): v for k, v in cols.items()}
    for c in cands:
        if not c: continue
        k = re.sub(r"[^a-z0-9]", "", c.strip().lower())
        if k in norm: return norm[k]
    wanted = [re.sub(r"[^a-z0-9]", "", c.strip().lower()) for c in cands if c]
    for raw, orig in cols.items():
        raw_norm = re.sub(r"[^a-z0-9]", "", raw)
        if any(w in raw_norm for w in wanted): return orig
    return None

def _read_csv(path: str) -> pd.DataFrame | None:
    txt = _download_blob_text(path)
    if txt is None: return None
    try: return pd.read_csv(io.StringIO(txt))
    except Exception: return None

def _parse_athena(df: pd.DataFrame) -> tuple[dict[str, str], dict[str, str]]:
    if df is None or df.empty: return {}, {}
    c_measure = _find_col(df, "Measure")
    c_cand    = _find_col(df, "Candidate Value")
    c_top     = _find_col(df, "Top Performers")
    if not c_measure:
        str_cols = [c for c in df.columns if df[c].dtype == object]
        c_measure = str_cols[0] if str_cols else df.columns[0]
    if not c_cand: c_cand = df.columns[-1]
    cand_map, top_map = {}, {}
    for _, row in df.iterrows():
        m = str(row.get(c_measure, "")).strip()
        if not m: continue
        v_c = row.get(c_cand, ""); v_t = row.get(c_top, "") if c_top else ""
        cand_map[m] = "" if pd.isna(v_c) else str(v_c).strip()
        top_map[m]  = "" if pd.isna(v_t) else str(v_t).strip()
    return cand_map, top_map

def _parse_genos(df: pd.DataFrame) -> Dict[str, str]:
    if df is None or df.empty: return {}
    c_trait = _find_col(df, "Measure", "Trait")
    c_score = _find_col(df, "Band")
    if not c_trait or not c_score:
        str_cols = [c for c in df.columns if df[c].dtype == object]
        c_trait = c_trait or (str_cols[0] if str_cols else df.columns[0])
        c_score = c_score or df.columns[-1]
    out: Dict[str, str] = {}
    for _, row in df.iterrows():
        t = str(row.get(c_trait, "")).strip()
        v = row.get(c_score, "")
        out[t] = "" if pd.isna(v) else str(v).strip()
    return out

@st.cache_data(ttl=30, show_spinner=True)
def load_candidate_measure_maps(cand: str) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    csvs = _list_csvs_for_candidate(cand)
    # pick athena/genos files
    athena_path = next((p for p in csvs if re.search(r"(athena|athen[_-]?vs[_-]?top)", p, re.I)), None)
    genos_path  = next((p for p in csvs if re.search(r"genos", p, re.I)), None)
    ath_df = _read_csv(athena_path) if athena_path else None
    ge_df  = _read_csv(genos_path)  if genos_path  else None
    ath_cand_map, ath_top_map = _parse_athena(ath_df)
    ge_map = _parse_genos(ge_df)
    return ath_cand_map, ath_top_map, ge_map


@st.cache_data(ttl=30, show_spinner=True)
def build_comparison_table(candidates: List[str]) -> pd.DataFrame:
    # Preserve order without duplicates
    def add_unique(seq: List[str], key: str):
        if key not in seen:
            seen.add(key)
            seq.append(key)

    candidate_scores: Dict[str, Dict[str, str]] = {}
    top_scores: Dict[str, str] = {}

    # Ordered measure buckets
    athena_measures: List[str] = []
    genos_measures: List[str] = []
    seen: set[str] = set()  # for athena+genos combined keys

    for cand in candidates:
        a_cand, a_top, ge = load_candidate_measure_maps(cand)

        # capture Top Performers once (Athena only)
        if not top_scores and a_top:
            top_scores = a_top

        merged: Dict[str, str] = {}

        # Athena measures first, in incoming order
        for m, v in a_cand.items():
            merged[m] = v
            if m not in athena_measures:
                add_unique(athena_measures, m)

        # GENOS measures next, in incoming order, with a clear prefix
        for trait, band in ge.items():
            key = f"{trait}"
            merged[key] = band
            if key not in genos_measures:
                # use a separate seen set for GENOS order to avoid cross-polluting Athena keys
                genos_measures.append(key)

        candidate_scores[cand] = merged

    # Final ordered list: Athena first, GENOS last
    measures = athena_measures + genos_measures

    # Build rows
    rows = []
    for m in measures:
        # Only Athena rows have a Top Performers value; GENOS rows blank
        row = {
            "Measure": m,
            "Top Performers": top_scores.get(m, "") if not m.startswith("GENOS – ") else ""
        }
        for cand in candidates:
            row[cand] = candidate_scores.get(cand, {}).get(m, "")
        rows.append(row)

    cols = ["Measure"] + candidates + ["Top Performers"]
    return pd.DataFrame(rows, columns=cols)


def render_comparison_table(selected: List[str], title: str = "Side-by-side scores") -> None:
    if not selected:
        st.info("Select at least one candidate to compare.")
        return
    df = build_comparison_table(selected)
    if df.empty:
        st.warning("No Athena/Genos data found for the selected candidates.")
        return
    st.markdown(f"### {title}")
    st.dataframe(df, use_container_width=True)

# No top-level Streamlit UI here — keep this file import-safe!
