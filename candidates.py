import os, io, re, json
import pandas as pd
import streamlit as st
import html as _html
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.identity import DefaultAzureCredential
#from config import make_bsc, _download_blob_bytes
from agent_comparer import compare_summaries_agent
st.set_page_config(page_title="Candidates", page_icon="🧩", layout="wide")
from send_back import render_candidate_download

CONTAINER = os.getenv("CONTAINER", "dashboard")

@st.cache_resource
def make_bsc() -> BlobServiceClient:
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str)
    acct = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    cred = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    return BlobServiceClient(account_url=f"https://{acct}.blob.core.windows.net", credential=cred)

def get_cc():
    return make_bsc().get_container_client(CONTAINER)
    
def _download_blob_bytes(path: str) -> bytes | None:
    try:
        return get_cc().download_blob(path).readall()
    except Exception:
        return None

#CONTAINER = os.getenv('CONTAINER', 'dashboard')
# We define a get client function 
def get_cc():
    return make_bsc().get_container_client(CONTAINER)
# We cache candidate data for 30 seconds so if anything changes in that 30 seconds it gets updated
@st.cache_data(ttl=30)
def list_candidate_prefixes() -> list[str]: # A list of strings 
    cc = get_cc() # Grab the container client
    prefixes = set() # Store prefixes in an empty set, because "prefixes" are the file names
    for item in cc.walk_blobs(delimiter="/"): # Pulling names from the folders
        if hasattr(item, "name") and item.name:
            p = item.name.strip("/")
            if p:
                prefixes.add(p)
                
    return sorted(prefixes)
    
# Cache data for 30 seconds
@st.cache_data(ttl=30)
def list_csvs_for_candidate(cand: str) -> list[str]:
    # We grab the csv paths so that we can load the csvs
    cc = get_cc()
    start = cand.rstrip("/") + "/"
    paths = []
    for blob in cc.list_blobs(name_starts_with=start):
        if blob.name.lower().endswith(".csv"):
            paths.append(blob.name)
    return sorted(paths)
    
# We load the csvs so that we can display them in streamlit 
def load_csv(blob_path: str) -> pd.DataFrame | None:
    try:
        b = _download_blob_bytes(blob_path)
        if b is None:
            return None
        return pd.read_csv(io.StringIO(b.decode("utf-8")))
    except Exception:
        return None
        
def load_summary(cand: str) -> str:
    """Read dashboard/{cand}/summary.txt → str ('' if missing)."""
    try:
        data = get_cc().download_blob(f"{cand.rstrip('/')}/summary.txt").readall()
        return data.decode("utf-8", errors="replace")
    except Exception:
        return ""


def save_summary(cand: str, text: str):
    """Write dashboard/{cand}/summary.txt with text/plain content type."""
    get_cc().upload_blob(
        f"{cand}/summary.txt",
        text.encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="text/plain"),
    )
      
@st.cache_data(show_spinner=True)
def list_candidates_from_dashboard(_bsc: BlobServiceClient, container: str) -> list[str]:
    cc = _bsc.get_container_client(container)  # use the param you passed in
    return sorted({b.name.split("/", 1)[0] for b in cc.walk_blobs(name_starts_with="", delimiter="/")})

bsc = make_bsc()
dash = os.getenv("CONTAINER", "dashboard")  # use uppercase key consistently
all_candidates = list_candidates_from_dashboard(bsc, dash)

# Here's where we build the download piece that makes it easy to paste into an email.
def build_candidate_email_table(cand: str, use_edits: bool, edited_summary: str) -> str:
    # Load CSVs for the candidate
    csvs = list_csvs_for_candidate(cand)
    athena_path = next((p for p in csvs if "athena" in p.lower()), None)
    genos_path = next((p for p in csvs if "genos" in p.lower()), None)

    athena_df = load_csv(athena_path) if athena_path else None
    genos_df = load_csv(genos_path) if genos_path else None
    # Start HTML email structure
    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; font-size: 14px; color: #222;">
        <h2>Candidate Summary – {cand}</h2>
        <p>{edited_summary if use_edits else load_summary(cand)}</p>
    """

    # add athena table if athena table is available
    if athena_df is not None and not athena_df.empty:
        html += "<h3>Athena vs Top Performers</h3>"
        html += athena_df.to_html(
            index=False,
            border=1,
            justify="left",
            classes="dataframe",
            escape=False
        )

    # add genos if it is available
    if genos_df is not None and not genos_df.empty:
        html += "<h3>Genos Emotional Intelligence Scores</h3>"
        html += genos_df.to_html(
            index=False,
            border=1,
            justify="left",
            classes="dataframe",
            escape=False
        )

    html += """
        <p style="margin-top: 20px; font-style: italic;">
            _Exported from HR Dashboard_
        </p>
    </body>
    </html>
    """

    return html

st.title("Candidates")
st.caption("Each candidate is a folder under the 'dashboard' container. Expand to view data and edit the summary.")
# session bootstrap
if "candidates" not in st.session_state:
    st.session_state["candidates"] = list_candidate_prefixes()  # seed UI list

# Track which expander should remain open across reruns
if "open_cand" not in st.session_state:
    st.session_state["open_cand"] = None

# List candidates
candidates = list_candidate_prefixes()

if "candidates" not in st.session_state:
    st.session_state.candidates = list_candidate_prefixes()
candidates = st.session_state.candidates

if not candidates:
    st.info("No candidates are pending approval.")
else:
    for cand in candidates:
        with st.expander(cand, expanded=False):
            # mode switching (need to add functionality to write summaries differently)
            mode = st.radio(
                "View mode",
                options=["Solo view", "Compare"],
                index=0,
                horizontal=True,
                key=f"mode-{cand}",
            )
            
            if mode == "Solo view":
                current_summary = load_summary(cand) or ""
            
                edited_summary = st.text_area(
                    label=f"Edit Summary – {cand}",
                    value=current_summary,
                    height=260,
                    key=f"solo-ta-{cand}",
                )
            
                # Save edits into session state for later email/download use
                st.session_state[f"edited_summary_{cand}"] = edited_summary
                
                # Build HTML for email/download including edits
        
                solo_html = f"""
                <html>
                <body style="font-family: Arial, sans-serif; font-size: 14px; color: #222;">
                    <h2>Candidate Summary – {cand}</h2>
                    <pre style="white-space: pre-wrap; line-height:1.4;">{_html.escape(edited_summary or "")}</pre>
                    <p style="margin-top:20px; font-style:italic;">Exported from HR Dashboard</p>
                </body>
                </html>
                """.strip()
            
                st.session_state[f"email_html_solo_{cand}"] = solo_html
                try:
                    csvs = list_csvs_for_candidate(cand)
                except Exception as e:
                    st.error(f"Failed to list CSVs for {cand}: {e}")
                    csvs = []
                
                athena_path = next((p for p in csvs if re.search(r"(athena|athen[_-]?vs[_-]?top)", p, re.I)), None)
                genos_path = next((p for p in csvs if "genos" in p.lower()), None)

                athena_df = load_csv(athena_path) if athena_path else None
                genos_df  = load_csv(genos_path)  if genos_path  else None
                
                if (athena_df is None or athena_df.empty) and (genos_df is None or genos_df.empty):
                    st.info("No Athena or Genos tables found for this candidate.")
                else:
                    if athena_df is not None and not athena_df.empty:
                        st.subheader("Athena vs Top Performers", anchor=False)
                        st.dataframe(athena_df, use_container_width=True)
                
                    if genos_df is not None and not genos_df.empty:
                        st.subheader("Genos Emotional Intelligence Scores", anchor=False)
                        st.dataframe(genos_df, use_container_width=True)
                # After you compute edited_summary (inside Solo view):
                full_html = build_candidate_email_table(
                    cand=cand,
                    use_edits=True,
                    edited_summary=edited_summary
                )
                
                clicked = st.download_button(
                    "📄 Download Summary (HTML) and remove candidate from bank",
                    data=full_html.encode("utf-8"),
                    file_name=f"{cand}_summary.html",
                    mime="text/html",
                    key=f"dl-solo-html-{cand}"
                )
                if clicked:
                    render_candidate_download(cand, full_html)  # archive the full HTML (with tables)
                
                st.divider()
            
            else:

                import compare as cmp
            
                # Let user pick who to compare against
                options = [c for c in all_candidates if c != cand]
                others = st.multiselect(
                    "Compare with...",
                    options=options,
                    default=[],
                    max_selections=3,
                    key=f"cmp-multi-{cand}",
                    help="Pick up to 3 candidates to compare.",
                )
            
                compare_mode = len(others) > 0
            
                if not compare_mode:
                    st.info("Select at least one other candidate to compare.")
                else:
                    st.info("Compare mode is active. The single-candidate editor is hidden.")
            
                    # Selected candidates
                    selected = [cand] + others
            
                    # Build + render comparison table
                    df = cmp.build_comparison_table(selected)
                    cmp.render_comparison_table(selected, title=f"{cand} vs selected candidates")
            
                    # Provide a combined editor for the first selected "other"
                    other = others[0]
                    with st.expander(f"📝 Edit combined summary: {cand} vs {other}", expanded=True):
                        cmp.render_combined_editor(cand, other, context_key="pane1")
            
            
                    combined_key = f"combined-ta-{cand}-{other}-pane1"
                    combined_text = st.session_state.get(combined_key, "")
                    if not combined_text:
                        seed_key = f"combined-draft-{cand}-{other}-pane1"
                        combined_text = st.session_state.get(seed_key, "")
            
            
                    import html as _html
                    html_doc = f"""
                    <html>
                    <body style="font-family: Arial, sans-serif; font-size: 14px; color: #222;">
                        <h2>Candidate Comparison – {cand} vs {other}</h2>
            
                        <h3>Comparison</h3>
                        <pre style="white-space: pre-wrap; line-height:1.4;">{_html.escape(combined_text or "")}</pre>
            
                        <h3>Comparison Table</h3>
                        {df.to_html(index=False, border=1, justify="left", escape=False)}
            
                        <p style="margin-top:20px; font-style:italic;">Exported from HR Dashboard</p>
                    </body>
                    </html>
                    """.strip()
            
                    # Expose for email logic (optional)
                    st.session_state[f"email_html_{cand}_{other}_pane1"] = html_doc
            
                    # Download button for the same HTML (includes the edited combined summary)
                    st.download_button(
                        "📄 Download Summary + Comparison (HTML)",
                        data=html_doc.encode("utf-8"),
                        file_name=f"{cand}_vs_{other}_summary_comparison.html",
                        mime="text/html",
                        key=f"dl-html-{cand}-{other}"
                    )
            
                    st.divider()

            try:
                csvs = list_csvs_for_candidate(cand)
            except Exception as e:
                st.error(f"Failed to list CSVs for {cand}: {e}")
                continue

            if not csvs:
                st.write("_No CSVs found for this candidate._")
                continue

            for path in csvs:
                df = load_csv(path)
    
            athena_path = next((p for p in csvs if "athena" in p.lower()), None)
            athena_df = load_csv(athena_path) if athena_path else None
            # Combined editor for the current candidate vs. the first selected "other"


            def _col(df, target: str):
                if df is None:
                    return None
                tl = target.strip().lower()
                for c in df.columns:
                    if c.strip().lower() == tl:
                        return c
                return None

            def _to_set(v):
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
            st.caption(f"Athena fit: {athena_fit:.1%}")

