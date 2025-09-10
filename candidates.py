import os, io, re, json
import pandas as pd
import streamlit as st
import html as _html
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.identity import DefaultAzureCredential
#from config import make_bsc, _download_blob_bytes
from agent_comparer import compare_summaries_agent 
from send_back import render_candidate_download, delete_candidate_from_dashboard
st.set_page_config(page_title="Candidate Page", page_icon="🧩", layout="wide")
from send_back import _archive_cc 

import re
if "removed_candidates" not in st.session_state:
    st.session_state.removed_candidates = set()

def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.strip().lower()).strip("-")

def _finished_exists(blob_name: str) -> bool:
    cc = _archive_cc()
    try:
        return cc.get_blob_client(blob_name).exists()
    except Exception:
        return False

def _finished_load(blob_name: str) -> str | None:
    cc = _archive_cc()
    try:
        return cc.download_blob(blob_name).readall().decode("utf-8", errors="replace")
    except Exception:
        return None

if "active_cand" not in st.session_state:
    st.session_state.active_cand = None
    
from functools import partial

def set_active(cand: str):
    st.session_state.active_cand = cand

if "compare_selections" not in st.session_state:
    st.session_state.compare_selections = {}   # {cand: [others]}
if "compare_triggered" not in st.session_state:
    st.session_state.compare_triggered = {}    # {cand: bool}
if "last_loaded_tables" not in st.session_state:
    st.session_state.last_loaded_tables = {}   # {cand: {"athena": df, "genos": df}}

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
@st.cache_data(ttl=600)
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
#@st.cache_data(ttl=600)
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
      
#@st.cache_data(show_spinner=True)
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
    
def _remove_and_refresh(cands: list[str]):
    removed = []
    missing = []
    for name in cands:
        try:
            deleted_count, _ = delete_candidate_from_dashboard(name)
            if deleted_count > 0:
                removed.append(name)
                st.session_state.removed_candidates.add(name)
            else:
                missing.append(name)
        except Exception:
            missing.append(name)

    if removed:
        st.toast(f"Removed {len(removed)} candidate(s): {', '.join(removed)}", icon="✅")
    if missing:
        st.toast(f"No files found for: {', '.join(missing)}", icon="⚠️")
    st.cache_data.clear()
    st.rerun()

st.title("Candidate Bank")
st.caption("Expand each candidate to view data, make comparisons, and edit summaries.")
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
candidates = [c for c in candidates if c not in st.session_state.removed_candidates]

if not candidates:
    st.info("No candidates are pending approval.")

else:
    for cand in candidates:
    # keep this expander open if it was the last interacted one
        is_open = (st.session_state.active_cand == cand)
    
        with st.expander(cand, expanded=is_open):
            # Any widget inside should mark this cand as active on change
            mode = st.radio(
                "View mode",
                options=["Solo view", "Compare"],
                index=0,
                horizontal=True,
                key=f"mode-{cand}",
                on_change=partial(set_active, cand),   # ← keeps expander open
            )
    
            # Example: compare multiselect
            selected = st.multiselect(
                f"Compare {cand} with others",
                options=[c for c in all_candidates if c != cand],
                default=st.session_state.get("compare_selections", {}).get(cand, []),
                key=f"cmp-multi-{cand}",
                on_change=partial(set_active, cand),   # ← keeps expander open
            )
    
            # Example: text area in Solo view
            if mode == "Solo view":
                # Get current text
                edited_summary = st.text_area(
                    f"Edit Summary – {cand}",
                    value=load_summary(cand),
                    height=260,
                    key=f"solo-ta-{cand}",
                    on_change=partial(set_active, cand),
                )
            
                # Keep it in session for later reuse
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
            
                # Load CSVs (safe if missing)
                try:
                    csvs = list_csvs_for_candidate(cand)
                except Exception as e:
                    st.error(f"Failed to list CSVs for {cand}: {e}")
                    csvs = []
            
                athena_path = next((p for p in csvs if re.search(r"(athena|athen[_-]?vs[_-]?top)", p, re.I)), None)
                genos_path  = next((p for p in csvs if "genos" in p.lower()), None)
            
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
            
                # Full HTML including tables (uses the edited text)
                full_html = build_candidate_email_table(
                    cand=cand,
                    use_edits=True,
                    edited_summary=edited_summary,
                )
                

                # Solo: one-click Save & Download, then remove the candidate
                clicked = st.download_button(
                    "📄 Save and Download (HTML)",
                    data=full_html.encode("utf-8"),
                    file_name=f"{cand}_summary.html",
                    mime="text/html",
                    key=f"dl-solo-html-{cand}",
                )
                
                if clicked:
                    # persist the plain-text summary where the app reads it from
                    save_summary(cand, edited_summary)         # writes dashboard/{cand}/summary.txt
                
                    # also archive the pretty HTML export
                    render_candidate_download(cand, full_html) # currently goes to Finished
                    st.success("Saved summary to dashboard and archived HTML.")

                if st.button(
                    "🗑️ Remove from dashboard",
                    key=f"rm-dash-solo-{cand}",
                    on_click=partial(set_active, cand),
                ):
                    deleted_count, _ = delete_candidate_from_dashboard(cand)
                    if deleted_count > 0:
                        st.toast(f"Removed {cand} from dashboard ({deleted_count} files).", icon="✅")
                    else:
                        st.toast(f"No files found for {cand} under dashboard/", icon="⚠️")
                    st.session_state.setdefault("removed_candidates", set()).add(cand)
                    st.rerun()            
            else:

    
                import compare as cmp
            
                # Reuse the existing multiselect; don't recreate it with the same key
                key_multi = f"cmp-multi-{cand}"
                others = st.session_state.get(key_multi, [])
                compare_mode = len(others) > 0
                
                if len(all_candidates) <= 1:
                    st.info("Comparison requires at least two candidates in the dashboard.")
                    st.stop()
                else:
                    st.info("Compare mode is active. The single-candidate editor is hidden.")
            
                    # Selected candidates
                    # Selected candidates
                selected = [cand] + others
                
                # --- build & render the two separate tables (on-screen only) ---
                ath_df = cmp.build_athena_table(selected)
                gen_df = cmp.build_gensos_table(selected)
                
                if ath_df.empty and gen_df.empty:
                    st.warning("No Athena/Genos data found for the selected candidates.")
                else:
                    if not ath_df.empty:
                        st.markdown("### Athena scores")
                        st.dataframe(ath_df, use_container_width=True)
                    if not gen_df.empty:
                        st.markdown("### Genos bands")
                        st.dataframe(gen_df, use_container_width=True)
                
                # Pick the "other" candidate (first selected)
                other = others[0]
                # Build a single DF for the agent (from the two tables)
                import pandas as pd
                parts = []
                if ath_df is not None and not ath_df.empty:
                    a = ath_df.drop(columns=["Top Performers"], errors="ignore").copy()
                    a.insert(0, "Section", "Athena")
                    parts.append(a)
                if gen_df is not None and not gen_df.empty:
                    g = gen_df.copy()
                    g.insert(0, "Section", "Genos")
                    parts.append(g)
                df_agent = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

                # Blob path + editor key
                # ---- identify blob + editor key ------------------------------------------------
                compare_blob = f"{_slug(cand)}_vs_{_slug(other)}_cohesive_summary.html"
                editor_key   = f"cmp-summary-text-{cand}-{other}"  # stores TEXT only
                
                # ---- preload previously-saved text (once) -------------------------------------
                from send_back import load_summary_only
                if editor_key not in st.session_state:
                    if _finished_exists(compare_blob):
                        st.session_state[editor_key] = load_summary_only(compare_blob)
                        st.info("Loaded existing cohesive summary text.")
                    else:
                        st.session_state[editor_key] = ""  # empty until user generates
                
                # ---- (re)build the agent dataframe upfront ------------------------------------
                parts = []
                if ath_df is not None and not ath_df.empty:
                    a = ath_df.drop(columns=["Top Performers"], errors="ignore").copy()
                    a.insert(0, "Section", "Athena")
                    parts.append(a)
                if gen_df is not None and not gen_df.empty:
                    g = gen_df.copy()
                    g.insert(0, "Section", "Genos")
                    parts.append(g)
                df_agent = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
                
                # ---- handle a pending generate BEFORE any widgets are drawn --------------------
                pending_flag = f"pending_gen_{cand}_{other}"
                if st.session_state.get(pending_flag):
                    with st.spinner("Comparing…"):
                        out_text = compare_summaries_agent(cand=cand, other=other, df=df_agent)
                        st.session_state[editor_key] = out_text
                    st.session_state[pending_flag] = False
                    st.toast("Draft generated — edit it below.", icon="📝")
                
                # ---- UI: show either the Generate button OR the editor -------------------------
                cols = st.columns([1, 1])
                
                with cols[0]:
                    # Only show the Generate button when there's no text yet
                    if not st.session_state.get(editor_key):
                        if st.button("✨ Generate cohesive summary", key=f"gen-{cand}-{other}",
                                     on_click=partial(set_active, cand)):
                            st.session_state[pending_flag] = True
                            st.rerun()
                
                # Only render the editor AFTER we have text (from prior save or after generate)
                if st.session_state.get(editor_key):
                    summary_text = st.text_area(
                        "Cohesive summary",
                        key=editor_key,      # bind by key only; no value= to avoid re-instantiation issues
                        height=300,
                    )
                else:
                    st.info("Click **Generate cohesive summary** to create a draft.")
                
                # ---- build HTML (tables already computed) -------------------------------------
                with cols[1]:
                    sections_html = []
                    if ath_df is not None and not ath_df.empty:
                        sections_html.append(
                            "<h3>Athena scores</h3>" +
                            ath_df.to_html(index=False, border=1, justify="left", escape=False)
                        )
                    if gen_df is not None and not gen_df.empty:
                        sections_html.append(
                            "<h3>Genos bands</h3>" +
                            gen_df.to_html(index=False, border=1, justify="left", escape=False)
                        )
                
                    from html import escape as _escape
                    html_doc = f"""
                    <html>
                    <body style="font-family: Arial, sans-serif; font-size: 14px; color: #222;">
                      <h2>Cohesive Summary – {cand} vs {other}</h2>
                
                      <!-- SUMMARY_START -->
                      <div id="summary-text" style="white-space: pre-wrap; line-height:1.5;">
                        {_escape((st.session_state.get(editor_key) or '').strip())}
                      </div>
                      <!-- SUMMARY_END -->
                
                      {''.join(sections_html)}
                      <p style="margin-top:20px; font-style:italic;">Exported from HR Dashboard</p>
                    </body>
                    </html>
                    """.strip()
                
                    file_name = f"{_slug(cand)}-vs-{_slug(other)}.html"
                    clicked = st.download_button(
                        "💾 Save & Download (HTML)",
                        data=html_doc.encode("utf-8"),
                        file_name=file_name,
                        mime="text/html",
                        key=f"dl-{_slug(cand)}-{_slug(other)}",
                    )
                    if clicked:
                        from send_back import render_comparison_download
                        render_comparison_download(cand, other, html_doc)
                        st.success("Saved & ready to download.")
                
                            
                    else:
                        # Optional: allow override
                        if st.button(
                            "♻️ Regenerate anyway",
                            key=f"regen-{cand}-{other}",
                            on_click=partial(set_active, cand),
                        ):
                            # Clear session and (optionally) delete old blob; then rerun
                            st.session_state.pop(agent_key, None)
                            # If you want to delete the old blob too:
                            # try: _archive_cc().delete_blob(compare_blob)
                            # except Exception: pass
                            st.experimental_rerun()

                    
                    # 'selected' already exists above as: selected = [cand] + others
                    cols_rm = st.columns([1, 1])
                    with cols_rm[1]:
                        # Removes the primary + all currently compared candidates in this expander
                        if st.button("🗑️ Remove all compared candidates", key=f"rm-all-{_slug(cand)}"):
                            _remove_and_refresh(selected)


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

