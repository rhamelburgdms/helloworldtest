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
st.session_state.setdefault("refresh_nonce", 0)

@st.cache_data(ttl=5)
def list_candidate_prefixes(_nonce: int) -> list[str]:
    cc = get_cc()
    prefixes = set()
    for item in cc.walk_blobs(delimiter="/"):
        if hasattr(item, "name") and item.name:
            p = item.name.strip("/")
            if p:
                prefixes.add(p)
    return sorted(prefixes)

# use it:
current_candidates = list_candidate_prefixes(st.session_state["refresh_nonce"])

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

# Here's where we build the download piece that makes it easy to paste into an email.
def build_candidate_email_table(cand: str, use_edits: bool, edited_summary: str) -> str:
    # Load CSVs for the candidate
    csvs = list_csvs_for_candidate(cand)
    athena_path = next((p for p in csvs if "athena" in p.lower()), None)
    genos_path = next((p for p in csvs if "genos" in p.lower()), None)

    athena_df = load_csv(athena_path) if athena_path else None
    genos_df = load_csv(genos_path) if genos_path else None
    # Start HTML email structure
    from html import escape as _escape
    import re as _re

    raw = edited_summary if use_edits else load_summary(cand)
    raw = raw or ""
    safe_text = _escape(raw)
    safe_text = _re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", safe_text)
    
    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; font-size: 14px; color: #222;">
        <h2>Candidate Summary – {cand}</h2>
        <div style="white-space: pre-wrap; line-height:1.5;">
            {safe_text}
        </div>
    </body>
    </html>
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
    removed, missing = [], []
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

    # Ensure no stale list survives
    st.session_state.pop("candidates", None)

    # Clear cached list_candidate_prefixes() and CSV loads
    st.cache_data.clear()
    st.rerun()

st.title("Candidate Bank")
st.caption("Expand each candidate to view data, make comparisons, and edit summaries.")

# Track which expander should remain open across reruns
if "open_cand" not in st.session_state:
    st.session_state["open_cand"] = None

# List candidates
# Always compute fresh (list_candidate_prefixes is @st.cache_data but you clear it on delete)
#current_candidates = list_candidate_prefixes()

# Hide anything you just removed in this session (instant UX)
current_candidates = [c for c in current_candidates if c not in st.session_state.removed_candidates]

if not current_candidates:
    st.info("No candidates are pending approval.")

else:
    for cand in current_candidates:
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

            if mode == "Solo view":
                # OPTIONAL: clear compare picks when leaving Compare
                st.session_state.pop(f"cmp-multi-{cand}", None)
            # === SOLO VIEW: edit -> SAVE (form) -> then download the exact saved payload ===

                state_key = f"solo-ta-{cand}"
                edited_summary = st.text_area(
                    f"Edit Summary – {cand}",
                    value=st.session_state.get(state_key, load_summary(cand)),
                    height=260,
                    key=state_key,
                    on_change=partial(set_active, cand),
                )
                st.session_state[f"edited_summary_{cand}"] = edited_summary
                
                # Load CSVs (safe if missing) — keep your existing table code
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
                
                # Save first (form), then expose a download bound to the saved payload
                with st.form(f"save_form_{cand}", clear_on_submit=False):
                    submitted = st.form_submit_button("💾 Save updated summary")
                    if submitted:
                        # 1) Persist the plain-text summary to dashboard/{cand}/summary.txt
                        save_summary(cand, edited_summary)
                
                        # 2) Build HTML directly from the edited text in memory (no blob reads)
                        html = build_candidate_email_table(
                            cand=cand,
                            use_edits=True,
                            edited_summary=edited_summary,
                        )
                
                        # 3) Archive the HTML to the finished container (upload can take time, that's fine)
                        render_candidate_download(cand, html)
                
                        # 4) Stash the exact payload we just saved/archived for a guaranteed-correct download
                        st.session_state[f"last_html_{cand}"] = html
                
                        st.success("Saved to dashboard and archived HTML.")
                
                # Outside the form: always offer the **last saved** HTML for download
                if html := st.session_state.get(f"last_html_{cand}"):
                    st.download_button(
                        "📄 Download last saved HTML",
                        data=html.encode("utf-8"),
                        file_name=f"{cand}_summary.html",
                        mime="text/html",
                        key=f"dl-last-{cand}",
                    )

                if st.button(
                    "🗑️ Remove from dashboard",
                    key=f"rm-dash-solo-{cand}",
                    on_click=partial(set_active, cand),
                ):
                    _remove_and_refresh([cand])  # ← uses the helper that clears st.cache_data and reruns
            elif mode == 'Compare':
                import compare as cmp
            
                key_multi = f"cmp-multi-{cand}"
                others = st.multiselect(
                    f"Compare {cand} with others",
                    options=[c for c in current_candidates if c != cand],
                    default=st.session_state.get(key_multi, []),
                    key=key_multi,
                    on_change=partial(set_active, cand),
                )
                if not others:
                    st.info("Pick at least one other candidate to compare with this one.")
                    continue
            
                selected = [cand] + others
                ath_df = cmp.build_athena_table(selected)
                gen_df = cmp.build_gensos_table(selected)
            
                if ath_df.empty and gen_df.empty:
                    st.warning("No Athena/Genos data found for the selected candidates.")
                    st.stop()
            
                if not ath_df.empty:
                    st.markdown("### Athena scores")
                    st.dataframe(ath_df, use_container_width=True)
                if not gen_df.empty:
                    st.markdown("### Genos bands")
                    st.dataframe(gen_df, use_container_width=True)
            
                other = others[0]
            
                # ----- Build DF for agent (unchanged) -----
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
            
                compare_blob = f"{_slug(cand)}_vs_{_slug(other)}_cohesive_summary.html"
                editor_key   = f"cmp-summary-text-{cand}-{other}"  # stores TEXT only
            
                from send_back import load_summary_only
                if editor_key not in st.session_state:
                    if _finished_exists(compare_blob):
                        st.session_state[editor_key] = load_summary_only(compare_blob)
                        st.info("Loaded existing cohesive summary text.")
                    else:
                        st.session_state[editor_key] = ""
            
                pending_flag = f"pending_gen_{cand}_{other}"
                if st.session_state.get(pending_flag):
                    with st.spinner("Comparing…"):
                        out_text = compare_summaries_agent(cand=cand, other=other, df=df_agent)
                        st.session_state[editor_key] = out_text
                    st.session_state[pending_flag] = False
                    st.toast("Draft generated — edit it below.", icon="📝")
            
                # ---------- Editor FIRST so its value commits before any buttons ----------
                summary_text = st.text_area(
                    "Cohesive summary",
                    key=editor_key,
                    height=300,
                    help="Edit the generated draft before saving/downloading",
                )
            
                # ---------- Toolbar ----------
                t1, t2, t3, t4 = st.columns([1.3, 1.6, 1.6, 1.3])

                    def _slug_local(s: str) -> str:
                        import re as _re
                        return _re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")
                
                    # Helper to build the HTML from current text + tables (same rendering as yours)
                    def _build_compare_html(cand: str, other: str, text: str, ath_df: pd.DataFrame | None, gen_df: pd.DataFrame | None) -> str:
                        from html import escape as _escape
                        import re as _re
                
                        # sections
                        sections_html = []
                        if ath_df is not None and not ath_df.empty:
                            sections_html.append("<h3>Athena scores</h3>" + ath_df.to_html(index=False, border=1, justify="left", escape=False))
                        if gen_df is not None and not gen_df.empty:
                            sections_html.append("<h3>Genos bands</h3>" + gen_df.to_html(index=False, border=1, justify="left", escape=False))
                
                        raw = (text or "").replace("\r\n", "\n")
                        pattern = r'(?<![/\d])\b\d+\.\s+'  # robust inline numbered list
                        matches = list(_re.finditer(pattern, raw))
                        if len(matches) >= 2:
                            before = raw[:matches[0].start()].strip()
                            tail   = raw[matches[0].start():]
                            items  = [p.strip() for p in _re.split(pattern, tail) if p.strip()]
                            list_html = "<ol>" + "".join(f"<li>{_escape(it)}</li>" for it in items) + "</ol>"
                            head_html = f'<div style="white-space: pre-wrap; line-height:1.5;">{_escape(before)}</div>' if before else ""
                            current_html = head_html + list_html
                        else:
                            current_html = f'<div style="white-space: pre-wrap; line-height:1.5;">{_escape(raw.strip())}</div>'
                
                        # Convert **bold** to <strong> after escaping/building
                        current_html = _re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", current_html)
                
                        html_doc = f"""
                        <html>
                        <body style="font-family: Arial, sans-serif; font-size: 14px; color: #222;">
                          <h2>Cohesive Summary – {cand} vs {other}</h2>
                          <!-- SUMMARY_START -->
                          {current_html}
                          <!-- SUMMARY_END -->
                          {''.join(sections_html)}
                          <p style="margin-top:20px; font-style:italic;">Exported from HR Dashboard</p>
                        </body>
                        </html>
                        """.strip()
                        return html_doc
                
                    # 1) Generate (disabled once text exists)
                    with t1:
                        if not st.session_state.get(editor_key):
                            if st.button(
                                "✨ Generate cohesive summary",
                                key=f"gen-{cand}-{other}",
                                help="Draft a first pass using the comparison tables",
                                on_click=partial(set_active, cand),
                            ):
                                st.session_state[pending_flag] = True
                                st.rerun()
                        else:
                            st.button("✨ Generate cohesive summary", disabled=True, key=f"gen-disabled-{cand}-{other}")
                
                    # 2) SAVE (form) → persist + stash last_html
                    with t2:
                        with st.form(f"cmp_save_form_{_slug_local(cand)}_{_slug_local(other)}", clear_on_submit=False):
                            save_clicked = st.form_submit_button("💾 Save updated comparison")
                            if save_clicked:
                                html_doc = _build_compare_html(cand, other, st.session_state.get(editor_key, ""), ath_df, gen_df)
                
                                # Archive/upload first
                                from send_back import render_comparison_download
                                render_comparison_download(cand, other, html_doc)
                
                                # Then stash the exact saved payload for deterministic download
                                st.session_state[f"last_cmp_html_{cand}_{other}"] = html_doc
                                st.success("Saved comparison HTML.")
                
                    # 3) DOWNLOAD always serves the *last saved* payload
                    with t3:
                        file_name = f"{_slug_local(cand)}-vs-{_slug_local(other)}.html"
                        last_html = st.session_state.get(f"last_cmp_html_{cand}_{other}")
                        st.download_button(
                            "📄 Download last saved HTML",
                            data=(last_html or _build_compare_html(cand, other, st.session_state.get(editor_key, ""), ath_df, gen_df)).encode("utf-8"),
                            file_name=file_name,
                            mime="text/html",
                            key=f"dl-last-{_slug_local(cand)}-{_slug_local(other)}",
                            help="Downloads the most recently saved comparison HTML",
                        )

                
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

