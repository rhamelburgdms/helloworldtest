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

GENOS_LEGEND_HTML = """
<div style="margin-top:8px; padding:10px 12px; border:1px solid #eee; border-radius:8px; background:#fafafa; font-size:13px; line-height:1.5;">
  <strong>Genos Band Mapping</strong><br>
  1-20 <b>Very Low</b> – Exhibits this emotional intelligence trait much less often than average. Represents a real jeopardy<br>
  21-40 <b>Low</b> – Exhibits this trait less often than typical or average. Development needed<br>
  41-60 <b>Average</b> – Exhibits this trait as often as the typical person does in the workplace<br>
  61-80 <b>High</b> – Exhibits this trait more often than the typical person; well developed behavioral trait<br>
  81-99 <b>Very High</b> – Significant strength; has the ability to increase or improvement this trait in others
</div>
""".strip()

if "removed_candidates" not in st.session_state:
    st.session_state.removed_candidates = set()

def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.strip().lower()).strip("-")
# replace slugs with pretty names in text
import re as _re
def _deslug_names(text: str, mapping: dict[str, str]) -> str:
    if not text: return text or ""
    s = text
    for raw, pretty in mapping.items():
        if not raw or raw == pretty: continue
        s = _re.sub(rf'(?<![A-Za-z0-9]){_re.escape(raw)}(?![A-Za-z0-9])', pretty, s)
    return s

# replace slugs with pretty names in DataFrame (values/cols/index)
import pandas as pd
def _normalize_df_names(df: pd.DataFrame, name_map: dict[str, str]) -> pd.DataFrame:
    if df is None or df.empty or not name_map: return df
    out = df.copy()
    def _sub_all(x):
        if not isinstance(x, str): return x
        t = x
        for raw, pretty in name_map.items():
            if not raw or raw == pretty: continue
            t = _re.sub(rf'(?<![A-Za-z0-9]){_re.escape(raw)}(?![A-Za-z0-9])', pretty, t)
        return t
    out = out.applymap(_sub_all)
    out.columns = [_sub_all(str(c)) for c in out.columns]
    try: out.index = [_sub_all(str(i)) for i in out.index]
    except Exception: pass
    return out

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
        html += "<h3>Genos Scores</h3>"
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

import re as _re
import pandas as pd

def open_editor(cand: str):
    sum_key    = f"solo-ta-{cand}"      # preview state (what the user sees)
    editor_key = f"solo-editor-{cand}"  # editor state (what the textarea binds to)

    # Make sure preview has something (first run)
    if sum_key not in st.session_state:
        st.session_state[sum_key] = load_summary(cand) or ""

    # Seed the editor from the preview so it opens with exactly what was shown
    st.session_state[editor_key] = st.session_state.get(sum_key, "")

    # Keep the expander open
    st.session_state[f"edit_open_{cand}"] = True
    st.session_state.active_cand = cand


def close_editor(cand: str):
    st.session_state[f"edit_open_{cand}"] = False
    st.session_state.active_cand = cand   # keep this expander open
    
def _build_solo_html(
    cand: str,
    text: str,
    ath_df: pd.DataFrame | None,
    gen_df: pd.DataFrame | None,
    *,
    include_genos_legend: bool = True,
) -> str:
    from html import escape as _escape
    import re as _re

    # Pretty title (keep slugs only for filenames)
    title_cand = display_name(cand) if "display_name" in globals() else cand

    # Escape user text but keep **bold**
    safe_text = _escape(text or "")
    safe_text = _re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", safe_text)

    parts = [
        f"<h2 style='margin:0 0 12px 0'>Candidate Summary — {title_cand}</h2>",
        f"<div style='white-space:pre-wrap; line-height:1.5'>{safe_text}</div>",
    ]

    if ath_df is not None and not ath_df.empty:
        parts.append(
            "<h3 style='margin:20px 0 8px'>Athena scores</h3>"
            + ath_df.to_html(index=False, border=1, justify='left', escape=False)
        )
    if gen_df is not None and not gen_df.empty:
        parts.append(
            "<h3 style='margin:20px 0 8px'>Genos bands</h3>"
            + gen_df.to_html(index=False, border=1, justify='left', escape=False)
        )
        # Reuse the same legend you showed in Solo
        if 'GENOS_LEGEND_HTML' in globals() and include_genos_legend:
            parts.append(GENOS_LEGEND_HTML)

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>{title_cand} — Summary</title>
</head>
<body style="font-family:Arial, Helvetica, sans-serif; font-size:14px; color:#222; padding:24px;">
{''.join(parts)}
<p style="margin-top:24px; font-style:italic; color:#666">Exported from HR Dashboard</p>
</body>
</html>"""
   
def _build_compare_html(
    cand, other, text, ath_df, gen_df, *, include_genos_legend: bool = True
) -> str:
    from html import escape as _escape
    import re as _re

    title_cand  = display_name(cand)
    title_other = display_name(other)

    safe = _escape(text or "")
    safe = _re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", safe)

    parts = [
        f"<h2 style='margin:0 0 12px 0'>Comparison — {title_cand} vs {title_other}</h2>",
        f"<div style='white-space:pre-wrap; line-height:1.5'>{safe}</div>",
    ]

    if ath_df is not None and not ath_df.empty:
        parts.append(
            "<h3 style='margin:20px 0 8px'>Athena Scores</h3>"
            + ath_df.to_html(index=False, border=1, justify='left', escape=False)
        )

    if gen_df is not None and not gen_df.empty:
        parts.append(
            "<h3 style='margin:20px 0 8px'>Genos Scores</h3>"
            + gen_df.to_html(index=False, border=1, justify='left', escape=False)
        )
        if include_genos_legend:
            parts.append(GENOS_LEGEND_HTML)  # 👈 append legend box

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>{title_cand} vs {title_other} — Comparison</title>
</head>
<body style="font-family:Arial, Helvetica, sans-serif; font-size:14px; color:#222; padding:24px;">
{''.join(parts)}
<p style="margin-top:24px; font-style:italic; color:#666">Exported from HR Dashboard</p>
</body>
</html>"""



def display_name(s: str) -> str:
    # Replace underscores/dashes with spaces
    name = _re.sub(r'[_\-]+', ' ', s.strip('/').strip())
    # Insert a space before Capital letters that follow a lowercase (CamelCase → Camel Case)
    name = _re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', name)
    # Collapse extra spaces
    name = _re.sub(r'\s+', ' ', name).strip()
    # Optional: if ALL CAPS, title-case it
    if name.isupper():
        name = name.title()
    return name

def athena_fit_rowwise(df: pd.DataFrame) -> tuple[float, list[dict]]:
    if df is None or df.empty:
        return 0.0, []
    cols = {c.strip().lower(): c for c in df.columns}
    tp_col = cols.get("top performers")
    cf_col = cols.get("candidate value")
    trait_col = cols.get("trait")
    if not tp_col or not cf_col:
        return 0.0, []
    def _split_flags(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return set()
        if isinstance(v, str):
            parts = _re.split(r"[;,/|\n]+", v)
        else:
            parts = [str(v)]
        tokens = []
        for p in parts:
            p = p.strip().lower()
            if p:
                tokens.extend([t.strip() for t in p.split("+") if t.strip()])
        return set(tokens)

    num = den = 0
    details = []
    for _, r in df.iterrows():
        tp = _split_flags(r.get(tp_col))
        cf = _split_flags(r.get(cf_col))
        if not tp:
            continue
        m = tp & cf
        num += len(m)
        den += len(tp)
        details.append({
            "Trait": (r.get(trait_col) if trait_col else ""),
            "Top Performers (parsed)": sorted(tp),
            "Candidate Value (parsed)": sorted(cf),
            "Matches": sorted(m),
            "Row fit": (len(m) / len(tp)) if tp else 0.0,
        })
    return (num / den if den else 0.0), details
   
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
        is_open = (
            st.session_state.get("active_cand") == cand
            or st.session_state.get(f"edit_open_{cand}", False)
        )

        with st.expander(display_name(cand), expanded=is_open):


            mode = st.radio(
                "View mode",
                options=["Solo view", "Compare"],
                index=0,
                horizontal=True,
                key=f"mode-{cand}",
                on_change=partial(set_active, cand),   # ← keeps expander open
            )
            
            if mode == "Solo view":
                                # --- summary state + edit toggle ---
                sum_key    = f"solo-ta-{cand}"          # preview source of truth
                editor_key = f"solo-editor-{cand}"      # editor has its own key
                edit_key   = f"edit_open_{cand}"
                
                # seed preview once (so read-only shows something on first render)
                if sum_key not in st.session_state:
                    st.session_state[sum_key] = load_summary(cand) or ""
                if edit_key not in st.session_state:
                    st.session_state[edit_key] = False
                
                # header + buttons
                # header (inside the expander, Solo view)
                hdr, btns = st.columns([6, 2])
                with hdr:
                    st.subheader(display_name(cand), anchor=False)
                with btns:
                    # Only show "Edit summary" in the header when the editor is closed
                    if not st.session_state.get(f"edit_open_{cand}", False):
                        st.button(
                            "✏️ Edit summary",
                            key=f"edit-{cand}",
                            use_container_width=True,
                            on_click=open_editor, args=(cand,),   # seeds editor_key from preview
                        )
                    else:
                        st.empty()  # no Done here – it will live next to Save below
                
                # editor open → bind ONLY to editor_key; it was seeded from preview when opening
                if st.session_state[edit_key]:
                    st.text_area(
                        f"Edit Summary – {display_name(cand)}",
                        key=editor_key,
                        height=400,
                    )

                
                    with st.form(f"save_form_{cand}", clear_on_submit=False):
                        save_clicked = st.form_submit_button("💾 Save updated summary", use_container_width=True)
                        if save_clicked:
                            new_text = st.session_state.get(editor_key, "")
                            # update preview + persist
                            st.session_state[sum_key] = new_text
                            save_summary(cand, new_text)
                            html = build_candidate_email_table(cand=cand, use_edits=True, edited_summary=new_text)
                            render_candidate_download(cand, html)
                            st.session_state[f"last_html_{cand}"] = html
                            st.success("Saved to dashboard and archived HTML.")
                    
                            # close editor and keep this expander open
                            st.session_state[edit_key] = False
                            st.session_state.active_cand = cand
                    
                            # force the UI to re-evaluate the branch now (no second click)
                            st.rerun()


                else:
                    # read-only preview uses the preview state (sum_key)
                    st.markdown(st.session_state[sum_key] or "_No summary yet._")


            
                try:
                    csvs = list_csvs_for_candidate(cand)
                except Exception as e:
                    st.error(f"Failed to list CSVs for {cand}: {e}")
                    csvs = []
            
                athena_path = next((p for p in csvs if re.search(r"(athena|athen[_-]?vs[_-]?top)", p, re.I)), None)
                genos_path  = next((p for p in csvs if "genos" in p.lower()), None)
            
                athena_df = load_csv(athena_path) if athena_path else None
                genos_df  = load_csv(genos_path)  if genos_path  else None
            
                if athena_df is not None and not athena_df.empty:
                    athena_fit, _row_details = athena_fit_rowwise(athena_df)
                    st.markdown(f"### Athena Fit: **{athena_fit:.1%}**")
            
                if (athena_df is None or athena_df.empty) and (genos_df is None or genos_df.empty):
                    st.info("No Athena or Genos tables found for this candidate.")
                else:
                    # Compact Genos view (no duplicate columns, no long Interpretation)
                    genos_view = None
                    if genos_df is not None and not genos_df.empty:
                        genos_view = genos_df.copy()
                        # normalize headers & drop duplicate names
                        genos_view.columns = pd.Index([str(c).strip() for c in genos_view.columns])
                        genos_view = genos_view.loc[:, ~genos_view.columns.duplicated()]
                        # remove any existing 'Band' (case-insensitive)
                        band_cols = [c for c in genos_view.columns if str(c).lower() == "band"]
                        if band_cols:
                            genos_view = genos_view.drop(columns=band_cols)
                        # choose a numeric score column to map → band
                        possible_scores = [c for c in genos_view.columns
                                           if str(c).lower() in ("score", "percentile", "genos score", "overall score")]
                        score_col = next(iter(possible_scores), None)
                        if score_col is not None:
                            v = pd.to_numeric(genos_view[score_col], errors="coerce")
                            bins   = [0, 20, 40, 60, 80, 100]
                            labels = ["Very Low", "Low", "Average", "High", "Very High"]
                            genos_view["Band"] = pd.cut(v, bins=bins, labels=labels, include_lowest=True)
                        # drop the long text column if present (case-insensitive)
                        interp_cols = [c for c in genos_view.columns if str(c).lower() == "interpretation"]
                        if interp_cols:
                            genos_view = genos_view.drop(columns=interp_cols)
                        # keep concise columns if available
                        preferred = [c for c in ["Trait", score_col, "Band"] if c and c in genos_view.columns]
                        if preferred:
                            genos_view = genos_view[preferred]
            
                    # Side-by-side tables
                    GENOS_LEGEND_HTML = """
                    <div style="margin-top:8px; padding:10px 12px; border:1px solid #eee; border-radius:8px; background:#fafafa; font-size:13px; line-height:1.5;">
                      <strong>Genos Band Mapping</strong><br>
                      1-20 <b>Very Low</b> – Exhibits this emotional intelligence trait much less often than average. Represents a real jeopardy<br>
                      21-40 <b>Low</b> – Exhibits this trait less often than typical or average. Development needed<br>
                      41-60 <b>Average</b> – Exhibits this trait as often as the typical person does in the workplace<br>
                      61-80 <b>High</b> – Exhibits this trait more often than the typical person; well developed behavioral trait<br>
                      81-99 <b>Very High</b> – Significant strength; has the ability to increase or improvement this trait in others
                    </div>
                    """.strip()
            
                    tables = []
                    if athena_df is not None and not athena_df.empty:
                        tables.append(("Athena Report", athena_df, 500))
                    if genos_view is not None and not genos_view.empty:
                        tables.append(("Genos Report", genos_view, 320))
            
                    if len(tables) == 2:
                        col1, col2 = st.columns(2)
                        with col1:
                            st.subheader(tables[0][0], anchor=False)
                            st.dataframe(tables[0][1], height=tables[0][2], use_container_width=True)
                            if tables[0][0] == "Genos Report":
                                st.markdown(GENOS_LEGEND_HTML, unsafe_allow_html=True)
                        with col2:
                            st.subheader(tables[1][0], anchor=False)
                            st.dataframe(tables[1][1], height=tables[1][2], use_container_width=True)
                            if tables[1][0] == "Genos Report":
                                st.markdown(GENOS_LEGEND_HTML, unsafe_allow_html=True)
                    elif len(tables) == 1:
                        title, df_one, h = tables[0]
                        st.subheader(title, anchor=False)
                        st.dataframe(df_one, height=h, use_container_width=True)
                        if title == "Genos Report":
                            st.markdown(GENOS_LEGEND_HTML, unsafe_allow_html=True)
                    file_name = f"{_slug(cand)}-summary.html"
                    solo_html = _build_solo_html(
                        cand,
                        st.session_state.get(sum_key, ""),     # the Solo summary text shown above
                        athena_df,                             # Solo Athena df (may be None)
                        (genos_view if (genos_view is not None and not genos_view.empty) else genos_df),
                    )
                    st.download_button(
                        "📄 Download summary HTML",
                        data=solo_html.encode("utf-8"),
                        file_name=file_name,
                        mime="text/html",
                        key=f"dl-solo-{_slug(cand)}",
                        use_container_width=True,
                    )
                # Remove from dashboard
                if st.button(
                    "🗑️ Remove from dashboard",
                    key=f"rm-dash-solo-{cand}",
                    on_click=partial(set_active, cand),
                    use_container_width=True,
                ):
                    _remove_and_refresh([cand])

                
            elif mode == "Compare":
                import compare as cmp
                
                key_multi = f"cmp-multi-{cand}"

                # Build options for "others"
                options = [c for c in current_candidates if c != cand]
                
                # Seed + sanitize session value so it never contains removed candidates
                if key_multi not in st.session_state:
                    st.session_state[key_multi] = []
                st.session_state[key_multi] = [c for c in st.session_state[key_multi] if c in options]
                
                # Use key only (no default=) so the sanitized state is the source of truth
                others = st.multiselect(
                    f"Compare {display_name(cand)} with others",
                    options=options,
                    format_func=display_name,
                    key=key_multi,
                    on_change=partial(set_active, cand),
                )


                if not others:
                    st.info("You haven’t selected any candidates yet.")
                    st.stop()
                
                selected = [cand] + others
                ath_df = cmp.build_athena_table(selected)
                gen_df = cmp.build_gensos_table(selected)
                
                if ath_df.empty and gen_df.empty:
                    st.warning("No Athena/Genos data found for the selected candidates.")
                    st.stop()
                    
                # ---- Compare top: summary + editor (above tables) ----
    
                import re
                import pandas as pd
                
                # ---- Compare top: summary + editor (above tables) ----
                import pandas as pd
                
                other = others[0]
                cand_label  = display_name(cand)
                other_label = display_name(other)
                
                # 1) Keys FIRST
                editor_key  = f"cmp-summary-text-{cand}-{other}"
                open_key    = f"cmp-editor-open-{cand}-{other}"
                pending_key = f"cmp-pending-gen-{cand}-{other}"
                
                # 2) Build df_agent from the two tables
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
                
                # 3) Name map for pretty labels
                name_map = {x: display_name(x) for x in ([cand] + others)}
                
                # 4) Normalize names inside the table used for prompting
                df_agent_clean = _normalize_df_names(df_agent, name_map)  # <- requires helper defined once
                
                # 5) Load previously saved summary once
                compare_blob = f"{_slug(cand)}_vs_{_slug(other)}_cohesive_summary.html"
                if editor_key not in st.session_state:
                    try:
                        from send_back import load_summary_only
                        loaded = load_summary_only(compare_blob) or ""
                        # de-slug any old text
                        st.session_state[editor_key] = _deslug_names(loaded, name_map)
                        if loaded:
                            st.info("Loaded existing comparison summary text.")
                    except Exception:
                        st.session_state[editor_key] = ""
                
                # 6) Ensure editor toggle exists
                st.session_state.setdefault(open_key, False)
                
                # 7) If generation is pending, run it now (use pretty names + cleaned table)
                if st.session_state.get(pending_key):
                    with st.spinner("Comparing and drafting summary…"):
                        out_text = compare_summaries_agent(
                            cand=cand_label,
                            other=other_label,
                            df=df_agent_clean,
                        )
                        st.session_state[editor_key] = _deslug_names(out_text or "", name_map)
                    st.session_state[pending_key] = False
                    st.session_state[open_key] = False
                    st.toast("Draft generated — it’s displayed above the tables.", icon="📝")
                
                # 8) Summary UI (above tables)
                st.markdown("### Comparison summary")
                
                if st.session_state.get(editor_key):
                    with st.container(border=True):
                        st.markdown(st.session_state[editor_key])
                
                    c1, c2, c3, c4, c5 = st.columns([1.2, 1.1, 1.6, 1.6, 1.6])
                
                    with c1:
                        if not st.session_state[open_key]:
                            if st.button("✏️ Edit summary", key=f"open-edit-{cand}-{other}"):
                                st.session_state[open_key] = True
                                st.rerun()
                        else:
                            if st.button("✅ Done editing", key=f"close-edit-{cand}-{other}"):
                                st.session_state[open_key] = False
                                st.rerun()
                
                    with c2:
                        if st.button("🔄 Regenerate", key=f"regen-{cand}-{other}"):
                            st.session_state[pending_key] = True
                            st.rerun()
                
                    with c3:
                        if st.button("🗑️ Remove compared candidates", key=f"rm-compared-{_slug(cand)}"):
                            if others:
                                st.session_state[key_multi] = []  # clear selection to avoid default-not-in-options
                                _remove_and_refresh(others)       # this calls st.rerun()
                            else:
                                st.toast("No other candidates selected.", icon="⚠️")
                
                    with c4:
                        can_save = bool(st.session_state.get(editor_key, "").strip())
                        if st.button(
                            "💾 Save updated comparison",
                            key=f"cmp-save-{_slug(cand)}-{_slug(other)}",
                            use_container_width=True,
                            disabled=not can_save,
                            help="Save and archive to 'finished'",
                        ):
                            html_doc = _build_compare_html(cand, other, st.session_state.get(editor_key, ""), ath_df, gen_df)
                            from send_back import render_comparison_download, _archive_cc
                            render_comparison_download(cand, other, html_doc)
                            st.session_state[f"last_cmp_html_{cand}_{other}"] = html_doc
                            try:
                                from azure.storage.blob import ContentSettings
                                cc = _archive_cc()
                                cc.upload_blob(f"compare/{_slug(cand)}-vs-{_slug(other)}.html", html_doc.encode("utf-8"),
                                               overwrite=True, content_settings=ContentSettings(content_type="text/html"))
                                cc.upload_blob(f"{_slug(cand)}_vs_{_slug(other)}_cohesive_summary.html",
                                               (st.session_state.get(editor_key, "") or "").encode("utf-8"),
                                               overwrite=True, content_settings=ContentSettings(content_type="text/plain"))
                                st.toast("Saved and archived to ‘finished’.", icon="📦")
                            except Exception as e:
                                st.warning(f"Saved, but archiving to 'finished' failed: {e}")
                            st.success("Saved comparison HTML.")
                
                    with c5:
                        file_name = f"{_slug(cand)}-vs-{_slug(other)}.html"
                        last_html = st.session_state.get(f"last_cmp_html_{cand}_{other}")
                        st.download_button(
                            "📄 Download last saved HTML",
                            data=(last_html or _build_compare_html(cand, other, st.session_state.get(editor_key, ""), ath_df, gen_df)).encode("utf-8"),
                            file_name=file_name, mime="text/html", use_container_width=True,
                            key=f"dl-last-{_slug(cand)}-{_slug(other)}",
                        )
                
                    if st.session_state[open_key]:
                        edited = st.text_area(
                            "Summary editor",
                            value=st.session_state.get(editor_key, ""),
                            key=f"cmp-editor-ui-{cand}-{other}",
                            height=300,
                        )
                        st.session_state[editor_key] = edited
                
                else:
                    if st.button("✨ Generate comparison summary", key=f"gen-top-{cand}-{other}"):
                        st.session_state[pending_key] = True
                        st.rerun()
                
                st.divider()


                


                # 2) If no text yet: show the top Generate button (still ABOVE the tables)
                st.divider()
                
                # ---- Now render the tables AFTER the summary section ----
                if ath_df is not None and not ath_df.empty:
                    st.markdown("### Athena scores")
                    st.dataframe(ath_df, use_container_width=True)
                
                if gen_df is not None and not gen_df.empty:
                    st.markdown("### Genos scores")
                    st.dataframe(gen_df, use_container_width=True)
                
               
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

            def athena_fit_rowwise(df: pd.DataFrame) -> tuple[float, list[dict]]:
                """
                Row-wise Athena fit:
                  - For each row, compute (# matches between Top Performers & Candidate Value) / (# Top Performers flags on that row).
                  - Return the average across rows that have at least 1 Top Performer flag.
            
                Returns:
                  (fit_avg, details)
                    fit_avg: float in [0,1]
                    details: list of per-row dicts (for optional debugging/UX)
                """
                if df is None or df.empty:
                    return 0.0, []
            
                # Column lookups (same style you already use)
                tp_col = _col(df, "Top Performers")
                cf_col = _col(df, "Candidate Value")
                trait_col = _col(df, "Trait") or "Trait"
            
                if not tp_col or not cf_col:
                    return 0.0, []
            
                # Slightly more generous splitter: includes newlines
                import re as _re
                def _to_set_local(v):
                    if v is None or (isinstance(v, float) and pd.isna(v)):
                        return set()
                    if isinstance(v, str):
                        parts = _re.split(r"[;,/|\n]+", v)
                    elif isinstance(v, (list, tuple, set)):
                        parts = list(v)
                    else:
                        parts = [str(v)]
                    # normalize tokens
                    tokens = []
                    for p in parts:
                        p = p.strip().lower()
                        if not p:
                            continue
                        # break “unique + excellent” into two flags
                        tokens.extend([t.strip() for t in p.split("+") if t.strip()])
                    return set(tokens)
            
                rows, num, den = [], 0, 0
                for _, r in df.iterrows():
                    tp = _to_set_local(r.get(tp_col))
                    cf = _to_set_local(r.get(cf_col))
                    if not tp:
                        # no denominator this row
                        continue
                    match = tp & cf
                    row_den = len(tp)
                    row_num = len(match)
                    den += row_den
                    num += row_num
                    rows.append({
                        "Trait": r.get(trait_col, ""),
                        "Top Performers (parsed)": sorted(tp),
                        "Candidate Value (parsed)": sorted(cf),
                        "Matches": sorted(match),
                        "Row fit": row_num / row_den if row_den else 0.0,
                    })
            
                fit_avg = (num / den) if den else 0.0
                return float(fit_avg), rows


            #athena_fit, row_details = athena_fit_rowwise(athena_df)
            #st.caption(f"Athena fit (row-weighted): {athena_fit:.1%}")
            
        
