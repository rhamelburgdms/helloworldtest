import os, io, re, json
import pandas as pd
import streamlit as st
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.identity import DefaultAzureCredential

st.set_page_config(page_title="Candidates", page_icon="🧩", layout="wide")


CONTAINER = os.getenv("CONTAINER", "dashboard")   # everything comes from 'dashboard'
CSV_EXTS = {".csv"}

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

@st.cache_data(ttl=30)
def list_candidate_prefixes() -> list[str]:
    """Top-level prefixes inside the dashboard container."""
    cc = get_cc()
    prefixes = set()
    try:
        for item in cc.walk_blobs(delimiter="/"):
            if hasattr(item, "name") and item.name:
                p = item.name.strip("/")
                if p:
                    prefixes.add(p)
    except TypeError:
        for blob in cc.list_blobs():
            parts = blob.name.split("/", 1)
            if len(parts) == 2:
                prefixes.add(parts[0])
    return sorted(prefixes)

@st.cache_data(ttl=30)
def list_csvs_for_candidate(cand: str) -> list[str]:
    """List CSV blob paths under dashboard/{cand}/"""
    cc = get_cc()
    start = cand.rstrip("/") + "/"
    paths = []
    for blob in cc.list_blobs(name_starts_with=start):
        if blob.name.lower().endswith(".csv"):
            paths.append(blob.name)
    return sorted(paths)

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
    b = _download_blob_bytes(f"{cand}/summary.txt")
    return b.decode("utf-8", errors="replace") if b is not None else ""

def save_summary(cand: str, text: str):
    """Write dashboard/{cand}/summary.txt with text/plain content type."""
    get_cc().upload_blob(
        f"{cand}/summary.txt",
        text.encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="text/plain"),
    )


def _df_to_markdown(df: pd.DataFrame) -> str:
    """Prefer a real Markdown table; fallback to code block if tabulate isn't installed."""
    try:
        # requires 'tabulate' to be installed for pretty MD tables
        return df.to_markdown(index=False)
    except Exception:
        # readable monospaced fallback
        return "```\n" + df.to_string(index=False) + "\n```"

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

    # Add Athena table if available
    if athena_df is not None and not athena_df.empty:
        html += "<h3>Athena vs Top Performers</h3>"
        html += athena_df.to_html(
            index=False,
            border=1,
            justify="left",
            classes="dataframe",
            escape=False
        )

    # Add Genos table if available
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
            # --- Inline summary editor ---
            st.subheader("Candidate Summary", anchor=False)
            current_summary = load_summary(cand)
            edited_summary = st.text_area(
                f"Edit Summary – {cand}",
                value=current_summary,
                height=220,
                key=f"summary-editor-{cand}",
                placeholder="No summary yet for this candidate."
            )

            cols = st.columns([1, 1, 2, 6])   # Save / Revert / Download MD / spacer
            with cols[0]:
                if st.button("Save Summary", key=f"save-summary-{cand}"):
                    try:
                        save_summary(cand, edited_summary)
                        st.success("Summary saved to dashboard container.")
                    except Exception as e:
                        st.error(f"Failed to save summary: {e}")


            with cols[2]:
                use_edits = st.checkbox("Use current edits", value=True, key=f"use-edits-{cand}")
                email_html = build_candidate_email_table(cand, use_edits, edited_summary)
                st.download_button(
                    "📄 Download Email-Ready Summary",
                    data=email_html,
                    file_name=f"{cand}_summary.html",
                    mime="text/html",
                    key=f"dltxt-{cand}",
                    help="Download a ready-to-paste HTML summary with tables and borders."
                )



            st.divider()

            # --- CSVs for this candidate (optional display) ---
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
                if df is not None:
                    st.markdown(f"**{path.split('/')[-1]}**")
                    st.dataframe(df, use_container_width=True)
                else:
                    st.warning(f"Failed to load `{path}`.")

            # --- Athena fit calculation (using dashboard CSVs) ---
            athena_path = next((p for p in csvs if "athena" in p.lower()), None)
            athena_df = load_csv(athena_path) if athena_path else None

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
