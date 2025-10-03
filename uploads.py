import os, re, unicodedata, zipfile, io
from pathlib import Path
import streamlit as st
from azure.storage.blob import BlobServiceClient, ContentSettings

RAW_CONTAINER = os.getenv("RAW_CONTAINER", "raw")
bsc = BlobServiceClient.from_connection_string(os.environ["AZURE_STORAGE_CONNECTION_STRING"])

def to_pascal_compact(name: str) -> str:
    if not name:
        return ""
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    tokens = re.split(r"[^A-Za-z0-9]+", name)
    tokens = [t for t in tokens if t]
    return "".join(t.capitalize() for t in tokens)

from pathlib import Path
import streamlit as st
from azure.storage.blob import ContentSettings

st.subheader("Upload candidate folder")

# Let the user type the candidate's name
candidate_name = st.text_input("Enter candidate name (FirstName LastName)")

uploaded_files = st.file_uploader(
    "Upload up to 5 files for this candidate",
    accept_multiple_files=True,
    type=["pdf", "csv", "docx"]
)

if uploaded_files and candidate_name and st.button("Upload", key="upload_btn"):
    candidate_id = to_pascal_compact(candidate_name)
    cc = bsc.get_container_client(RAW_CONTAINER)

    for uploaded_file in uploaded_files[:5]:
        file_data = uploaded_file.read()
        filename = uploaded_file.name

        blob_name = f"{candidate_id}/{filename}"
        content_type = (
            "application/pdf" if filename.lower().endswith(".pdf")
            else "text/csv" if filename.lower().endswith(".csv")
            else "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            if filename.lower().endswith(".docx")
            else "application/octet-stream"
        )

        cc.upload_blob(
            name=blob_name,
            data=file_data,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type),
        )

    st.success(f"Uploaded {len(uploaded_files[:5])} files to raw/{candidate_id}/")



