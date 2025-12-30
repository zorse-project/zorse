"""Ingestion module for The Stack v2 dataset via Software Heritage."""

import os
from typing import Optional

import boto3
from datasets import load_dataset
from dotenv import load_dotenv
from loguru import logger
from smart_open import open as sopen

from processing.filters import passes_filters

load_dotenv()

# Valid extensions for each language (case-insensitive)
VALID_EXTENSIONS = {
    "COBOL": {
        "cbl", "cob", "cobol", "cpy", "ccp", "wks", "pco"
    },
    "REXX": {
        "rexx", "rex", "rx", "rxj", "pprx", "orx", "rexg", "exec"
    },
    "RPGLE": {
        "rpgle", "sqlrpgle", "rpg", "dds"
    },
}

# Initialize AWS S3 client for Software Heritage
session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
)
s3 = session.client("s3")


def download_blob(id: str, encoding: str) -> Optional[str]:
    """Download blob content from Software Heritage S3."""
    s3_url = f"s3://softwareheritage/content/{id}"

    try:
        with sopen(
            s3_url, "rb", compression=".gz", transport_params={"client": s3}
        ) as fin:
            content = fin.read().decode(encoding)
    except Exception as e:
        logger.error(f"Failed downloading from {s3_url} with error: {e}")
        return None

    return content


def load_stack(language: str):
    """
    Load and yield normalized rows from The Stack v2 for a given language.
    
    Args:
        language: Language name (e.g., "COBOL", "REXX", "RPGLE")
        
    Yields:
        Dictionary with normalized row data matching the unified schema
    """
    dataset = load_dataset(
        "bigcode/the-stack-v2-dedup",
        data_dir=f"data/{language}",
        token=os.environ["HF_TOKEN"],
        split="train",
    )
    
    dataset = dataset.map(
        lambda row: {
            "content": download_blob(row["blob_id"], row["src_encoding"]),
            "licenses": row["detected_licenses"],
            "license_type": row["license_type"],
            "host_url": "https://github.com",
            "repo_name": row["repo_name"],
            "file_path": row["path"],
            "language": row["language"],
            "extension": row["extension"],
            "branch": row["branch_name"],
            "revision_id": row["revision_id"],
            "commit_date": row["committer_date"].isoformat(),
        },
        remove_columns=dataset.column_names
    )
    
    # Get valid extensions for this language
    valid_extensions = VALID_EXTENSIONS.get(language.upper(), set())
    
    count = 0
    skipped_extensions = 0
    
    for row in dataset:
        if row["content"] is None:
            continue
        
        # Filter by extension if specified for this language
        if valid_extensions:
            extension = row["extension"].lower().lstrip(".") if row["extension"] else ""
            if extension not in valid_extensions:
                skipped_extensions += 1
                continue
            
        # Apply shared filtering logic
        num_tokens = passes_filters(row["content"])
        if num_tokens is None:
            continue
        
        # Normalize to unified schema
        normalized_row = {
            "content": row["content"],
            "repo_name": row["repo_name"],
            "file_path": row["file_path"],
            "language": row["language"],
            "extension": row.get("extension", ""),
            "license_type": row["license_type"],  # License type (string)
            "licenses": row.get("licenses", []),  # Array of licenses
            "host_url": row["host_url"],
            "source": "stack",
            "num_tokens": num_tokens,
            "revision_id": row["revision_id"],
            "commit_date": row["commit_date"],
            "branch": row.get("branch", ""),  # Include branch
        }
        
        yield normalized_row
        count += 1
    
    if skipped_extensions > 0:
        logger.info(f"Loaded {count} files for {language} (skipped {skipped_extensions} files with invalid extensions)")
    else:
        logger.info(f"Loaded {count} files for {language}")

