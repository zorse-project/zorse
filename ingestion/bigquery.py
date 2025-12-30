"""Ingestion module for GitHub data via Google BigQuery."""

import base64
from typing import Optional

from google.cloud import bigquery
from loguru import logger

from processing.filters import passes_filters
from processing.licenses import classify_license_type

# Valid extensions for each language (case-insensitive)
VALID_EXTENSIONS = {
    "JCL": {
        "jcl", "job","proc", "prc", "cntl"
    },
    "PL/I": {
        "pli", "pl1", "plinc"
    },
    "HLASM": {
        "hla", "hlasm", "assemble"
    },
    "BMS": {
        "bms", "bmc"
    },
}

# Lazy-load BigQuery client to avoid import-time credential checks
_client: Optional[bigquery.Client] = None


def _get_client() -> bigquery.Client:
    """Get or initialize the BigQuery client (lazy loading)."""
    global _client
    if _client is None:
        _client = bigquery.Client()
    return _client


def _extract_extension(file_path: str) -> str:
    """Extract file extension from path."""
    if not file_path:
        return ""
    # Get the last part after the last dot
    parts = file_path.rsplit(".", 1)
    if len(parts) > 1:
        return parts[-1].lower()
    return ""


def _infer_language_from_extension(extension: str) -> str:
    """Infer language from file extension."""
    extension = extension.lower().lstrip(".")
    
    # Check each language's valid extensions
    for language, extensions in VALID_EXTENSIONS.items():
        if extension in extensions:
            return language
    
    # Default fallback
    if extension == "jcl":
        return "JCL"
    elif extension in ("pli", "pl1"):
        return "PL/I"
    elif extension == "asm":
        return "HLASM"
    elif extension == "bms":
        return "BMS"
    
    return "UNKNOWN"


def load_bigquery():
    """
    Query GitHub public repos via BigQuery and yield normalized rows.
    
    Extracts mainframe-related files:
    - JCL (.jcl, .proc, .cntl)
    - PL/I (.pli, .pl1, .plinc, .plc)
    - HLASM (.asm, .mac, .mlc, .cpy)
    - BMS (.bms, .map)
    
    Yields:
        Dictionary with normalized row data matching the unified schema
    """
    # Build extension filter for WHERE clause
    all_extensions = []
    for extensions in VALID_EXTENSIONS.values():
        all_extensions.extend(extensions)
    
    # Remove duplicates and create LIKE clauses
    extension_patterns = [f"f.path LIKE '%.{ext}'" for ext in set(all_extensions)]
    extension_filter = " OR ".join(extension_patterns)
    
    query = f"""
    SELECT
      f.repo_name,
      f.path,
      c.content,
      l.license
    FROM (
      SELECT f.*, ROW_NUMBER() OVER (PARTITION BY id ORDER BY path DESC) AS seqnum
      FROM `bigquery-public-data.github_repos.files` AS f
    ) f
    JOIN `bigquery-public-data.github_repos.contents` AS c
      ON f.id = c.id AND seqnum = 1
    LEFT JOIN `bigquery-public-data.github_repos.licenses` AS l
      ON f.repo_name = l.repo_name
    WHERE
      NOT c.binary
      AND ({extension_filter})
      AND c.size < 2000000
    """
    
    logger.info("Executing BigQuery query...")
    client = _get_client()
    query_job = client.query(query)
    results = query_job.result()
    
    count = 0
    skipped = 0
    skipped_extensions = 0
    
    for row in results:
        # Extract extension and infer language
        extension = _extract_extension(row.path)
        language = _infer_language_from_extension(extension)
        
        # Filter by valid extensions
        all_valid_extensions = set()
        for ext_set in VALID_EXTENSIONS.values():
            all_valid_extensions.update(ext_set)
        
        if extension not in all_valid_extensions:
            skipped_extensions += 1
            continue
        
        # Decode base64 content
        try:
            content_bytes = base64.b64decode(row.content)
            # Use latin-1 encoding with errors="ignore" to handle binary-like content
            content = content_bytes.decode("latin-1", errors="ignore")
        except Exception as e:
            logger.warning(f"Failed to decode content for {row.repo_name}/{row.path}: {e}")
            skipped += 1
            continue
        
        # Apply shared filtering logic
        num_tokens = passes_filters(content)
        if num_tokens is None:
            skipped += 1
            continue
        
        # Classify license type (permissive or no_license)
        license_name = row.license or None
        license_type = classify_license_type(license_name)
        licenses_array = [license_name] if license_name else []
        
        # Normalize to unified schema
        normalized_row = {
            "content": content,
            "repo_name": row.repo_name,
            "file_path": row.path,
            "language": language,
            "extension": extension,
            "license_type": license_type,  # "permissive" or "no_license"
            "licenses": licenses_array,  # Array of actual license names
            "host_url": "https://github.com",
            "source": "bigquery",
            "num_tokens": num_tokens,
            # BigQuery doesn't have commit/revision info, use empty values
            "revision_id": "",  # Not available in BigQuery
            "commit_date": "",  # Not available in BigQuery
            "branch": "",  # Not available in BigQuery
        }
        
        yield normalized_row
        count += 1
    
    skip_msg = f"Loaded {count} files from BigQuery (skipped {skipped} failed filters"
    if skipped_extensions > 0:
        skip_msg += f", {skipped_extensions} invalid extensions"
    skip_msg += ")"
    logger.info(skip_msg)

