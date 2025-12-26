"""Script to combine multiple ingestion sources into a single JSONL file."""

import argparse
import json

from loguru import logger

from ingestion.bigquery import load_bigquery
from ingestion.stack import load_stack


def build_dataset(output_path: str, languages: list[str] = None, include_bigquery: bool = False):
    """
    Combine multiple ingestion sources into a single JSONL file.
    
    Args:
        output_path: Path to write the output JSONL file
        languages: List of languages to ingest from Stack v2 (default: ["COBOL", "REXX", "RPGLE"])
        include_bigquery: Whether to include BigQuery ingestion
    """
    if languages is None:
        languages = ["COBOL", "REXX", "RPGLE"]
    
    all_rows = []
    
    # Load from Stack v2
    for language in languages:
        logger.info(f"Loading {language} from Stack v2...")
        rows = list(load_stack(language))
        all_rows.extend(rows)
        logger.info(f"Added {len(rows)} {language} files")
    
    # Load from BigQuery if requested
    if include_bigquery:
        logger.info("Loading data from BigQuery...")
        rows = list(load_bigquery())
        all_rows.extend(rows)
        logger.info(f"Added {len(rows)} BigQuery files")
    
    # Write to JSONL
    logger.info(f"Writing {len(all_rows)} total rows to {output_path}...")
    with open(output_path, "w") as fout:
        for row in all_rows:
            fout.write(json.dumps(row) + "\n")
    
    logger.info(f"Dataset built successfully: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build dataset by combining multiple ingestion sources."
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output JSONL file path"
    )
    parser.add_argument(
        "--languages",
        nargs="+",
        default=["COBOL", "REXX", "RPGLE"],
        help="Languages to ingest from Stack v2"
    )
    parser.add_argument(
        "--include-bigquery",
        action="store_true",
        help="Include BigQuery ingestion"
    )
    
    args = parser.parse_args()
    
    build_dataset(
        output_path=args.output,
        languages=args.languages,
        include_bigquery=args.include_bigquery,
    )

