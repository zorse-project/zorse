import argparse
import json
import os
from typing import Optional

import boto3
from datasets import load_dataset
from dotenv import load_dotenv
from loguru import logger
from smart_open import open as sopen
from transformers import AutoTokenizer

load_dotenv()

MIN_LINES = 10 # minimum number of lines in a file
MAX_LINES = 10000 # maximum number of lines in a file 
MAX_TOKENS = 128000 # maximum number of tokens in a file

LANGUAGES = ["COBOL", "REXX", "RPGLE"]

TOKENIZER = AutoTokenizer.from_pretrained("meta-llama/Llama-3.2-3B")


session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
)
s3 = session.client("s3")

def download_blob(id: str, encoding: str) -> Optional[str]:
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


def load_split(language: str):
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
            "file_path": row["path"], # path within the repo
            "language": row["language"],
            "extension": row["extension"],
            "branch": row["branch_name"],
            "revision_id": row["revision_id"], # SWH revision (commit) id
            "commit_date": row["committer_date"].isoformat(),
        },
        remove_columns=dataset.column_names
    ).filter(
        lambda row: row["content"] is not None and MIN_LINES <= len(row["content"].splitlines()) <= MAX_LINES
    )
    
    tokenised_dataset = []
    for row in dataset:
        num_tokens = len(TOKENIZER.tokenize(row["content"]))
        if num_tokens <= MAX_TOKENS:
            row["num_tokens"] = num_tokens # approximate token count with Llama 3.2 tokenizer
            tokenised_dataset.append(row)
    
    logger.info(f"Loaded {len(tokenised_dataset)} files for {language}")
    return tokenised_dataset


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--languages", nargs="+", default="REXX")
    args = parser.parse_args()

    for language in args.languages:
        dataset = load_split(language)
        with open(f"data/{language}_stack_v2.jsonl", "w") as fout:
            for row in dataset:
                fout.write(json.dumps(row) + "\n")

    logger.info("Done!")