import argparse
import os
from typing import List

from datasets import concatenate_datasets, load_dataset
from dotenv import load_dotenv
from huggingface_hub import create_repo
from huggingface_hub.utils import HfHubHTTPError
from loguru import logger

load_dotenv()


def upload_to_hf(file_paths: List[str], dataset_name):
    """
    Uploads one or more JSONL files to Hugging Face Datasets. If the dataset repo
    already exists, it appends the new data to the existing dataset and re-pushes it.
    Otherwise, it creates a new dataset.
    
    Args:
        file_paths (List[str]): Paths to the JSONL files to upload.
        dataset_name (str): Name of the dataset on Hugging Face.
    """
    
    for file_path in file_paths:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file '{file_path}' does not exist.")
        
        if not file_path.endswith(".jsonl"):
            raise ValueError("The file must be a JSONL file with a '.jsonl' extension.")
    
    token = os.getenv("HF_TOKEN")   
    if not token:
        raise ValueError("No Hugging Face token provided. Set the HF_TOKEN environment variable or pass it explicitly.")
    
    repo_id = f"zorse/{dataset_name}"
    create_repo(repo_id=repo_id, token=token, repo_type="dataset", private=True, exist_ok=True)
    
    try:
        existing_dataset = load_dataset(repo_id, split="train", use_auth_token=token)
        logger.info("Existing dataset found. It will be merged with the new data.")
    except HfHubHTTPError:
        logger.info("No existing dataset found. A new dataset will be created.")
        existing_dataset = None

    dataset = load_dataset("json", data_files=file_paths, split="train")
    
    if existing_dataset is not None:
        combined_dataset = concatenate_datasets([existing_dataset, dataset])
    else:
        combined_dataset = dataset

    combined_dataset.push_to_hub(
        repo_id=repo_id,
        token=token
    )
    
    logger.info(f"Dataset '{dataset_name}' successfully uploaded/updated to Hugging Face Datasets at: https://huggingface.co/datasets/{repo_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload a JSONL file to Hugging Face Datasets.")
    parser.add_argument("--name", type=str, required=True, help="Name of the dataset on Hugging Face.")
    parser.add_argument("--paths", nargs="+", required=True, help="Paths to the JSONL files.")
    
    args = parser.parse_args()
    
    upload_to_hf(
        file_paths=args.paths,
        dataset_name=args.name,
    ) 