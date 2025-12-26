"""Tests for upload.py"""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from upload import upload_to_hf


class TestUploadToHF:
    """Test the upload_to_hf function."""
    
    def test_file_not_found(self):
        """Test that FileNotFoundError is raised for non-existent files."""
        with pytest.raises(FileNotFoundError):
            upload_to_hf(["nonexistent.jsonl"], "test-dataset")
    
    def test_invalid_file_extension(self):
        """Test that ValueError is raised for non-JSONL files."""
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            f.write(b"test")
            temp_path = f.name
        
        try:
            with pytest.raises(ValueError, match="must be a JSONL file"):
                upload_to_hf([temp_path], "test-dataset")
        finally:
            os.unlink(temp_path)
    
    @patch.dict(os.environ, {}, clear=True)
    def test_missing_hf_token(self):
        """Test that ValueError is raised when HF_TOKEN is missing."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            f.write(b'{"test": "data"}\n')
            temp_path = f.name
        
        try:
            with pytest.raises(ValueError, match="No Hugging Face token"):
                upload_to_hf([temp_path], "test-dataset")
        finally:
            os.unlink(temp_path)
    
    @patch("upload.create_repo")
    @patch("upload.load_dataset")
    @patch("upload.concatenate_datasets")
    def test_upload_new_dataset(
        self, mock_concat, mock_load_dataset, mock_create_repo
    ):
        """Test uploading to a new dataset."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", mode="w", delete=False) as f:
            json.dump({"test": "data1"}, f)
            f.write("\n")
            json.dump({"test": "data2"}, f)
            f.write("\n")
            temp_path = f.name
        
        try:
            with patch.dict(os.environ, {"HF_TOKEN": "test-token"}):
                # Mock dataset object
                mock_dataset = MagicMock()
                mock_dataset.push_to_hub = MagicMock()
                
                # Mock load_dataset to raise for existing dataset check, but succeed for json load
                def load_side_effect(*args, **kwargs):
                    repo_id = args[0] if args else kwargs.get("repo_id", "")
                    if "zorse/test-dataset" in str(repo_id):
                        # Check for existing dataset - raise to simulate not found
                        from huggingface_hub.utils import HfHubHTTPError
                        raise HfHubHTTPError("Not found", response=MagicMock(status_code=404))
                    # For json file load, return dataset
                    return mock_dataset
                
                mock_load_dataset.side_effect = load_side_effect
                
                upload_to_hf([temp_path], "test-dataset")
                
                mock_create_repo.assert_called_once()
                mock_dataset.push_to_hub.assert_called_once()
        finally:
            os.unlink(temp_path)
    
    @patch("upload.create_repo")
    @patch("upload.load_dataset")
    @patch("upload.concatenate_datasets")
    def test_upload_merge_existing(
        self, mock_concat, mock_load_dataset, mock_create_repo
    ):
        """Test uploading and merging with existing dataset."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", mode="w", delete=False) as f:
            json.dump({"test": "new_data"}, f)
            f.write("\n")
            temp_path = f.name
        
        try:
            with patch.dict(os.environ, {"HF_TOKEN": "test-token"}):
                # Mock existing dataset
                mock_existing = MagicMock()
                mock_new = MagicMock()
                mock_combined = MagicMock()
                mock_combined.push_to_hub = MagicMock()
                
                def load_side_effect(*args, **kwargs):
                    repo_id = args[0] if args else kwargs.get("repo_id", "")
                    if "zorse/test-dataset" in str(repo_id):
                        return mock_existing
                    return mock_new
                
                mock_load_dataset.side_effect = load_side_effect
                mock_concat.return_value = mock_combined
                
                upload_to_hf([temp_path], "test-dataset")
                
                mock_concat.assert_called_once()
                mock_combined.push_to_hub.assert_called_once()
        finally:
            os.unlink(temp_path)
    
    @patch("upload.create_repo")
    @patch("upload.load_dataset")
    def test_upload_multiple_files(self, mock_load_dataset, mock_create_repo):
        """Test uploading multiple JSONL files."""
        files = []
        try:
            for i in range(2):
                f = tempfile.NamedTemporaryFile(suffix=".jsonl", mode="w", delete=False)
                json.dump({"file": i, "data": f"test{i}"}, f)
                f.write("\n")
                f.close()
                files.append(f.name)
            
            with patch.dict(os.environ, {"HF_TOKEN": "test-token"}):
                mock_dataset = MagicMock()
                mock_dataset.push_to_hub = MagicMock()
                
                def load_side_effect(*args, **kwargs):
                    repo_id = args[0] if args else ""
                    if "zorse/test-dataset" in str(repo_id):
                        from huggingface_hub.utils import HfHubHTTPError
                        raise HfHubHTTPError("Not found", response=MagicMock(status_code=404))
                    return mock_dataset
                
                mock_load_dataset.side_effect = load_side_effect
                
                upload_to_hf(files, "test-dataset")
                
                # Should create repo once
                mock_create_repo.assert_called_once()
                # Should push once with all files
                mock_dataset.push_to_hub.assert_called_once()
        finally:
            for f in files:
                os.unlink(f)

