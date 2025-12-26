"""Tests for ingestion/stack.py"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from ingestion.stack import download_blob, load_stack


class TestDownloadBlob:
    """Test the download_blob function."""
    
    @patch("ingestion.stack.sopen")
    def test_download_blob_success(self, mock_sopen):
        """Test successful blob download."""
        mock_file = MagicMock()
        mock_file.read.return_value = b"test content"
        mock_sopen.return_value.__enter__.return_value = mock_file
        
        result = download_blob("test-id", "utf-8")
        
        assert result == "test content"
        mock_sopen.assert_called_once()
    
    @patch("ingestion.stack.sopen")
    def test_download_blob_failure(self, mock_sopen):
        """Test blob download failure."""
        mock_sopen.side_effect = Exception("Download failed")
        
        result = download_blob("test-id", "utf-8")
        
        assert result is None
    
    @patch("ingestion.stack.sopen")
    def test_download_blob_different_encoding(self, mock_sopen):
        """Test blob download with different encoding."""
        mock_file = MagicMock()
        mock_file.read.return_value = b"test content"
        mock_sopen.return_value.__enter__.return_value = mock_file
        
        result = download_blob("test-id", "latin-1")
        
        assert result == "test content"


class TestLoadStack:
    """Test the load_stack function."""
    
    @patch("ingestion.stack.load_dataset")
    @patch("ingestion.stack.download_blob")
    @patch("ingestion.stack.passes_filters")
    def test_load_stack_success(self, mock_passes_filters, mock_download, mock_load_dataset):
        """Test successful stack loading."""
        # Mock dataset
        mock_dataset = MagicMock()
        
        # Create mapped row (after dataset.map() transformation)
        mock_row = {
            "content": "\n".join(["line"] * 20),  # Content from download_blob
            "licenses": ["MIT"],
            "license_type": "MIT",
            "host_url": "https://github.com",
            "repo_name": "test/repo",
            "file_path": "test/file.cob",
            "language": "COBOL",
            "extension": ".cob",
            "branch": "main",
            "revision_id": "abc123",
            "commit_date": datetime(2024, 1, 1).isoformat(),
        }
        
        # Mock the map() to return an iterable dataset
        mapped_dataset = MagicMock()
        mapped_dataset.__iter__ = lambda self: iter([mock_row])
        mock_dataset.map.return_value = mapped_dataset
        mock_load_dataset.return_value = mock_dataset
        
        # Mock blob download
        mock_download.return_value = "\n".join(["line"] * 20)
        
        # Mock filter passing
        mock_passes_filters.return_value = 100  # Token count
        
        results = list(load_stack("COBOL"))
        
        assert len(results) == 1
        result = results[0]
        assert result["repo_name"] == "test/repo"
        assert result["file_path"] == "test/file.cob"
        assert result["language"] == "COBOL"
        assert result["license_type"] == "MIT"
        assert result["source"] == "stack"
        assert result["num_tokens"] == 100
        assert result["host_url"] == "https://github.com"
        assert "revision_id" in result
        assert "commit_date" in result
    
    @patch("ingestion.stack.load_dataset")
    @patch("ingestion.stack.download_blob")
    @patch("ingestion.stack.passes_filters")
    def test_load_stack_filters_out_none_content(self, mock_passes_filters, mock_download, mock_load_dataset):
        """Test that None content is filtered out."""
        mock_dataset = MagicMock()
        # Row with None content (as returned by map())
        mock_row = {
            "content": None,
            "repo_name": "test/repo",
            "file_path": "test/file.cob",
        }
        
        mapped_dataset = MagicMock()
        mapped_dataset.__iter__ = lambda self: iter([mock_row])
        mock_dataset.map.return_value = mapped_dataset
        mock_load_dataset.return_value = mock_dataset
        
        results = list(load_stack("COBOL"))
        
        # Should be empty since content is None
        assert len(results) == 0
        mock_passes_filters.assert_not_called()
    
    @patch("ingestion.stack.load_dataset")
    @patch("ingestion.stack.download_blob")
    @patch("ingestion.stack.passes_filters")
    def test_load_stack_filters_out_invalid_content(self, mock_passes_filters, mock_download, mock_load_dataset):
        """Test that content failing filters is excluded."""
        mock_dataset = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__.side_effect = lambda key: {
            "blob_id": "test-blob-id",
            "src_encoding": "utf-8",
            "detected_licenses": ["MIT"],
            "license_type": "MIT",
            "repo_name": "test/repo",
            "path": "test/file.cob",
            "language": "COBOL",
            "extension": ".cob",
            "branch_name": "main",
            "revision_id": "abc123",
            "committer_date": datetime(2024, 1, 1),
        }.get(key)
        
        mock_dataset.map.return_value = [mock_row]
        mock_load_dataset.return_value = mock_dataset
        
        # Mock blob download returning content
        mock_download.return_value = "some content"
        
        # Mock filter failing
        mock_passes_filters.return_value = None
        
        results = list(load_stack("COBOL"))
        
        # Should be empty since content failed filters
        assert len(results) == 0
    
    @patch("ingestion.stack.load_dataset")
    @patch("ingestion.stack.download_blob")
    @patch("ingestion.stack.passes_filters")
    def test_load_stack_schema(self, mock_passes_filters, mock_download, mock_load_dataset):
        """Test that load_stack yields rows with correct schema."""
        mock_dataset = MagicMock()
        mock_row = {
            "content": "\n".join(["line"] * 20),
            "licenses": ["MIT"],
            "license_type": "MIT",
            "host_url": "https://github.com",
            "repo_name": "test/repo",
            "file_path": "test/file.cob",
            "language": "COBOL",
            "extension": ".cob",
            "branch": "main",
            "revision_id": "abc123",
            "commit_date": datetime(2024, 1, 1).isoformat(),
        }
        
        mapped_dataset = MagicMock()
        mapped_dataset.__iter__ = lambda self: iter([mock_row])
        mock_dataset.map.return_value = mapped_dataset
        mock_load_dataset.return_value = mock_dataset
        mock_download.return_value = "\n".join(["line"] * 20)
        mock_passes_filters.return_value = 100
        
        results = list(load_stack("COBOL"))
        
        assert len(results) == 1
        result = results[0]
        
        # Check required schema fields (matching HuggingFace dataset schema)
        required_fields = [
            "content", "repo_name", "file_path", "language", "extension",
            "license_type", "licenses", "host_url", "source", "num_tokens",
            "revision_id", "commit_date", "branch"
        ]
        for field in required_fields:
            assert field in result, f"Missing required field: {field}"
        
        assert result["source"] == "stack"
        assert result["host_url"] == "https://github.com"

