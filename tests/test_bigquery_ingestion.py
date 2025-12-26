"""Tests for ingestion/bigquery.py"""

import base64
from unittest.mock import MagicMock, patch

import pytest

from ingestion.bigquery import load_bigquery


class TestLoadBigQuery:
    """Test the load_bigquery function."""
    
    @patch("ingestion.bigquery._get_client")
    @patch("ingestion.bigquery.passes_filters")
    def test_load_bigquery_success(self, mock_passes_filters, mock_get_client):
        """Test successful BigQuery loading."""
        # Mock BigQuery result row
        mock_row = MagicMock()
        mock_row.repo_name = "test/repo"
        mock_row.path = "test/file.jcl"
        mock_row.content = base64.b64encode(("test content\n" * 20).encode("utf-8")).decode("utf-8")
        mock_row.license = "MIT"
        
        # Mock query result
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([mock_row]))
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = mock_result
        mock_client = MagicMock()
        mock_client.query.return_value = mock_query_job
        mock_get_client.return_value = mock_client
        
        # Mock filter passing
        mock_passes_filters.return_value = 150  # Token count
        
        results = list(load_bigquery())
        
        assert len(results) == 1
        result = results[0]
        assert result["repo_name"] == "test/repo"
        assert result["file_path"] == "test/file.jcl"
        assert result["language"] == "JCL"  # Language inferred from .jcl extension
        assert result["extension"] == "jcl"
        assert result["license_type"] == "permissive"  # MIT is a permissive license
        assert result["licenses"] == ["MIT"]
        assert result["source"] == "bigquery"
        assert result["num_tokens"] == 150
        assert result["host_url"] == "https://github.com"
        assert "content" in result
    
    @patch("ingestion.bigquery._get_client")
    @patch("ingestion.bigquery.passes_filters")
    def test_load_bigquery_filters_out_invalid_content(self, mock_passes_filters, mock_get_client):
        """Test that content failing filters is excluded."""
        mock_row = MagicMock()
        mock_row.repo_name = "test/repo"
        mock_row.path = "test/file.jcl"
        mock_row.content = base64.b64encode("test content".encode("utf-8")).decode("utf-8")
        mock_row.license = "MIT"
        
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([mock_row]))
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = mock_result
        mock_client = MagicMock()
        mock_client.query.return_value = mock_query_job
        mock_get_client.return_value = mock_client
        
        # Mock filter failing
        mock_passes_filters.return_value = None
        
        results = list(load_bigquery())
        
        # Should be empty since content failed filters
        assert len(results) == 0
    
    @patch("ingestion.bigquery._get_client")
    @patch("ingestion.bigquery.passes_filters")
    def test_load_bigquery_handles_decode_error(self, mock_passes_filters, mock_get_client):
        """Test that decode errors are handled gracefully."""
        import base64
        mock_row = MagicMock()
        mock_row.repo_name = "test/repo"
        mock_row.path = "test/file.jcl"
        # Use base64 that will fail to decode properly
        mock_row.content = "!!!invalid base64!!!"  # Invalid base64 - will decode but may fail filters
        mock_row.license = "MIT"
        
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([mock_row]))
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = mock_result
        mock_client = MagicMock()
        mock_client.query.return_value = mock_query_job
        mock_get_client.return_value = mock_client
        
        # Mock filter to fail (content decoded but likely doesn't pass filters)
        mock_passes_filters.return_value = None
        
        results = list(load_bigquery())
        
        # Should skip content that fails to decode
        assert len(results) == 0
        # Verify passes_filters was not called because decode failed
        mock_passes_filters.assert_not_called()
    
    @patch("ingestion.bigquery._get_client")
    @patch("ingestion.bigquery.passes_filters")
    def test_load_bigquery_schema(self, mock_passes_filters, mock_get_client):
        """Test that load_bigquery yields rows with correct schema."""
        mock_row = MagicMock()
        mock_row.repo_name = "test/repo"
        mock_row.path = "test/file.jcl"
        mock_row.content = base64.b64encode(("test content\n" * 20).encode("utf-8")).decode("utf-8")
        mock_row.license = "MIT"
        
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([mock_row]))
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = mock_result
        mock_client = MagicMock()
        mock_client.query.return_value = mock_query_job
        mock_get_client.return_value = mock_client
        
        mock_passes_filters.return_value = 200
        
        results = list(load_bigquery())
        
        assert len(results) == 1
        result = results[0]
        
        # Check required schema fields (matching HuggingFace dataset schema)
        required_fields = [
            "content", "repo_name", "file_path", "language", "extension",
            "license_type", "licenses", "host_url", "source",
            "num_tokens", "revision_id", "commit_date", "branch"
        ]
        for field in required_fields:
            assert field in result, f"Missing required field: {field}"
        
        assert result["source"] == "bigquery"
        assert result["language"] == "JCL"  # Language inferred from .jcl extension
        assert result["extension"] == "jcl"
        assert result["host_url"] == "https://github.com"
        # BigQuery doesn't have commit info
        assert result["revision_id"] == ""
        assert result["commit_date"] == ""
        assert result["branch"] == ""
    
    @patch("ingestion.bigquery._get_client")
    @patch("ingestion.bigquery.passes_filters")
    def test_load_bigquery_handles_missing_license(self, mock_passes_filters, mock_get_client):
        """Test that missing license defaults to 'unknown'."""
        mock_row = MagicMock()
        mock_row.repo_name = "test/repo"
        mock_row.path = "test/file.jcl"
        mock_row.content = base64.b64encode(("test content\n" * 20).encode("utf-8")).decode("utf-8")
        mock_row.license = None  # No license
        
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([mock_row]))
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = mock_result
        mock_client = MagicMock()
        mock_client.query.return_value = mock_query_job
        mock_get_client.return_value = mock_client
        
        mock_passes_filters.return_value = 150
        
        results = list(load_bigquery())
        
        assert len(results) == 1
        assert results[0]["license_type"] == "no_license"  # No license provided
        assert results[0]["licenses"] == []
    
    @patch("ingestion.bigquery._get_client")
    @patch("ingestion.bigquery.passes_filters")
    def test_load_bigquery_latin1_encoding(self, mock_passes_filters, mock_get_client):
        """Test that content is decoded using latin-1 encoding."""
        # Create content that might have issues with utf-8
        content = "test content with some bytes: \x80\x81\x82\n" * 20
        encoded = base64.b64encode(content.encode("latin-1")).decode("utf-8")
        
        mock_row = MagicMock()
        mock_row.repo_name = "test/repo"
        mock_row.path = "test/file.jcl"
        mock_row.content = encoded
        mock_row.license = "MIT"
        
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([mock_row]))
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = mock_result
        mock_client = MagicMock()
        mock_client.query.return_value = mock_query_job
        mock_get_client.return_value = mock_client
        
        mock_passes_filters.return_value = 150
        
        results = list(load_bigquery())
        
        assert len(results) == 1
        # Content should be decoded successfully
        assert "test content" in results[0]["content"]

