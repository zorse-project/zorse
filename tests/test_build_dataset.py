"""Tests for scripts/build_dataset.py"""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scripts.build_dataset import build_dataset


class TestBuildDataset:
    """Test the build_dataset function."""
    
    @patch("scripts.build_dataset.load_stack")
    @patch("scripts.build_dataset.load_bigquery")
    def test_build_dataset_stack_only(self, mock_load_bigquery, mock_load_stack):
        """Test building dataset from Stack v2 only."""
        # Mock stack data
        mock_stack_data = [
            {
                "content": "test content 1",
                "repo_name": "repo1",
                "file_path": "file1.cob",
                "language": "COBOL",
                "license": "MIT",
                "host_url": "https://github.com",
                "source": "stack",
                "num_tokens": 100,
            }
        ]
        mock_load_stack.return_value = iter(mock_stack_data)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.jsonl"
            build_dataset(str(output_path), languages=["COBOL"], include_bigquery=False)
            
            # Verify file was created
            assert output_path.exists()
            
            # Verify content
            with open(output_path) as f:
                lines = f.readlines()
                assert len(lines) == 1
                row = json.loads(lines[0])
                assert row["source"] == "stack"
                assert row["repo_name"] == "repo1"
            
            mock_load_stack.assert_called_once_with("COBOL")
            mock_load_bigquery.assert_not_called()
    
    @patch("scripts.build_dataset.load_stack")
    @patch("scripts.build_dataset.load_bigquery")
    def test_build_dataset_with_bigquery(self, mock_load_bigquery, mock_load_stack):
        """Test building dataset with BigQuery included."""
        # Mock stack data
        mock_stack_data = [
            {
                "content": "test content 1",
                "repo_name": "repo1",
                "file_path": "file1.cob",
                "language": "COBOL",
                "license": "MIT",
                "host_url": "https://github.com",
                "source": "stack",
                "num_tokens": 100,
            }
        ]
        mock_load_stack.return_value = iter(mock_stack_data)
        
        # Mock BigQuery data
        mock_bigquery_data = [
            {
                "content": "test content 2",
                "repo_name": "repo2",
                "file_path": "file2.jcl",
                "language": "JCL",
                "license_type": "Apache-2.0",
                "licenses": ["Apache-2.0"],
                "host_url": "https://github.com",
                "source": "bigquery",
                "num_tokens": 150,
            }
        ]
        mock_load_bigquery.return_value = iter(mock_bigquery_data)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.jsonl"
            build_dataset(
                str(output_path),
                languages=["COBOL"],
                include_bigquery=True
            )
            
            # Verify file was created
            assert output_path.exists()
            
            # Verify content
            with open(output_path) as f:
                lines = f.readlines()
                assert len(lines) == 2  # Both sources
            
            # Verify sources
            with open(output_path) as f:
                sources = [json.loads(line)["source"] for line in f]
                assert "stack" in sources
                assert "bigquery" in sources
            
            mock_load_bigquery.assert_called_once()
    
    @patch("scripts.build_dataset.load_stack")
    @patch("scripts.build_dataset.load_bigquery")
    def test_build_dataset_multiple_languages(self, mock_load_bigquery, mock_load_stack):
        """Test building dataset with multiple languages."""
        # Mock data for each language
        def mock_stack_generator(lang):
            return iter([{
                "content": f"test content {lang}",
                "repo_name": f"repo-{lang}",
                "file_path": f"file.{lang.lower()}",
                "language": lang,
                "license_type": "MIT",
                "licenses": ["MIT"],
                "host_url": "https://github.com",
                "source": "stack",
                "num_tokens": 100,
            }])
        
        mock_load_stack.side_effect = lambda lang: mock_stack_generator(lang)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.jsonl"
            build_dataset(
                str(output_path),
                languages=["COBOL", "REXX", "RPGLE"],
                include_bigquery=False
            )
            
            # Verify file was created
            assert output_path.exists()
            
            # Verify content
            with open(output_path) as f:
                lines = f.readlines()
                assert len(lines) == 3  # One per language
            
            # Verify all languages are present
            with open(output_path) as f:
                languages = [json.loads(line)["language"] for line in f]
                assert "COBOL" in languages
                assert "REXX" in languages
                assert "RPGLE" in languages
            
            assert mock_load_stack.call_count == 3
    
    @patch("scripts.build_dataset.load_stack")
    @patch("scripts.build_dataset.load_bigquery")
    def test_build_dataset_empty_results(self, mock_load_bigquery, mock_load_stack):
        """Test building dataset with no results."""
        mock_load_stack.return_value = iter([])
        mock_load_bigquery.return_value = iter([])
        
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.jsonl"
            build_dataset(
                str(output_path),
                languages=["COBOL"],
                include_bigquery=False
            )
            
            # File should still be created, just empty
            assert output_path.exists()
            
            with open(output_path) as f:
                lines = f.readlines()
                assert len(lines) == 0

