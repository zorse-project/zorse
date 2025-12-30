"""Pytest configuration and shared fixtures."""

import os
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def reset_env():
    """Reset environment variables before each test."""
    # Store original env
    original_env = os.environ.copy()
    yield
    # Restore original env
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture(autouse=True)
def mock_tokenizer():
    """Mock the tokenizer to avoid HuggingFace API calls during tests."""
    # Create a mock tokenizer
    mock_tok = MagicMock()
    # Make tokenize return a list of tokens based on word count (simple approximation)
    def mock_tokenize(text):
        # Simple approximation: split by whitespace
        return text.split() if text else []
    mock_tok.tokenize = mock_tokenize
    
    # Mock AutoTokenizer.from_pretrained to return our mock
    with patch("processing.filters.AutoTokenizer.from_pretrained", return_value=mock_tok):
        # Also reset the global tokenizer cache
        import processing.filters
        processing.filters._TOKENIZER = None
        yield mock_tok
        # Reset after test
        processing.filters._TOKENIZER = None


@pytest.fixture
def mock_hf_token():
    """Mock HuggingFace token for testing."""
    with patch.dict(os.environ, {"HF_TOKEN": "test-hf-token"}):
        yield "test-hf-token"


@pytest.fixture
def mock_aws_credentials():
    """Mock AWS credentials for testing."""
    with patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": "test-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret",
            "AWS_SESSION_TOKEN": "test-session",
        }
    ):
        yield


@pytest.fixture(autouse=True)
def mock_bigquery_client():
    """Mock BigQuery client to avoid credential checks during tests."""
    with patch("ingestion.bigquery.bigquery.Client") as mock_client_class:
        # Reset the global client cache
        import ingestion.bigquery
        ingestion.bigquery._client = None
        yield mock_client_class
        # Reset after test
        ingestion.bigquery._client = None

