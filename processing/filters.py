"""Shared filtering logic for content validation and token counting."""

from typing import Optional

from transformers import AutoTokenizer

# Filter constants
MIN_LINES = 10  # minimum number of lines in a file
MAX_LINES = 10000  # maximum number of lines in a file
MAX_TOKENS = 128000  # maximum number of tokens in a file

# Lazy-load tokenizer to avoid import-time network requests
_TOKENIZER: Optional[AutoTokenizer] = None


def _get_tokenizer() -> AutoTokenizer:
    """Get or initialize the tokenizer (lazy loading)."""
    global _TOKENIZER
    if _TOKENIZER is None:
        _TOKENIZER = AutoTokenizer.from_pretrained("meta-llama/Llama-3.2-3B")
    return _TOKENIZER


def passes_filters(content: str) -> int | None:
    """
    Returns token count if content passes filters, otherwise returns None.
    
    Args:
        content: The file content to check
        
    Returns:
        Token count (int) if content passes all filters, None otherwise
    """
    if not content:
        return None
    
    # Check line count
    lines = content.splitlines()
    num_lines = len(lines)
    
    if not (MIN_LINES <= num_lines <= MAX_LINES):
        return None
    
    # Check token count
    tokenizer = _get_tokenizer()
    num_tokens = len(tokenizer.tokenize(content))
    
    if num_tokens > MAX_TOKENS:
        return None
    
    return num_tokens

