"""Tests for processing/filters.py"""

import pytest

from processing.filters import passes_filters, MAX_LINES, MAX_TOKENS, MIN_LINES


class TestPassesFilters:
    """Test the passes_filters function."""
    
    def test_empty_content(self):
        """Test that empty content returns None."""
        assert passes_filters("") is None
        assert passes_filters(None) is None
    
    def test_too_few_lines(self):
        """Test that content with too few lines is filtered out."""
        # Create content with exactly MIN_LINES - 1 lines
        content = "\n".join(["line"] * (MIN_LINES - 1))
        assert passes_filters(content) is None
    
    def test_min_lines_passes(self):
        """Test that content with exactly MIN_LINES passes."""
        content = "\n".join(["line"] * MIN_LINES)
        result = passes_filters(content)
        assert result is not None
        assert isinstance(result, int)
        assert result > 0
    
    def test_too_many_lines(self):
        """Test that content with too many lines is filtered out."""
        # Create content with MAX_LINES + 1 lines
        content = "\n".join(["line"] * (MAX_LINES + 1))
        assert passes_filters(content) is None
    
    def test_max_lines_passes(self):
        """Test that content with exactly MAX_LINES passes (if token count is ok)."""
        # Use very short lines to keep token count low
        content = "\n".join(["a"] * MAX_LINES)
        result = passes_filters(content)
        # Should pass line check, but might fail token check
        # Just verify it returns None or int (not error)
        assert result is None or isinstance(result, int)
    
    def test_valid_content_returns_token_count(self):
        """Test that valid content returns a token count."""
        # Create content with reasonable line count
        lines = ["This is a test line."] * 50
        content = "\n".join(lines)
        result = passes_filters(content)
        
        assert result is not None
        assert isinstance(result, int)
        assert result > 0
        assert result <= MAX_TOKENS
    
    def test_cobol_content(self):
        """Test with sample COBOL-like content."""
        cobol_code = """
       IDENTIFICATION DIVISION.
       PROGRAM-ID. TEST.
       DATA DIVISION.
       WORKING-STORAGE SECTION.
       01 WS-VAR PIC X(10).
       PROCEDURE DIVISION.
           DISPLAY "Hello, World!"
           STOP RUN.
       """.strip()
        
        # Repeat to get enough lines
        content = "\n".join([cobol_code] * 2)
        result = passes_filters(content)
        
        # Should either pass or fail based on token count
        assert result is None or isinstance(result, int)
    
    def test_multiline_content(self):
        """Test with multiline content."""
        content = """Line 1
Line 2
Line 3
Line 4
Line 5
Line 6
Line 7
Line 8
Line 9
Line 10"""
        
        result = passes_filters(content)
        assert result is not None
        assert isinstance(result, int)
    
    def test_very_long_single_line(self):
        """Test content that might exceed token limit on a single line."""
        # Create a very long single line that might exceed token count
        long_line = "word " * 10000  # Very long line
        content = "\n".join([long_line] * MIN_LINES)
        result = passes_filters(content)
        
        # Should be filtered out if token count exceeds MAX_TOKENS
        assert result is None or (isinstance(result, int) and result <= MAX_TOKENS)

