"""Tests for AI assistance — mocked subprocess."""

import json
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from pegasus_v2f.ai_assist import (
    AISuggestion,
    ClaudeCLIProvider,
    get_provider,
    _build_prompt,
    _parse_response,
)
from pegasus_v2f.inspect import inspect_dataframe


@pytest.fixture
def sample_inspection():
    df = pd.DataFrame({
        "gene": ["AGER", "SFTPC", "MUC5B"],
        "chr": ["chr6", "chr8", "chr11"],
        "pos": [32151443, 22018712, 1219991],
        "PP_H4_abf": [0.95, 0.87, 0.42],
    })
    return inspect_dataframe(df, source_name="coloc_test")


GOOD_RESPONSE = json.dumps({
    "category": "COLOC",
    "reasoning": "PP.H4.abf contains posterior probabilities of colocalization.",
    "mappings": {"gene": "gene", "chromosome": "chr", "position": "pos", "score": "PP_H4_abf"},
    "centric": "variant",
    "quality_notes": ["Strip chr prefix for Ensembl compatibility"],
    "normalization_suggestions": ["Strip 'chr' prefix from chromosome column"],
    "confidence": 0.92,
})


class TestParseResponse:
    def test_clean_json(self):
        result = _parse_response(GOOD_RESPONSE)
        assert result is not None
        assert result.category == "COLOC"
        assert result.confidence == 0.92
        assert result.centric == "variant"

    def test_json_in_markdown(self):
        text = f"Here's my analysis:\n```json\n{GOOD_RESPONSE}\n```\nDone."
        result = _parse_response(text)
        assert result is not None
        assert result.category == "COLOC"

    def test_json_with_surrounding_text(self):
        text = f"I think this is COLOC.\n{GOOD_RESPONSE}\nHope that helps!"
        result = _parse_response(text)
        assert result is not None
        assert result.category == "COLOC"

    def test_malformed_json(self):
        result = _parse_response("This is not JSON at all")
        assert result is None

    def test_empty_response(self):
        result = _parse_response("")
        assert result is None


class TestBuildPrompt:
    def test_includes_inspection_data(self, sample_inspection):
        prompt = _build_prompt(sample_inspection)
        assert "coloc_test" in prompt
        assert "gene" in prompt
        assert "PEGASUS" in prompt

    def test_includes_categories(self, sample_inspection):
        prompt = _build_prompt(sample_inspection)
        assert "COLOC" in prompt
        assert "QTL" in prompt
        assert "GWAS" in prompt

    def test_includes_instructions(self, sample_inspection):
        prompt = _build_prompt(sample_inspection)
        assert "JSON" in prompt
        assert "category" in prompt


class TestClaudeCLIProvider:
    def test_is_available_when_on_path(self):
        with patch("shutil.which", return_value="/usr/local/bin/claude"):
            provider = ClaudeCLIProvider()
            assert provider.is_available()

    def test_not_available_when_missing(self):
        with patch("shutil.which", return_value=None):
            provider = ClaudeCLIProvider()
            assert not provider.is_available()

    def test_suggest_success(self, sample_inspection):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = GOOD_RESPONSE
        mock_result.stderr = ""

        with patch("subprocess.run", return_value=mock_result):
            provider = ClaudeCLIProvider()
            suggestion = provider.suggest(sample_inspection)
            assert suggestion is not None
            assert suggestion.category == "COLOC"

    def test_suggest_timeout(self, sample_inspection):
        import subprocess
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("claude", 60)):
            provider = ClaudeCLIProvider()
            suggestion = provider.suggest(sample_inspection)
            assert suggestion is None

    def test_suggest_nonzero_exit(self, sample_inspection):
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "Error"

        with patch("subprocess.run", return_value=mock_result):
            provider = ClaudeCLIProvider()
            suggestion = provider.suggest(sample_inspection)
            assert suggestion is None

    def test_provider_name(self):
        provider = ClaudeCLIProvider()
        assert provider.name == "claude"


class TestGetProvider:
    def test_auto_finds_claude(self):
        with patch("shutil.which", return_value="/usr/local/bin/claude"):
            provider = get_provider("auto")
            assert provider is not None
            assert provider.name == "claude"

    def test_auto_returns_none_without_claude(self):
        with patch("shutil.which", return_value=None):
            provider = get_provider("auto")
            assert provider is None

    def test_unknown_provider(self):
        provider = get_provider("unknown")
        assert provider is None
