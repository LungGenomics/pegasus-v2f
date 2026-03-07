"""Tests for AI assistance — mocked subprocess."""

import json
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from pegasus_v2f.ai_assist import (
    AIColumnSuggestion,
    AISuggestion,
    ClaudeCLIProvider,
    get_provider,
    _build_column_prompt,
    _build_prompt,
    _parse_column_response,
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
    "transformations": [
        {"type": "strip_prefix", "column": "chr", "prefix": "chr"},
    ],
    "confidence": 0.92,
})


class TestParseResponse:
    def test_clean_json(self):
        result = _parse_response(GOOD_RESPONSE)
        assert result is not None
        assert result.category == "COLOC"
        assert result.confidence == 0.92
        assert result.centric == "variant"
        assert len(result.transformations) == 1
        assert result.transformations[0]["type"] == "strip_prefix"

    def test_no_transformations_field(self):
        """Backwards-compatible: missing transformations defaults to empty list."""
        resp = json.dumps({"category": "EXP", "reasoning": "test", "confidence": 0.8})
        result = _parse_response(resp)
        assert result is not None
        assert result.transformations == []

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

    def test_includes_transform_vocabulary(self, sample_inspection):
        prompt = _build_prompt(sample_inspection)
        assert "Available Transformations" in prompt
        assert "strip_prefix" in prompt
        assert "coerce_numeric" in prompt
        assert "drop_nulls" in prompt
        assert "uppercase" in prompt

    def test_includes_heuristic_fixes(self, sample_inspection):
        from pegasus_v2f.inspect import SuggestedFix
        fixes = [SuggestedFix(
            "strip_chr_prefix",
            "Chr column has 'chr' prefix",
            {"type": "strip_prefix", "column": "chr", "prefix": "chr"},
        )]
        prompt = _build_prompt(sample_inspection, heuristic_fixes=fixes)
        assert "Heuristic Suggestions" in prompt
        assert "strip_chr_prefix" in prompt

    def test_no_heuristic_section_without_fixes(self, sample_inspection):
        prompt = _build_prompt(sample_inspection)
        assert "Heuristic Suggestions" not in prompt

    def test_empty_heuristic_fixes(self, sample_inspection):
        prompt = _build_prompt(sample_inspection, heuristic_fixes=[])
        assert "Heuristic Suggestions" not in prompt


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


class TestColumnDetection:
    def test_build_column_prompt_sentinel(self):
        cols = [
            {"name": "CHROM_NUM", "dtype": "text", "sample_values": ["1", "2", "3"]},
            {"name": "BP", "dtype": "numeric", "sample_values": ["1000000", "2000000"]},
        ]
        prompt = _build_column_prompt(cols, context="sentinel")
        assert "CHROM_NUM" in prompt
        assert "chromosome" in prompt
        assert "position" in prompt

    def test_build_column_prompt_source(self):
        cols = [{"name": "GENE_SYM", "dtype": "text", "sample_values": ["AGER"]}]
        prompt = _build_column_prompt(cols, context="source")
        assert "gene" in prompt
        assert "evidence" in prompt.lower()

    def test_parse_column_response_clean_json(self):
        text = '{"chromosome": "CHROM_NUM", "position": "BP"}'
        result = _parse_column_response(text)
        assert result is not None
        assert result.mappings["chromosome"] == "CHROM_NUM"
        assert result.mappings["position"] == "BP"

    def test_parse_column_response_markdown(self):
        text = '```json\n{"chromosome": "CHR", "gene": "NEAREST"}\n```'
        result = _parse_column_response(text)
        assert result is not None
        assert result.mappings["chromosome"] == "CHR"

    def test_parse_column_response_malformed(self):
        result = _parse_column_response("I don't understand")
        assert result is None

    def test_parse_column_response_empty_dict(self):
        result = _parse_column_response("{}")
        assert result is not None
        assert result.mappings == {}

    def test_suggest_columns_with_mock(self, sample_inspection):
        provider = ClaudeCLIProvider()
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = '{"chromosome": "chr", "position": "pos", "gene": "gene"}'

        with patch("subprocess.run", return_value=mock_result):
            cols = [{"name": "chr", "sample_values": ["1"]}, {"name": "pos", "sample_values": ["1000"]}]
            suggestion = provider.suggest_columns(cols, context="sentinel")
            assert suggestion is not None
            assert suggestion.mappings["chromosome"] == "chr"

    def test_suggest_columns_timeout(self):
        import subprocess
        provider = ClaudeCLIProvider()
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("claude", 30)):
            result = provider.suggest_columns([{"name": "x"}])
            assert result is None


class TestAISuggestionSerialization:
    def test_to_dict_includes_transformations(self):
        suggestion = AISuggestion(
            category="COLOC",
            category_reasoning="test",
            column_mappings={"gene": "gene"},
            centric="variant",
            quality_notes=["note"],
            transformations=[{"type": "strip_prefix", "column": "chr", "prefix": "chr"}],
            confidence=0.9,
        )
        d = suggestion.to_dict()
        assert "transformations" in d
        assert len(d["transformations"]) == 1
        assert d["transformations"][0]["type"] == "strip_prefix"
        assert "normalization_suggestions" not in d

    def test_suggest_passes_heuristic_fixes(self, sample_inspection):
        """Verify heuristic_fixes are passed through to _build_prompt."""
        from pegasus_v2f.inspect import SuggestedFix

        fixes = [SuggestedFix("test_fix", "Test message", {"type": "uppercase", "column": "gene"})]
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = GOOD_RESPONSE

        with patch("subprocess.run", return_value=mock_result) as mock_run:
            provider = ClaudeCLIProvider()
            provider.suggest(sample_inspection, heuristic_fixes=fixes)
            # Verify the prompt contains heuristic fix context
            prompt_arg = mock_run.call_args[0][0][3]  # ["claude", "--print", "-p", prompt]
            assert "Heuristic Suggestions" in prompt_arg
            assert "test_fix" in prompt_arg
