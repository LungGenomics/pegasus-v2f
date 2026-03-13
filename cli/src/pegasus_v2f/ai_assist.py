"""Pluggable AI assistance for source inspection and mapping suggestions."""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pegasus_v2f.pegasus_schema import EVIDENCE_CATEGORY_PROFILES

if TYPE_CHECKING:
    from pegasus_v2f.inspect import InspectionResult

logger = logging.getLogger(__name__)


@dataclass
class AISuggestion:
    category: str | None
    category_reasoning: str
    column_mappings: dict[str, str]  # pegasus_field -> column_name
    centric: str  # "gene" or "variant"
    quality_notes: list[str]
    confidence: float  # 0.0 - 1.0
    transformations: list[dict] = field(default_factory=list)
    raw_response: str = ""

    def to_dict(self) -> dict:
        return {
            "category": self.category,
            "category_reasoning": self.category_reasoning,
            "column_mappings": self.column_mappings,
            "centric": self.centric,
            "quality_notes": self.quality_notes,
            "transformations": self.transformations,
            "confidence": self.confidence,
        }


@dataclass
class AIColumnSuggestion:
    """AI-suggested column role mappings."""
    mappings: dict[str, str]  # role -> column_name (e.g. "chromosome" -> "CHROM_NUM")
    reasoning: str = ""
    raw_response: str = ""

    def to_dict(self) -> dict:
        return {"mappings": self.mappings, "reasoning": self.reasoning}


class AIProvider(ABC):
    """Abstract base for AI suggestion providers."""

    @abstractmethod
    def suggest(self, inspection: InspectionResult, *, heuristic_fixes: list | None = None, timeout: int = 60) -> AISuggestion | None:
        ...

    def suggest_columns(
        self, columns: list[dict], context: str = "sentinel", timeout: int = 30,
    ) -> AIColumnSuggestion | None:
        """Suggest column role mappings from column names and sample values.

        This is a lightweight call for when heuristic detection fails.
        """
        prompt = _build_column_prompt(columns, context)
        try:
            result = subprocess.run(
                ["claude", "--print", "-p", prompt],
                capture_output=True, text=True, timeout=timeout,
            )
            if result.returncode != 0:
                return None
            return _parse_column_response(result.stdout)
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return None

    @abstractmethod
    def is_available(self) -> bool:
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        ...


class ClaudeCLIProvider(AIProvider):
    """Uses `claude --print` subprocess."""

    @property
    def name(self) -> str:
        return "claude"

    def is_available(self) -> bool:
        return shutil.which("claude") is not None

    def suggest(self, inspection: InspectionResult, *, heuristic_fixes: list | None = None, timeout: int = 60) -> AISuggestion | None:
        prompt = _build_prompt(inspection, heuristic_fixes=heuristic_fixes)
        try:
            result = subprocess.run(
                ["claude", "--print", "-p", prompt],
                capture_output=True, text=True, timeout=timeout,
            )
            if result.returncode != 0:
                logger.warning(f"claude CLI returned {result.returncode}: {result.stderr[:200]}")
                return None
            return _parse_response(result.stdout)
        except subprocess.TimeoutExpired:
            logger.warning(f"claude CLI timed out after {timeout}s")
            return None
        except FileNotFoundError:
            logger.warning("claude CLI not found on PATH")
            return None


def get_provider(name: str = "auto") -> AIProvider | None:
    """Get AI provider by name. 'auto' tries claude CLI first."""
    if name in ("auto", "claude"):
        provider = ClaudeCLIProvider()
        if provider.is_available():
            return provider
        if name == "claude":
            logger.warning("claude CLI not found on PATH")
    return None


def _build_prompt(inspection, *, heuristic_fixes: list | None = None) -> str:
    """Build structured prompt with inspection data and PEGASUS context.

    Accepts InspectionResult or StudyInspectionResult.
    If heuristic_fixes is provided, includes them as context for the AI.
    """
    # Compact inspection data — handle both result types
    source_name = getattr(inspection, "source_name", None) or getattr(inspection, "source_label", "")
    inspection_data = {
        "source_name": source_name,
        "row_count": inspection.row_count,
        "columns": [c.to_dict() for c in inspection.columns],
    }
    if inspection.gene_analysis:
        inspection_data["gene_analysis"] = inspection.gene_analysis.to_dict()
    if inspection.chromosome_analysis:
        inspection_data["chromosome_analysis"] = inspection.chromosome_analysis.to_dict()
    suggested_mappings = getattr(inspection, "suggested_mappings", None)
    if suggested_mappings:
        inspection_data["suggested_mappings"] = suggested_mappings

    # Category descriptions for context
    categories = {}
    for abbrev, profile in EVIDENCE_CATEGORY_PROFILES.items():
        categories[abbrev] = {
            "label": profile.label,
            "description": profile.description,
            "centric_group": profile.centric_group,
            "typical_value": profile.typical_value,
        }

    # Heuristic fixes section
    heuristic_section = ""
    if heuristic_fixes:
        fixes_data = [
            {"code": f.code, "message": f.message, "transformation": f.transformation}
            for f in heuristic_fixes
        ]
        heuristic_section = f"""
## Heuristic Suggestions
The automated inspector flagged these issues. You may accept, modify, or reject them. You may also add transformations they missed.
```json
{json.dumps(fixes_data, indent=2)}
```
"""

    return f"""You are a PEGASUS evidence categorization assistant. Analyze this data source inspection and suggest the best PEGASUS evidence category, column mappings, and data transformations.

## Inspection Data
```json
{json.dumps(inspection_data, indent=2)}
```

## PEGASUS Evidence Categories
```json
{json.dumps(categories, indent=2)}
```

## Scoring Context
PEGASUS scores genes by counting distinct categories with evidence. More categories = higher score. Categories are the unit of scoring.
{heuristic_section}
## Available Transformations
You may suggest data transformations from this list. Each is a JSON dict with "type" and type-specific fields.

- strip_prefix: {{"type": "strip_prefix", "column": "<col>", "prefix": "<prefix>"}} — Remove a prefix from all values in a column.
- uppercase: {{"type": "uppercase", "column": "<col>"}} — Uppercase all values (e.g. gene symbol normalization).
- drop_nulls: {{"type": "drop_nulls", "column": "<col>"}} — Drop rows where column is null/empty/NaN.
- coerce_numeric: {{"type": "coerce_numeric", "column": "<col>"}} — Convert to numeric, non-numeric becomes NaN.
- rename: {{"type": "rename", "columns": {{"old_name": "new_name"}}}} — Rename columns.
- filter_values: {{"type": "filter_values", "column": "<col>", "pattern": "<regex>"}} — Keep rows matching regex.
- deduplicate: {{"type": "deduplicate", "column": "<col>"}} — Remove duplicate rows (keeps first).
- select: {{"type": "select", "columns": ["col1", "col2"]}} — Select specific columns (drop others).
- parse_variant_id: {{"type": "parse_variant_id", "column": "<col>"}} — Parse a variant ID column (e.g. "chr1:16979534C:A", "10:103897116:G:A") into separate "chr" and "pos" columns. Use when data has a combined variant identifier but no separate chromosome/position columns.
- split_column: {{"type": "split_column", "column": "<col>", "delimiter": "<delim>", "index": N, "output": "<out_col>"}} — Split a column by delimiter and keep the Nth part (0-based). If "output" is omitted, overwrites the source column. Use when a value like "GENE_rsid_alleles" needs to be split to extract one component.

Only suggest transformations that are clearly needed based on the data. Do not suggest transformations speculatively.

## Instructions
Respond with ONLY a JSON object (no markdown, no explanation outside JSON):
{{
  "category": "ABBREV",
  "reasoning": "1-2 sentence explanation of why this category fits",
  "mappings": {{"gene": "column_name", "score": "column_name", ...}},
  "centric": "gene" or "variant",
  "quality_notes": ["non-transform observations about data quality"],
  "transformations": [{{"type": "strip_prefix", "column": "chr", "prefix": "chr"}}],
  "confidence": 0.0-1.0
}}

Use standard PEGASUS field names in mappings: gene, chromosome, position, rsid, pvalue, score, effect_size, tissue, cell_type, trait.
"""


def _parse_response(text: str) -> AISuggestion | None:
    """Parse AI response. Tries JSON extraction."""
    text = text.strip()

    # Try to find JSON in the response
    json_str = text
    # Handle markdown code blocks
    if "```" in text:
        parts = text.split("```")
        for part in parts:
            part = part.strip()
            if part.startswith("json"):
                part = part[4:].strip()
            if part.startswith("{"):
                json_str = part
                break

    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        # Try to find first { ... } block
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try:
                data = json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                logger.warning("Could not parse AI response as JSON")
                return None
        else:
            logger.warning("No JSON found in AI response")
            return None

    return AISuggestion(
        category=data.get("category"),
        category_reasoning=data.get("reasoning", ""),
        column_mappings=data.get("mappings", {}),
        centric=data.get("centric", "gene"),
        quality_notes=data.get("quality_notes", []),
        transformations=data.get("transformations", []),
        confidence=float(data.get("confidence", 0.5)),
        raw_response=text,
    )


# ---------------------------------------------------------------------------
# Lightweight column role detection
# ---------------------------------------------------------------------------


def _build_column_prompt(columns: list[dict], context: str = "sentinel") -> str:
    """Build a prompt for AI column role detection."""
    if context == "sentinel":
        roles = "chromosome, position, trait, gene, pvalue, rsid, sentinel_id"
        description = (
            "sentinel variant data for a GWAS study. "
            "Chromosome and position columns are required for locus clustering."
        )
    else:
        roles = "gene, chromosome, position, rsid, pvalue, score, effect_size"
        description = "genomic evidence data for PEGASUS gene prioritization."

    col_summary = json.dumps(columns, indent=2)

    return f"""Given these columns from {description}, identify which column serves each role.

## Columns (name, type, sample values)
```json
{col_summary}
```

## Roles to identify
{roles}

Respond with ONLY a JSON object mapping role to column name. Only include roles where you're confident of the match. Example:
{{"chromosome": "CHROM_NUM", "position": "BP", "gene": "nearest_gene"}}

If a column clearly contains the data but has an unusual name, still map it.
Omit roles that have no matching column.
"""


def _parse_column_response(text: str) -> AIColumnSuggestion | None:
    """Parse AI column suggestion response."""
    text = text.strip()

    # Extract JSON
    json_str = text
    if "```" in text:
        parts = text.split("```")
        for part in parts:
            part = part.strip()
            if part.startswith("json"):
                part = part[4:].strip()
            if part.startswith("{"):
                json_str = part
                break

    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try:
                data = json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                return None
        else:
            return None

    if not isinstance(data, dict):
        return None

    return AIColumnSuggestion(
        mappings=data,
        raw_response=text,
    )
