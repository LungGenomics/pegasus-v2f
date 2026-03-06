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
    normalization_suggestions: list[str]
    confidence: float  # 0.0 - 1.0
    raw_response: str = ""

    def to_dict(self) -> dict:
        return {
            "category": self.category,
            "category_reasoning": self.category_reasoning,
            "column_mappings": self.column_mappings,
            "centric": self.centric,
            "quality_notes": self.quality_notes,
            "normalization_suggestions": self.normalization_suggestions,
            "confidence": self.confidence,
        }


class AIProvider(ABC):
    """Abstract base for AI suggestion providers."""

    @abstractmethod
    def suggest(self, inspection: InspectionResult, timeout: int = 60) -> AISuggestion | None:
        ...

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

    def suggest(self, inspection: InspectionResult, timeout: int = 60) -> AISuggestion | None:
        prompt = _build_prompt(inspection)
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


def _build_prompt(inspection: InspectionResult) -> str:
    """Build structured prompt with inspection data and PEGASUS context."""
    # Compact inspection data
    inspection_data = {
        "source_name": inspection.source_name,
        "row_count": inspection.row_count,
        "columns": [c.to_dict() for c in inspection.columns],
    }
    if inspection.gene_analysis:
        inspection_data["gene_analysis"] = inspection.gene_analysis.to_dict()
    if inspection.chromosome_analysis:
        inspection_data["chromosome_analysis"] = inspection.chromosome_analysis.to_dict()
    if inspection.suggested_mappings:
        inspection_data["suggested_mappings"] = inspection.suggested_mappings

    # Category descriptions for context
    categories = {}
    for abbrev, profile in EVIDENCE_CATEGORY_PROFILES.items():
        categories[abbrev] = {
            "label": profile.label,
            "description": profile.description,
            "centric_group": profile.centric_group,
            "typical_value": profile.typical_value,
        }

    return f"""You are a PEGASUS evidence categorization assistant. Analyze this data source inspection and suggest the best PEGASUS evidence category and column mappings.

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

## Instructions
Respond with ONLY a JSON object (no markdown, no explanation outside JSON):
{{
  "category": "ABBREV",
  "reasoning": "1-2 sentence explanation of why this category fits",
  "mappings": {{"gene": "column_name", "score": "column_name", ...}},
  "centric": "gene" or "variant",
  "quality_notes": ["list of data quality observations"],
  "normalization_suggestions": ["list of suggested normalizations"],
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
        normalization_suggestions=data.get("normalization_suggestions", []),
        confidence=float(data.get("confidence", 0.5)),
        raw_response=text,
    )
