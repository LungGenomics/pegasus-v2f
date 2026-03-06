"""Structured validation reporting for v2f pipeline operations.

Modules accept an optional ``report`` parameter and append items to it as they
process data.  The CLI creates a Report, threads it through the call stack, and
renders it at the end so the user sees what was dropped and why.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ReportItem:
    """A single tagged issue discovered during processing."""

    severity: str  # "info", "warning", "error"
    code: str  # machine-readable key, e.g. "empty_gene"
    message: str  # human-readable description
    count: int = 1
    details: dict[str, Any] | None = None


@dataclass
class Report:
    """Accumulates what happened during a pipeline operation."""

    operation: str
    counters: dict[str, int] = field(default_factory=dict)
    items: list[ReportItem] = field(default_factory=list)
    children: list[Report] = field(default_factory=list)

    # --- Append helpers ---

    def info(self, code: str, message: str, count: int = 1, **details: Any) -> None:
        self.items.append(ReportItem("info", code, message, count, details or None))

    def warning(self, code: str, message: str, count: int = 1, **details: Any) -> None:
        self.items.append(ReportItem("warning", code, message, count, details or None))

    def error(self, code: str, message: str, count: int = 1, **details: Any) -> None:
        self.items.append(ReportItem("error", code, message, count, details or None))

    def child(self, operation: str) -> Report:
        """Create and attach a nested sub-report."""
        c = Report(operation=operation)
        self.children.append(c)
        return c

    # --- Queries ---

    @property
    def has_warnings(self) -> bool:
        return (
            any(i.severity in ("warning", "error") for i in self.items)
            or any(c.has_warnings for c in self.children)
        )

    @property
    def has_errors(self) -> bool:
        return (
            any(i.severity == "error" for i in self.items)
            or any(c.has_errors for c in self.children)
        )

    @property
    def warning_count(self) -> int:
        own = sum(1 for i in self.items if i.severity in ("warning", "error"))
        return own + sum(c.warning_count for c in self.children)

    # --- Serialisation ---

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"operation": self.operation}
        if self.counters:
            d["counters"] = self.counters
        if self.items:
            d["items"] = [
                {
                    "severity": i.severity,
                    "code": i.code,
                    "message": i.message,
                    "count": i.count,
                    **({"details": i.details} if i.details else {}),
                }
                for i in self.items
            ]
        if self.children:
            d["children"] = [c.to_dict() for c in self.children]
        return d

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

_SEVERITY_STYLE = {
    "info": "dim",
    "warning": "yellow",
    "error": "bold red",
}


def render_report(report: Report, *, console: Any | None = None, json_mode: bool = False) -> None:
    """Render a Report to the terminal.

    Args:
        report: The report to render.
        console: A ``rich.console.Console`` instance.  If *None* one is created
            targeting *stderr* so that stdout stays clean for piping.
        json_mode: If True, print JSON to stdout instead of Rich text to stderr.
    """
    if json_mode:
        print(report.to_json())
        return

    if not report.has_warnings and not report.items:
        return  # nothing to show

    from rich.console import Console
    from rich.tree import Tree

    if console is None:
        import sys
        console = Console(stderr=True, file=sys.stderr)

    tree = _build_tree(report)
    console.print(tree)


def _build_tree(report: Report) -> Any:
    from rich.tree import Tree

    label = f"[bold]{report.operation}[/bold]"
    if report.counters:
        parts = []
        if "rows_in" in report.counters:
            parts.append(f"{report.counters['rows_in']} in")
        if "rows_out" in report.counters:
            parts.append(f"{report.counters['rows_out']} out")
        if "rows_dropped" in report.counters:
            parts.append(f"{report.counters['rows_dropped']} dropped")
        # Include any other counters not already shown
        for k, v in report.counters.items():
            if k not in ("rows_in", "rows_out", "rows_dropped"):
                parts.append(f"{k}={v}")
        if parts:
            label += f"  ({', '.join(parts)})"

    tree = Tree(label)

    for item in report.items:
        style = _SEVERITY_STYLE.get(item.severity, "")
        count_str = f" ({item.count})" if item.count > 1 else ""
        tree.add(f"[{style}]{item.severity}[/{style}] {item.code}{count_str}: {item.message}")

    for child in report.children:
        tree.add(_build_tree(child))

    return tree
