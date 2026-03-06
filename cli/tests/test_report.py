"""Tests for the Report structured validation reporting system."""

from pegasus_v2f.report import Report, ReportItem, render_report


def test_report_basic():
    r = Report(operation="test")
    r.info("loaded", "loaded 100 rows")
    r.warning("empty_gene", "rows dropped", count=5)
    r.error("insert_failed", "rows failed", count=2)

    assert len(r.items) == 3
    assert r.items[0].severity == "info"
    assert r.items[1].severity == "warning"
    assert r.items[1].count == 5
    assert r.items[2].severity == "error"


def test_report_counters():
    r = Report(operation="test")
    r.counters["rows_in"] = 100
    r.counters["rows_out"] = 95

    d = r.to_dict()
    assert d["counters"]["rows_in"] == 100
    assert d["counters"]["rows_out"] == 95


def test_report_children():
    parent = Report(operation="build")
    child1 = parent.child("source:shrine_genes")
    child2 = parent.child("source:coloc")

    child1.info("loaded", "50 rows")
    child2.warning("empty_gene", "3 rows dropped", count=3)

    assert len(parent.children) == 2
    assert parent.children[0].operation == "source:shrine_genes"
    assert parent.children[1].operation == "source:coloc"


def test_has_warnings():
    r = Report(operation="test")
    assert not r.has_warnings

    r.info("ok", "all good")
    assert not r.has_warnings

    r.warning("minor", "something dropped")
    assert r.has_warnings


def test_has_warnings_nested():
    parent = Report(operation="build")
    assert not parent.has_warnings

    child = parent.child("source")
    assert not parent.has_warnings

    child.warning("dropped", "rows dropped")
    assert parent.has_warnings


def test_has_errors():
    r = Report(operation="test")
    assert not r.has_errors

    r.warning("minor", "just a warning")
    assert not r.has_errors

    r.error("failed", "something broke")
    assert r.has_errors


def test_has_errors_nested():
    parent = Report(operation="build")
    child = parent.child("source")
    child.error("failed", "boom")
    assert parent.has_errors


def test_warning_count():
    parent = Report(operation="build")
    parent.warning("a", "a")
    child = parent.child("source")
    child.warning("b", "b")
    child.error("c", "c")

    assert parent.warning_count == 3  # 1 own + 2 child


def test_to_dict_empty():
    r = Report(operation="test")
    d = r.to_dict()
    assert d == {"operation": "test"}


def test_to_dict_full():
    r = Report(operation="source_add")
    r.counters["rows_in"] = 100
    r.warning("empty_gene", "rows dropped", count=5, field="gene")

    d = r.to_dict()
    assert d["operation"] == "source_add"
    assert d["counters"]["rows_in"] == 100
    assert len(d["items"]) == 1
    assert d["items"][0]["code"] == "empty_gene"
    assert d["items"][0]["count"] == 5
    assert d["items"][0]["details"]["field"] == "gene"


def test_to_dict_nested():
    parent = Report(operation="build")
    child = parent.child("source:x")
    child.info("ok", "loaded")

    d = parent.to_dict()
    assert len(d["children"]) == 1
    assert d["children"][0]["operation"] == "source:x"


def test_to_json():
    r = Report(operation="test")
    r.info("loaded", "ok")
    j = r.to_json()
    assert '"operation": "test"' in j
    assert '"loaded"' in j


def test_render_report_json(capsys):
    r = Report(operation="test")
    r.warning("dropped", "5 rows dropped", count=5)

    render_report(r, json_mode=True)

    captured = capsys.readouterr()
    assert '"operation": "test"' in captured.out
    assert '"dropped"' in captured.out


def test_render_report_silent_when_no_warnings(capsys):
    r = Report(operation="test")
    r.info("loaded", "all good")

    render_report(r)  # should produce no output

    captured = capsys.readouterr()
    assert captured.out == ""
