"""Tests for PEGASUS CLI commands (rescore, export pegasus)."""

import duckdb
import pytest
from click.testing import CliRunner

from pegasus_v2f.cli import cli
from pegasus_v2f.pegasus_schema import create_pegasus_schema


@pytest.fixture
def pegasus_db_path(tmp_path):
    """Create a DuckDB file with PEGASUS schema and data for CLI testing."""
    db_path = tmp_path / "test.duckdb"
    c = duckdb.connect(str(db_path))
    create_pegasus_schema(c)

    # Minimal data
    c.execute(
        "INSERT INTO studies (study_id, study_name, trait) VALUES ('s1', 'test', 'HEIGHT')"
    )
    c.execute(
        "INSERT INTO loci (locus_id, study_id, chromosome, start_position, end_position, lead_pvalue) "
        "VALUES ('l1', 's1', '1', 900000, 1100000, 1e-10)"
    )
    c.execute(
        "INSERT INTO genes (gene_symbol, chromosome, start_position, end_position) "
        "VALUES ('GENE_A', '1', 950000, 1050000)"
    )
    c.execute(
        "INSERT INTO evidence (gene_symbol, evidence_category, source_tag) "
        "VALUES ('GENE_A', 'GWAS', 'src1')"
    )

    # Store config in _pegasus_meta
    from pegasus_v2f.db_meta import ensure_meta_table, write_meta
    import yaml
    ensure_meta_table(c)
    config = {
        "version": 1,
        "pegasus": {
            "study": [{"id_prefix": "test", "traits": ["HEIGHT"]}],
        },
    }
    write_meta(c, "config", yaml.dump(config))

    c.close()
    return db_path


class TestRescore:
    def test_rescore_runs(self, pegasus_db_path, tmp_path):
        """Rescore command runs scoring and reports count."""
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text(
            "version: 1\n"
            "pegasus:\n"
            "  study:\n"
            "  - id_prefix: test\n"
            "    traits: [HEIGHT]\n"
            "database:\n"
            "  backend: duckdb\n"
            f"  name: {pegasus_db_path.name}\n"
        )

        runner = CliRunner()
        result = runner.invoke(cli, ["--db", str(pegasus_db_path), "--project", str(tmp_path), "rescore"])
        assert result.exit_code == 0
        assert "Scored" in result.output or "scored" in result.output


class TestStudyPreview:
    def test_preview_runs(self, pegasus_db_path):
        """Study preview command displays locus summary."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--db", str(pegasus_db_path), "study", "preview", "test"])
        assert result.exit_code == 0
        assert "loci" in result.output.lower()

    def test_preview_nonexistent_study(self, pegasus_db_path):
        """Preview of nonexistent study shows 'no loci' message."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--db", str(pegasus_db_path), "study", "preview", "nope"])
        assert result.exit_code == 0
        assert "no loci" in result.output.lower()


class TestSourceAddEvidence:
    """Test source add with evidence configuration flags."""

    @pytest.fixture
    def source_file(self, tmp_path):
        """Create a simple TSV source file."""
        tsv = tmp_path / "genes.tsv"
        tsv.write_text("gene\tscore\nGENE_A\t0.9\nGENE_B\t0.5\n")
        return tsv

    def test_non_interactive_with_category(self, pegasus_db_path, tmp_path, source_file):
        """source add with --category and --skip creates evidence block."""
        runner = CliRunner()
        result = runner.invoke(cli, [
            "--db", str(pegasus_db_path),
            "source", "add", "test_src",
            "--type", "file",
            "--path", str(source_file),
            "--skip", "0",
            "--category", "KNOW",
        ])
        assert result.exit_code == 0, result.output
        assert "test_src" in result.output

        # Verify evidence was loaded
        import duckdb
        c = duckdb.connect(str(pegasus_db_path), read_only=True)
        rows = c.execute(
            "SELECT COUNT(*) FROM evidence WHERE source_tag = 'test_src'"
        ).fetchone()[0]
        c.close()
        assert rows == 2

    def test_non_interactive_with_traits(self, pegasus_db_path, tmp_path, source_file):
        """source add with --traits attaches trait tags to evidence."""
        runner = CliRunner()
        result = runner.invoke(cli, [
            "--db", str(pegasus_db_path),
            "source", "add", "trait_src",
            "--type", "file",
            "--path", str(source_file),
            "--skip", "0",
            "--category", "KNOW",
            "--traits", "FEV1,FVC",
        ])
        assert result.exit_code == 0, result.output

        import duckdb
        c = duckdb.connect(str(pegasus_db_path), read_only=True)
        trait_val = c.execute(
            "SELECT DISTINCT trait FROM evidence WHERE source_tag = 'trait_src'"
        ).fetchone()[0]
        c.close()
        assert "FEV1" in trait_val
        assert "FVC" in trait_val

    def test_non_interactive_with_source_tag(self, pegasus_db_path, tmp_path, source_file):
        """source add with --source-tag uses custom tag."""
        runner = CliRunner()
        result = runner.invoke(cli, [
            "--db", str(pegasus_db_path),
            "source", "add", "custom_tag_src",
            "--type", "file",
            "--path", str(source_file),
            "--skip", "0",
            "--category", "KNOW",
            "--source-tag", "my_custom_tag",
        ])
        assert result.exit_code == 0, result.output

        import duckdb
        c = duckdb.connect(str(pegasus_db_path), read_only=True)
        rows = c.execute(
            "SELECT COUNT(*) FROM evidence WHERE source_tag = 'my_custom_tag'"
        ).fetchone()[0]
        c.close()
        assert rows == 2

    def test_evidence_json_single_block(self, pegasus_db_path, tmp_path, source_file):
        """source add with --evidence-json creates evidence blocks."""
        import json
        runner = CliRunner()
        ev = [{"source_tag": "interactive_src", "category": "KNOW",
               "fields": {"gene": "gene", "score": "score"}}]
        result = runner.invoke(cli, [
            "--db", str(pegasus_db_path),
            "source", "add", "interactive_src",
            "--type", "file",
            "--path", str(source_file),
            "--skip", "0",
            "--evidence-json", json.dumps(ev),
        ])
        assert result.exit_code == 0, result.output

        import duckdb
        c = duckdb.connect(str(pegasus_db_path), read_only=True)
        rows = c.execute(
            "SELECT COUNT(*) FROM evidence WHERE source_tag = 'interactive_src'"
        ).fetchone()[0]
        c.close()
        assert rows == 2

    def test_no_evidence_flags_no_wizard(self, pegasus_db_path, tmp_path, source_file):
        """source add without evidence flags runs wizard; with mocked questionary returning
        no selections, adds raw source only."""
        import sys
        from unittest.mock import patch, MagicMock
        runner = CliRunner()

        # Mock questionary module before it gets imported inside the function
        mock_q = MagicMock()
        mock_q.confirm.return_value.ask.return_value = False  # variant toggle = no
        mock_q.select.return_value.ask.return_value = "gene"  # gene column
        mock_q.checkbox.return_value.ask.return_value = []  # no evidence columns

        with patch.dict(sys.modules, {"questionary": mock_q}):
            result = runner.invoke(cli, [
                "--db", str(pegasus_db_path),
                "source", "add", "raw_src",
                "--type", "file",
                "--path", str(source_file),
                "--skip", "0",
            ], input="y\n")
            assert result.exit_code == 0, result.output

        import duckdb
        c = duckdb.connect(str(pegasus_db_path), read_only=True)
        rows = c.execute(
            "SELECT COUNT(*) FROM evidence WHERE source_tag = 'raw_src'"
        ).fetchone()[0]
        c.close()
        assert rows == 0

    def test_evidence_json_with_traits(self, pegasus_db_path, tmp_path, source_file):
        """source add with --evidence-json and traits loads evidence with trait tags."""
        import json
        runner = CliRunner()
        ev = [{"source_tag": "trait_prompt_src", "category": "KNOW",
               "fields": {"gene": "gene", "score": "score"},
               "traits": ["FEV1", "FVC"]}]
        result = runner.invoke(cli, [
            "--db", str(pegasus_db_path),
            "source", "add", "trait_prompt_src",
            "--type", "file",
            "--path", str(source_file),
            "--skip", "0",
            "--evidence-json", json.dumps(ev),
        ])
        assert result.exit_code == 0, result.output

        import duckdb
        c = duckdb.connect(str(pegasus_db_path), read_only=True)
        trait_val = c.execute(
            "SELECT DISTINCT trait FROM evidence WHERE source_tag = 'trait_prompt_src'"
        ).fetchone()[0]
        c.close()
        assert "FEV1" in trait_val

    def test_evidence_json_auto_centric(self, pegasus_db_path, tmp_path, source_file):
        """--evidence-json auto-sets centric to 'gene' when no chr/pos in fields."""
        import json
        runner = CliRunner()
        ev = [{"source_tag": "auto_centric", "category": "KNOW",
               "fields": {"gene": "gene", "score": "score"}}]
        result = runner.invoke(cli, [
            "--db", str(pegasus_db_path),
            "source", "add", "auto_centric_src",
            "--type", "file",
            "--path", str(source_file),
            "--skip", "0",
            "--evidence-json", json.dumps(ev),
        ])
        assert result.exit_code == 0, result.output


class TestExportPegasus:
    def test_export_creates_files(self, pegasus_db_path, tmp_path):
        """Export pegasus command creates the 3 deliverable files."""
        output_dir = tmp_path / "export"
        runner = CliRunner()
        result = runner.invoke(
            cli, ["--db", str(pegasus_db_path), "export", "pegasus", "s1", "-o", str(output_dir)]
        )
        assert result.exit_code == 0
        assert "evidence_matrix" in result.output
        assert "metadata" in result.output
        assert "peg_list" in result.output
        assert (output_dir / "evidence_matrix.tsv").exists()
        assert (output_dir / "metadata.yaml").exists()
        assert (output_dir / "peg_list.tsv").exists()
