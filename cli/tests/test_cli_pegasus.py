"""Tests for PEGASUS CLI commands (materialize, export pegasus)."""

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
    c.execute("INSERT INTO studies (study_id, trait) VALUES ('s1', 'HEIGHT')")
    c.execute(
        "INSERT INTO loci (locus_id, study_id, chromosome, start_position, end_position, lead_pvalue) "
        "VALUES ('l1', 's1', '1', 900000, 1100000, 1e-10)"
    )
    c.execute(
        "INSERT INTO genes (gene_symbol, chromosome, start_position, end_position) "
        "VALUES ('GENE_A', '1', 950000, 1050000)"
    )
    c.execute(
        "INSERT INTO locus_gene_evidence (locus_id, gene_symbol, evidence_category, source_tag) "
        "VALUES ('l1', 'GENE_A', 'GWAS', 'src1')"
    )

    # Store config in _pegasus_meta for materialize to find
    c.execute(
        "CREATE TABLE IF NOT EXISTS _pegasus_meta (key VARCHAR PRIMARY KEY, value VARCHAR)"
    )
    import yaml
    config = {
        "version": 1,
        "pegasus": {
            "study": [{"id_prefix": "test", "traits": ["HEIGHT"]}],
            "integration": {"method": "criteria_count_v1", "effector_threshold": 0.25, "criteria": []},
        },
    }
    c.execute(
        "INSERT INTO _pegasus_meta VALUES ('config', ?)",
        [yaml.dump(config)],
    )

    c.close()
    return db_path


class TestMaterialize:
    def test_materialize_scores(self, pegasus_db_path, tmp_path):
        """Materialize command runs scoring and reports count."""
        # Create a v2f.yaml for the config
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text(
            "version: 1\n"
            "pegasus:\n"
            "  study:\n"
            "  - id_prefix: test\n"
            "    traits: [HEIGHT]\n"
            "  integration:\n"
            "    method: criteria_count_v1\n"
            "    effector_threshold: 0.25\n"
            "    criteria: []\n"
            "database:\n"
            "  backend: duckdb\n"
            f"  name: {pegasus_db_path.name}\n"
        )

        runner = CliRunner()
        result = runner.invoke(cli, ["--db", str(pegasus_db_path), "--project", str(tmp_path), "materialize"])
        assert result.exit_code == 0
        assert "Scored" in result.output
        assert "1" in result.output  # 1 locus-gene pair

    def test_materialize_no_pegasus_config(self, tmp_path):
        """Materialize fails gracefully without pegasus config."""
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\ndata_sources: []\n")

        db_path = tmp_path / "test.duckdb"
        runner = CliRunner()
        result = runner.invoke(cli, ["--db", str(db_path), "--project", str(tmp_path), "materialize"])
        assert result.exit_code != 0
        assert "pegasus" in result.output.lower() or "PEGASUS" in result.output


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
