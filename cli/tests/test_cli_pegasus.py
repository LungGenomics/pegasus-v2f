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


class TestSourceConfigureLoad:
    """Test source configure + load two-step flow."""

    @pytest.fixture
    def source_file(self, tmp_path):
        """Create a simple TSV source file."""
        tsv = tmp_path / "genes.tsv"
        tsv.write_text("gene\tscore\nGENE_A\t0.9\nGENE_B\t0.5\n")
        return tsv

    @pytest.fixture
    def project_dir(self, tmp_path, pegasus_db_path, source_file):
        """Create a minimal v2f project directory."""
        import yaml
        config = {
            "version": 1,
            "database": {"backend": "duckdb", "name": pegasus_db_path.name},
        }
        (tmp_path / "v2f.yaml").write_text(yaml.dump(config))
        return tmp_path

    def test_configure_writes_config(self, project_dir, pegasus_db_path, source_file):
        """source configure proposes and writes config to v2f.yaml."""
        runner = CliRunner()
        result = runner.invoke(cli, [
            "--db", str(pegasus_db_path),
            "--project", str(project_dir),
            "source", "configure", str(source_file),
            "--name", "test_src",
            "--category", "KNOW",
        ])
        assert result.exit_code == 0, result.output
        assert "test_src" in result.output

        # Check config was written
        import yaml
        config = yaml.safe_load((project_dir / "v2f.yaml").read_text())
        sources = config.get("data_sources", [])
        assert any(s["name"] == "test_src" for s in sources)

    def test_configure_then_load(self, project_dir, pegasus_db_path, source_file):
        """source configure + source load creates evidence in DB."""
        runner = CliRunner()
        # Step 1: configure
        result = runner.invoke(cli, [
            "--db", str(pegasus_db_path),
            "--project", str(project_dir),
            "source", "configure", str(source_file),
            "--name", "test_src",
            "--category", "KNOW",
        ])
        assert result.exit_code == 0, result.output

        # Step 2: load
        result = runner.invoke(cli, [
            "--db", str(pegasus_db_path),
            "--project", str(project_dir),
            "source", "load", "test_src", "-y",
        ])
        assert result.exit_code == 0, result.output

    def test_configure_with_traits(self, project_dir, pegasus_db_path, source_file):
        """source configure with --traits writes traits to config."""
        runner = CliRunner()
        result = runner.invoke(cli, [
            "--db", str(pegasus_db_path),
            "--project", str(project_dir),
            "source", "configure", str(source_file),
            "--name", "trait_src",
            "--category", "KNOW",
            "--traits", "FEV1,FVC",
        ])
        assert result.exit_code == 0, result.output

        import yaml
        config = yaml.safe_load((project_dir / "v2f.yaml").read_text())
        sources = config.get("data_sources", [])
        src = next(s for s in sources if s["name"] == "trait_src")
        evidence = src.get("evidence", [])
        assert len(evidence) > 0
        assert "FEV1" in evidence[0].get("traits", [])

    def test_configure_with_source_tag(self, project_dir, pegasus_db_path, source_file):
        """source configure with --source-tag uses custom tag."""
        runner = CliRunner()
        result = runner.invoke(cli, [
            "--db", str(pegasus_db_path),
            "--project", str(project_dir),
            "source", "configure", str(source_file),
            "--name", "custom_tag_src",
            "--category", "KNOW",
            "--source-tag", "my_custom_tag",
        ])
        assert result.exit_code == 0, result.output

        import yaml
        config = yaml.safe_load((project_dir / "v2f.yaml").read_text())
        sources = config.get("data_sources", [])
        src = next(s for s in sources if s["name"] == "custom_tag_src")
        evidence = src.get("evidence", [])
        assert any(e.get("source_tag") == "my_custom_tag" for e in evidence)

    def test_configure_json_output(self, project_dir, pegasus_db_path, source_file):
        """source configure --json outputs structured JSON and still writes config."""
        import json, yaml
        runner = CliRunner()
        result = runner.invoke(cli, [
            "--db", str(pegasus_db_path),
            "--project", str(project_dir),
            "source", "configure", str(source_file),
            "--name", "json_src",
            "--category", "KNOW",
            "--json",
        ])
        assert result.exit_code == 0, result.output
        output = json.loads(result.output)
        assert "proposed_config" in output
        assert "validation" in output
        assert output.get("written") is True

        # Config should be written to v2f.yaml
        config = yaml.safe_load((project_dir / "v2f.yaml").read_text())
        sources = config.get("data_sources", [])
        assert any(s["name"] == "json_src" for s in sources)

    def test_configure_with_evidence_json(self, project_dir, pegasus_db_path, source_file):
        """source configure with --evidence-json overrides auto-proposed evidence."""
        import json, yaml
        evidence = [
            {"source_tag": "tag_a", "category": "QTL", "centric": "gene", "fields": {"gene": "gene", "score": "score"}},
            {"source_tag": "tag_b", "category": "FUNC", "centric": "gene", "fields": {"gene": "gene"}},
        ]
        runner = CliRunner()
        result = runner.invoke(cli, [
            "--db", str(pegasus_db_path),
            "--project", str(project_dir),
            "source", "configure", str(source_file),
            "--name", "multi_ev_src",
            "--evidence-json", json.dumps(evidence),
        ])
        assert result.exit_code == 0, result.output

        config = yaml.safe_load((project_dir / "v2f.yaml").read_text())
        sources = config.get("data_sources", [])
        src = next(s for s in sources if s["name"] == "multi_ev_src")
        assert len(src.get("evidence", [])) == 2
        assert src["evidence"][0]["source_tag"] == "tag_a"
        assert src["evidence"][1]["category"] == "FUNC"

    def test_configure_with_display_name(self, project_dir, pegasus_db_path, source_file):
        """source configure with --display-name sets display_name in config."""
        import yaml
        runner = CliRunner()
        result = runner.invoke(cli, [
            "--db", str(pegasus_db_path),
            "--project", str(project_dir),
            "source", "configure", str(source_file),
            "--name", "dn_src",
            "--category", "KNOW",
            "--display-name", "My Nice Name",
        ])
        assert result.exit_code == 0, result.output

        config = yaml.safe_load((project_dir / "v2f.yaml").read_text())
        sources = config.get("data_sources", [])
        src = next(s for s in sources if s["name"] == "dn_src")
        assert src.get("display_name") == "My Nice Name"

    def test_load_nonexistent_source_errors(self, project_dir, pegasus_db_path):
        """source load for a source not in v2f.yaml gives clear error."""
        runner = CliRunner()
        result = runner.invoke(cli, [
            "--db", str(pegasus_db_path),
            "--project", str(project_dir),
            "source", "load", "nonexistent_src", "-y",
        ])
        assert result.exit_code != 0
        assert "not found" in result.output.lower()


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
