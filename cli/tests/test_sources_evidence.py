"""Tests for evidence-aware source CRUD operations."""

import duckdb
import pytest

from pegasus_v2f.pegasus_schema import create_pegasus_schema
from pegasus_v2f.db_schema import create_schema
from pegasus_v2f.db_meta import ensure_meta_table
from pegasus_v2f.sources import remove_source, _delete_evidence_by_source_tag


PEGASUS_CONFIG = {
    "version": 1,
    "pegasus": {
        "study": {"id_prefix": "test", "traits": ["HEIGHT"]},
    },
}


@pytest.fixture
def evidence_db():
    """DB with PEGASUS schema + core schema + studies + loci for evidence CRUD testing."""
    c = duckdb.connect(":memory:")
    create_schema(c, config=PEGASUS_CONFIG)
    ensure_meta_table(c)

    # Study + locus
    c.execute(
        "INSERT INTO studies (study_id, study_name, trait) "
        "VALUES ('test_height', 'test', 'HEIGHT')"
    )
    c.execute(
        "INSERT INTO loci (locus_id, study_id, chromosome, start_position, end_position, lead_pvalue) "
        "VALUES ('l1', 'test_height', '1', 900000, 1100000, 1e-10)"
    )

    # Some existing evidence in the unified evidence table
    c.execute(
        "INSERT INTO evidence (gene_symbol, evidence_category, source_tag) "
        "VALUES ('GENE_A', 'GWAS', 'existing_src')"
    )
    c.execute(
        "INSERT INTO evidence (gene_symbol, evidence_category, source_tag) "
        "VALUES ('GENE_A', 'KNOW', 'to_remove')"
    )

    yield c
    c.close()


class TestDeleteEvidenceBySourceTag:
    def test_deletes_evidence(self, evidence_db):
        _delete_evidence_by_source_tag(evidence_db, "existing_src")
        count = evidence_db.execute(
            "SELECT COUNT(*) FROM evidence WHERE source_tag = 'existing_src'"
        ).fetchone()[0]
        assert count == 0

    def test_deletes_other_tag(self, evidence_db):
        _delete_evidence_by_source_tag(evidence_db, "to_remove")
        count = evidence_db.execute(
            "SELECT COUNT(*) FROM evidence WHERE source_tag = 'to_remove'"
        ).fetchone()[0]
        assert count == 0

    def test_doesnt_affect_other_tags(self, evidence_db):
        _delete_evidence_by_source_tag(evidence_db, "to_remove")
        # existing_src should still be there
        count = evidence_db.execute(
            "SELECT COUNT(*) FROM evidence WHERE source_tag = 'existing_src'"
        ).fetchone()[0]
        assert count == 1


class TestRemoveSourceEvidence:
    def test_removes_evidence_and_cleans_up(self, evidence_db):
        """remove_source with evidence block deletes evidence rows."""
        import yaml
        from pegasus_v2f.db_meta import write_meta
        config_with_source = {
            **PEGASUS_CONFIG,
            "data_sources": [{
                "name": "my_evidence_source",
                "source_type": "file",
                "evidence": [{
                    "category": "KNOW",
                    "source_tag": "to_remove",
                    "fields": {"gene": "gene"},
                }],
            }],
        }
        write_meta(evidence_db, "config", yaml.dump(config_with_source))

        # Verify evidence exists before removal
        before = evidence_db.execute(
            "SELECT COUNT(*) FROM evidence WHERE source_tag = 'to_remove'"
        ).fetchone()[0]
        assert before == 1

        # Remove it
        remove_source(evidence_db, "my_evidence_source", config=PEGASUS_CONFIG)

        # Evidence should be gone
        after = evidence_db.execute(
            "SELECT COUNT(*) FROM evidence WHERE source_tag = 'to_remove'"
        ).fetchone()[0]
        assert after == 0

    def test_removes_raw_source_drops_table(self, evidence_db):
        """remove_source without evidence block drops the raw table."""
        import yaml
        from pegasus_v2f.db_meta import write_meta

        from pegasus_v2f.db import raw_table_name
        raw_name = raw_table_name("test_source")
        evidence_db.execute(f'CREATE TABLE "{raw_name}" AS SELECT \'foo\' AS gene')
        config_with_source = {
            **PEGASUS_CONFIG,
            "data_sources": [{"name": "test_source", "source_type": "file"}],
        }
        write_meta(evidence_db, "config", yaml.dump(config_with_source))

        remove_source(evidence_db, "test_source", config=PEGASUS_CONFIG)

        # Table should be gone
        tables = [r[0] for r in evidence_db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()]
        assert raw_name not in tables
