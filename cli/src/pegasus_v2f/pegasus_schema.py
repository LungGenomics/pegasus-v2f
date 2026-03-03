"""PEGASUS schema — DDL for evidence model tables and controlled vocabulary."""

from __future__ import annotations

from typing import Any

from pegasus_v2f.db import is_postgres

# -- PEGASUS controlled vocabulary --
# 22 evidence category abbreviations from the PEGASUS standard.
# Keys are the canonical abbreviations used in the database;
# values are human-readable labels.

EVIDENCE_CATEGORIES: dict[str, str] = {
    "QTL": "Quantitative Trait Locus",
    "COLOC": "Colocalization",
    "GWAS": "GWAS Association",
    "PROX": "Proximity",
    "CODE": "Coding Variant",
    "RARE": "Rare Variant",
    "EXP": "Expression",
    "EPIG": "Epigenomic",
    "CHROM": "Chromatin Interaction",
    "REG": "Regulatory",
    "FUNC": "Functional",
    "MOD": "Animal Model",
    "DRUG": "Druggability",
    "PATH": "Pathway",
    "PPI": "Protein-Protein Interaction",
    "KNOW": "Known Biology",
    "LIT": "Literature",
    "CLIN": "Clinical",
    "OMICS": "Multi-omics",
    "PERT": "Perturbation",
    "EVOL": "Evolutionary",
    "OTHER": "Other",
}


# -- DDL for PEGASUS tables --

GENES_DDL = """
CREATE TABLE IF NOT EXISTS genes (
    gene_symbol VARCHAR PRIMARY KEY,
    ensembl_gene_id VARCHAR UNIQUE,
    gene_name VARCHAR,
    chromosome VARCHAR,
    start_position BIGINT,
    end_position BIGINT,
    strand INTEGER,
    genome_build VARCHAR DEFAULT 'GRCh38'
)
"""

VARIANTS_DDL = """
CREATE TABLE IF NOT EXISTS variants (
    variant_id VARCHAR PRIMARY KEY,
    rsid VARCHAR,
    chromosome VARCHAR NOT NULL,
    position BIGINT NOT NULL,
    ref_allele VARCHAR,
    alt_allele VARCHAR,
    genome_build VARCHAR DEFAULT 'GRCh38'
)
"""

STUDIES_DDL = """
CREATE TABLE IF NOT EXISTS studies (
    study_id VARCHAR PRIMARY KEY,
    trait VARCHAR NOT NULL,
    trait_description VARCHAR,
    trait_ontology_id VARCHAR,
    gwas_source VARCHAR,
    ancestry VARCHAR,
    sample_size INTEGER,
    n_loci INTEGER DEFAULT 0
)
"""

LOCI_DDL = """
CREATE TABLE IF NOT EXISTS loci (
    locus_id VARCHAR PRIMARY KEY,
    study_id VARCHAR NOT NULL REFERENCES studies(study_id),
    locus_name VARCHAR,
    chromosome VARCHAR NOT NULL,
    start_position BIGINT NOT NULL,
    end_position BIGINT NOT NULL,
    lead_variant_id VARCHAR,
    lead_rsid VARCHAR,
    lead_pvalue DOUBLE,
    locus_source VARCHAR NOT NULL DEFAULT 'curated',
    n_signals INTEGER DEFAULT 1,
    n_candidate_genes INTEGER DEFAULT 0
)
"""

# Auto-increment via sequences — works with IF NOT EXISTS in both
# DuckDB and PostgreSQL.
LOCUS_GENE_EVIDENCE_SEQ = (
    "CREATE SEQUENCE IF NOT EXISTS seq_locus_gene_evidence START 1"
)
LOCUS_GENE_EVIDENCE_DDL = """
CREATE TABLE IF NOT EXISTS locus_gene_evidence (
    id INTEGER PRIMARY KEY DEFAULT nextval('seq_locus_gene_evidence'),
    locus_id VARCHAR NOT NULL REFERENCES loci(locus_id),
    gene_symbol VARCHAR NOT NULL,
    evidence_category VARCHAR NOT NULL,
    evidence_stream VARCHAR NOT NULL DEFAULT '',
    source_tag VARCHAR NOT NULL,
    pvalue DOUBLE,
    effect_size DOUBLE,
    score DOUBLE,
    tissue VARCHAR,
    cell_type VARCHAR,
    is_supporting BOOLEAN,
    metadata JSON,
    UNIQUE (locus_id, gene_symbol, evidence_category, evidence_stream, source_tag)
)
"""

GENE_EVIDENCE_SEQ = "CREATE SEQUENCE IF NOT EXISTS seq_gene_evidence START 1"
GENE_EVIDENCE_DDL = """
CREATE TABLE IF NOT EXISTS gene_evidence (
    id INTEGER PRIMARY KEY DEFAULT nextval('seq_gene_evidence'),
    gene_symbol VARCHAR NOT NULL,
    evidence_category VARCHAR NOT NULL,
    evidence_type VARCHAR NOT NULL,
    source_tag VARCHAR NOT NULL,
    trait VARCHAR NOT NULL DEFAULT '',
    score DOUBLE,
    tissue VARCHAR,
    cell_type VARCHAR,
    metadata JSON,
    UNIQUE (gene_symbol, evidence_category, evidence_type, source_tag, trait)
)
"""

LOCUS_GENE_SCORES_DDL = """
CREATE TABLE IF NOT EXISTS locus_gene_scores (
    locus_id VARCHAR NOT NULL,
    gene_symbol VARCHAR NOT NULL,
    distance_to_lead_kb DOUBLE,
    is_nearest_gene BOOLEAN DEFAULT FALSE,
    is_within_locus BOOLEAN DEFAULT FALSE,
    integration_method VARCHAR,
    integration_score DOUBLE,
    integration_rank INTEGER,
    is_predicted_effector BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (locus_id, gene_symbol)
)
"""

DATA_SOURCES_DDL = """
CREATE TABLE IF NOT EXISTS data_sources (
    source_tag VARCHAR PRIMARY KEY,
    source_name VARCHAR NOT NULL,
    source_type VARCHAR,
    evidence_category VARCHAR,
    is_integrated BOOLEAN DEFAULT FALSE,
    version VARCHAR,
    url VARCHAR,
    citation VARCHAR,
    date_imported TIMESTAMP DEFAULT current_timestamp,
    record_count INTEGER,
    metadata JSON
)
"""

# All PEGASUS DDL in dependency order (sequences before tables, foreign keys respected).
PEGASUS_DDL = [
    GENES_DDL,
    VARIANTS_DDL,
    STUDIES_DDL,
    LOCI_DDL,
    LOCUS_GENE_EVIDENCE_SEQ,
    LOCUS_GENE_EVIDENCE_DDL,
    GENE_EVIDENCE_SEQ,
    GENE_EVIDENCE_DDL,
    LOCUS_GENE_SCORES_DDL,
    DATA_SOURCES_DDL,
]


def create_pegasus_schema(conn: Any) -> None:
    """Create all PEGASUS evidence model tables if they don't exist."""
    for ddl in PEGASUS_DDL:
        conn.execute(ddl)
    if is_postgres(conn):
        conn.commit()
