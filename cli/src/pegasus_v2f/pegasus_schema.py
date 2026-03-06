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
    genome_build VARCHAR DEFAULT 'hg38'
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
    genome_build VARCHAR DEFAULT 'hg38'
)
"""

STUDIES_DDL = """
CREATE TABLE IF NOT EXISTS studies (
    study_id VARCHAR PRIMARY KEY,
    study_name VARCHAR NOT NULL,
    trait VARCHAR NOT NULL,
    trait_description VARCHAR,
    trait_ontology_id VARCHAR,
    study_description VARCHAR,
    gwas_source VARCHAR,
    ancestry VARCHAR,
    sex VARCHAR,
    sample_size INTEGER,
    doi VARCHAR,
    year INTEGER,
    genome_build VARCHAR DEFAULT 'hg38',
    n_loci INTEGER DEFAULT 0
)
"""

LOCI_DDL = """
CREATE TABLE IF NOT EXISTS loci (
    locus_id VARCHAR PRIMARY KEY,
    study_id VARCHAR NOT NULL REFERENCES studies(study_id),
    trait VARCHAR,
    locus_name VARCHAR,
    chromosome VARCHAR NOT NULL,
    start_position BIGINT NOT NULL,
    end_position BIGINT NOT NULL,
    lead_variant_id VARCHAR,
    lead_rsid VARCHAR,
    lead_pvalue VARCHAR,
    nearest_gene VARCHAR,
    locus_source VARCHAR NOT NULL DEFAULT 'curated',
    n_signals INTEGER DEFAULT 1,
    n_candidate_genes INTEGER DEFAULT 0
)
"""

EVIDENCE_SEQ = "CREATE SEQUENCE IF NOT EXISTS seq_evidence START 1"
EVIDENCE_DDL = """
CREATE TABLE IF NOT EXISTS evidence (
    evidence_id INTEGER PRIMARY KEY DEFAULT nextval('seq_evidence'),
    gene_symbol VARCHAR NOT NULL,
    chromosome VARCHAR,
    position BIGINT,
    rsid VARCHAR,
    evidence_category VARCHAR NOT NULL,
    source_tag VARCHAR NOT NULL,
    trait VARCHAR,
    pvalue DOUBLE,
    effect_size DOUBLE,
    score DOUBLE,
    tissue VARCHAR,
    cell_type VARCHAR,
    ancestry VARCHAR,
    sex VARCHAR,
    evidence_stream VARCHAR,
    is_supporting BOOLEAN
)
"""

EVIDENCE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_evidence_source_tag ON evidence (source_tag)",
    "CREATE INDEX IF NOT EXISTS idx_evidence_gene_symbol ON evidence (gene_symbol)",
    "CREATE INDEX IF NOT EXISTS idx_evidence_chr_pos ON evidence (chromosome, position)",
]

SCORED_EVIDENCE_DDL = """
CREATE TABLE IF NOT EXISTS scored_evidence (
    locus_id VARCHAR NOT NULL,
    study_id VARCHAR NOT NULL,
    gene_symbol VARCHAR NOT NULL,
    evidence_category VARCHAR,
    source_tag VARCHAR,
    trait VARCHAR,
    pvalue DOUBLE,
    effect_size DOUBLE,
    score DOUBLE,
    tissue VARCHAR,
    cell_type VARCHAR,
    rsid VARCHAR,
    ancestry VARCHAR,
    sex VARCHAR,
    match_type VARCHAR,
    integration_rank INTEGER,
    is_predicted_effector BOOLEAN,
    n_candidate_genes INTEGER
)
"""

SCORED_EVIDENCE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_scored_locus_gene ON scored_evidence (locus_id, gene_symbol)",
    "CREATE INDEX IF NOT EXISTS idx_scored_study ON scored_evidence (study_id)",
    "CREATE INDEX IF NOT EXISTS idx_scored_gene ON scored_evidence (gene_symbol)",
]

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
    EVIDENCE_SEQ,
    EVIDENCE_DDL,
    *EVIDENCE_INDEXES,
    SCORED_EVIDENCE_DDL,
    *SCORED_EVIDENCE_INDEXES,
    DATA_SOURCES_DDL,
]


def create_pegasus_schema(conn: Any) -> None:
    """Create all PEGASUS evidence model tables if they don't exist."""
    for ddl in PEGASUS_DDL:
        conn.execute(ddl)
    if is_postgres(conn):
        conn.commit()
