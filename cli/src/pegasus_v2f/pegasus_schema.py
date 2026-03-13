"""PEGASUS schema — DDL for evidence model tables and controlled vocabulary."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pegasus_v2f.db import is_postgres

# -- PEGASUS controlled vocabulary --
# 22 evidence categories from the real PEGASUS standard.
# https://ebispot.github.io/PEGASUS/docs/peg-evidence


@dataclass
class CategoryProfile:
    """Rich profile for a PEGASUS evidence category."""

    abbrev: str
    label: str
    description: str
    centric_group: str  # "variant", "gene", "both"
    column_hints: list[str] = field(default_factory=list)
    source_name_hints: list[str] = field(default_factory=list)
    typical_value: str = "score"  # "pvalue", "score", "presence", "effect_size"


# All 22 real PEGASUS categories organized by centric group.
EVIDENCE_CATEGORY_PROFILES: dict[str, CategoryProfile] = {
    # --- Variant-centric (10) ---
    "LD": CategoryProfile(
        "LD", "Linkage Disequilibrium",
        "Variant is correlated with another variant, may act as proxy",
        "variant",
        column_hints=["ld", "r2", "d_prime", "proxy"],
        source_name_hints=["ld", "proxy"],
        typical_value="score",
    ),
    "FM": CategoryProfile(
        "FM", "Finemapping / Credible Sets",
        "High posterior probability of causality through finemapping",
        "variant",
        column_hints=["pip", "credible", "finemap", "susie", "posterior"],
        source_name_hints=["finemap", "credible", "susie"],
        typical_value="score",
    ),
    "COLOC": CategoryProfile(
        "COLOC", "Colocalization",
        "Variant affects multiple traits at the same locus (shared causal variant)",
        "variant",
        column_hints=["pp.h4", "pph4", "posterior", "h4", "pip", "coloc"],
        source_name_hints=["coloc"],
        typical_value="score",
    ),
    "QTL": CategoryProfile(
        "QTL", "Molecular QTL",
        "eQTL, sQTL, pQTL — variant influences expression, splicing, or protein levels",
        "variant",
        column_hints=["eqtl", "sqtl", "pqtl", "grex", "qtl"],
        source_name_hints=["eqtl", "sqtl", "pqtl", "qtl"],
        typical_value="pvalue",
    ),
    "MR": CategoryProfile(
        "MR", "Mendelian Randomization",
        "Genetic variants as proxies for exposures to test causal effects",
        "variant",
        column_hints=["mr", "mendelian", "ivw", "wald_ratio"],
        source_name_hints=["mr", "mendelian"],
        typical_value="pvalue",
    ),
    "REG": CategoryProfile(
        "REG", "Regulatory Region",
        "Variant lies in open chromatin or enhancer/promoter in relevant tissue",
        "variant",
        column_hints=["atac", "dnase", "enhancer", "promoter", "open_chromatin", "epig"],
        source_name_hints=["atac", "dnase", "regulatory", "epigenomic"],
        typical_value="score",
    ),
    "3D": CategoryProfile(
        "3D", "Chromatin Interaction",
        "Variant physically contacts gene promoter via 3D chromatin structure",
        "variant",
        column_hints=["hic", "hi-c", "tad", "chromatin", "4c", "capture"],
        source_name_hints=["chromatin", "hic", "3d"],
        typical_value="score",
    ),
    "FUNC": CategoryProfile(
        "FUNC", "Predicted Functional Impact",
        "Computational prediction of disrupted gene/protein function or motifs",
        "variant",
        column_hints=["cadd", "sift", "polyphen", "coding", "missense", "lof"],
        source_name_hints=["functional", "coding", "missense", "cadd"],
        typical_value="score",
    ),
    "PROX": CategoryProfile(
        "PROX", "Proximity to Gene",
        "Variant location within or adjacent to gene boundaries",
        "variant",
        column_hints=["proximity", "nearest", "distance", "closest"],
        source_name_hints=["proximity", "nearest"],
        typical_value="score",
    ),
    "GWAS": CategoryProfile(
        "GWAS", "GWAS Association",
        "Statistical association p-value from GWAS",
        "variant",
        column_hints=["pvalue", "p_value", "min_p", "gwas"],
        source_name_hints=["gwas", "sumstat"],
        typical_value="pvalue",
    ),
    "PHEWAS": CategoryProfile(
        "PHEWAS", "Phenome-Wide Association",
        "Variant shows associations across multiple traits (pleiotropy)",
        "variant",
        column_hints=["phewas", "pleiotropy", "n_traits"],
        source_name_hints=["phewas"],
        typical_value="score",
    ),
    # --- Gene-centric (9) ---
    "PPI": CategoryProfile(
        "PPI", "Protein-Protein Interaction",
        "Gene's protein engages with disease-relevant proteins",
        "gene",
        column_hints=["ppi", "string", "interactor", "protein_interaction"],
        source_name_hints=["ppi", "string", "interactome"],
        typical_value="score",
    ),
    "SET": CategoryProfile(
        "SET", "Pathway or Gene Sets",
        "Gene in phenotype-relevant pathways or complexes",
        "gene",
        column_hints=["pathway", "kegg", "go", "reactome", "gene_set"],
        source_name_hints=["pathway", "network", "gene_set"],
        typical_value="presence",
    ),
    "GENEBASE": CategoryProfile(
        "GENEBASE", "Gene-based Association",
        "Aggregate variant-level association within gene",
        "gene",
        column_hints=["magma", "vegas", "skat", "gene_based", "gene_p"],
        source_name_hints=["magma", "gene_based"],
        typical_value="pvalue",
    ),
    "EXP": CategoryProfile(
        "EXP", "Expression",
        "Differential expression in relevant tissues or patient populations",
        "gene",
        column_hints=["log2fc", "fold_change", "p_val_adj", "expression", "rpkm", "tpm"],
        source_name_hints=["deg", "expression", "single_cell"],
        typical_value="pvalue",
    ),
    "PERTURB": CategoryProfile(
        "PERTURB", "Perturbation",
        "Knockouts, CRISPR, organoids show phenotype effects",
        "gene",
        column_hints=["crispr", "knockout", "perturbation", "screen", "organoid"],
        source_name_hints=["crispr", "perturbation", "screen", "knockout"],
        typical_value="score",
    ),
    "KNOW": CategoryProfile(
        "KNOW", "Biological Knowledge",
        "Inferred relationships based on known biology",
        "gene",
        column_hints=["secreted", "localization", "function", "biology"],
        source_name_hints=["secretome", "hpa", "uniprot"],
        typical_value="presence",
    ),
    "TPWAS": CategoryProfile(
        "TPWAS", "Genetically Predicted Trait",
        "TWAS or PWAS — expression/protein levels associated with phenotype",
        "gene",
        column_hints=["twas", "pwas", "tpwas", "genetically_predicted"],
        source_name_hints=["twas", "pwas", "tpwas"],
        typical_value="pvalue",
    ),
    "DRUG": CategoryProfile(
        "DRUG", "Drug Related",
        "Gene targeted by therapeutics for the phenotype",
        "gene",
        column_hints=["drug", "therapeutic", "target", "drugbank"],
        source_name_hints=["drug", "therapeutic", "drugbank"],
        typical_value="presence",
    ),
    # --- Variant or Gene-centric (3) ---
    "CROSSP": CategoryProfile(
        "CROSSP", "Cross-phenotype",
        "Evidence from biologically related phenotypes",
        "both",
        column_hints=["cross_phenotype", "related_trait", "crossp"],
        source_name_hints=["cross_phenotype", "related"],
        typical_value="score",
    ),
    "LIT": CategoryProfile(
        "LIT", "Literature Curation",
        "Human-curated gene or variant-disease links from literature",
        "both",
        column_hints=["literature", "curated", "pubmed", "citation"],
        source_name_hints=["literature", "curated"],
        typical_value="presence",
    ),
    "DB": CategoryProfile(
        "DB", "Curated Database",
        "Evidence from ClinVar, ClinGen, OMIM, etc.",
        "both",
        column_hints=["clinvar", "omim", "clingen", "orphanet"],
        source_name_hints=["clinvar", "omim", "clingen"],
        typical_value="presence",
    ),
}

# Derived abbreviation → label dict (used by validation, wizard, etc.)
EVIDENCE_CATEGORIES: dict[str, str] = {
    p.abbrev: p.label for p in EVIDENCE_CATEGORY_PROFILES.values()
}


# -- DDL for PEGASUS tables --

GENES_DDL = """
CREATE TABLE IF NOT EXISTS genes (
    gene_symbol VARCHAR PRIMARY KEY,
    ensembl_gene_id VARCHAR,
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
