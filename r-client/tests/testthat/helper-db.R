# Test fixture: in-memory DuckDB with PEGASUS schema and sample data.
# This file is auto-loaded by testthat before any test file runs.

library(DBI)
library(duckdb)

#' Create a v2f_connection wrapping an in-memory DuckDB with test data
#' @return A v2f_connection object
create_test_db <- function() {
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")

  # -- Core tables --
  DBI::dbExecute(conn, "
    CREATE TABLE _pegasus_meta (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  ")

  DBI::dbExecute(conn, "
    CREATE TABLE gene_search_index (
      ensembl_gene_id TEXT PRIMARY KEY,
      gene TEXT,
      chromosome TEXT,
      start_position INTEGER,
      end_position INTEGER,
      searchable_text TEXT
    )
  ")

  DBI::dbExecute(conn, "
    CREATE TABLE source_metadata (
      table_name TEXT PRIMARY KEY,
      display_name TEXT,
      description TEXT,
      data_type TEXT,
      source_type TEXT,
      gene_column TEXT,
      unique_per_gene BOOLEAN,
      include_in_search BOOLEAN,
      last_updated TIMESTAMP
    )
  ")

  # -- PEGASUS tables --
  DBI::dbExecute(conn, "
    CREATE TABLE genes (
      gene_symbol VARCHAR PRIMARY KEY,
      ensembl_gene_id VARCHAR UNIQUE,
      gene_name VARCHAR,
      chromosome VARCHAR,
      start_position BIGINT,
      end_position BIGINT,
      strand INTEGER,
      genome_build VARCHAR DEFAULT 'GRCh38'
    )
  ")

  DBI::dbExecute(conn, "
    CREATE TABLE studies (
      study_id VARCHAR PRIMARY KEY,
      trait VARCHAR NOT NULL,
      trait_description VARCHAR,
      trait_ontology_id VARCHAR,
      gwas_source VARCHAR,
      ancestry VARCHAR,
      sample_size INTEGER,
      n_loci INTEGER DEFAULT 0
    )
  ")

  DBI::dbExecute(conn, "
    CREATE TABLE loci (
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
  ")

  DBI::dbExecute(conn, "CREATE SEQUENCE IF NOT EXISTS seq_locus_gene_evidence START 1")
  DBI::dbExecute(conn, "
    CREATE TABLE locus_gene_evidence (
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
      metadata VARCHAR,
      UNIQUE (locus_id, gene_symbol, evidence_category, evidence_stream, source_tag)
    )
  ")

  DBI::dbExecute(conn, "CREATE SEQUENCE IF NOT EXISTS seq_gene_evidence START 1")
  DBI::dbExecute(conn, "
    CREATE TABLE gene_evidence (
      id INTEGER PRIMARY KEY DEFAULT nextval('seq_gene_evidence'),
      gene_symbol VARCHAR NOT NULL,
      evidence_category VARCHAR NOT NULL,
      evidence_type VARCHAR NOT NULL,
      source_tag VARCHAR NOT NULL,
      trait VARCHAR NOT NULL DEFAULT '',
      score DOUBLE,
      tissue VARCHAR,
      cell_type VARCHAR,
      metadata VARCHAR,
      UNIQUE (gene_symbol, evidence_category, evidence_type, source_tag, trait)
    )
  ")

  DBI::dbExecute(conn, "
    CREATE TABLE locus_gene_scores (
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
  ")

  DBI::dbExecute(conn, "
    CREATE TABLE data_sources (
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
      metadata VARCHAR
    )
  ")

  # -- Insert fixture data --

  # Meta
  DBI::dbExecute(conn, "INSERT INTO _pegasus_meta (key, value) VALUES
    ('genome_build', 'GRCh38'),
    ('package_version', '0.1.0'),
    ('build_timestamp', '2024-01-15T12:00:00Z'),
    ('config', 'name: test_lung_v2f\ngenome_build: hg38')
  ")

  # Genes
  DBI::dbExecute(conn, "INSERT INTO genes VALUES
    ('HHIP', 'ENSG00000164161', 'Hedgehog Interacting Protein', 'chr4', 145567058, 145662865, -1, 'GRCh38'),
    ('FAM13A', 'ENSG00000138640', 'Family With Sequence Similarity 13 Member A', 'chr4', 89618056, 89852980, 1, 'GRCh38'),
    ('GSTCD', 'ENSG00000148634', 'Glutathione S-Transferase C-Terminal Domain', 'chr4', 106530476, 106630826, -1, 'GRCh38'),
    ('AGER', 'ENSG00000204305', 'Advanced Glycosylation End-Product Specific Receptor', 'chr6', 32180466, 32184252, -1, 'GRCh38'),
    ('HTR4', 'ENSG00000164270', 'Serotonin Receptor 4', 'chr5', 148405613, 148633843, 1, 'GRCh38')
  ")

  # Gene search index
  DBI::dbExecute(conn, "INSERT INTO gene_search_index VALUES
    ('ENSG00000164161', 'HHIP', 'chr4', 145567058, 145662865, 'HHIP Hedgehog Interacting Protein'),
    ('ENSG00000138640', 'FAM13A', 'chr4', 89618056, 89852980, 'FAM13A Family With Sequence Similarity 13 Member A'),
    ('ENSG00000148634', 'GSTCD', 'chr4', 106530476, 106630826, 'GSTCD Glutathione S-Transferase C-Terminal Domain'),
    ('ENSG00000204305', 'AGER', 'chr6', 32180466, 32184252, 'AGER Advanced Glycosylation End-Product Specific Receptor'),
    ('ENSG00000164270', 'HTR4', 'chr5', 148405613, 148633843, 'HTR4 Serotonin Receptor 4')
  ")

  # Studies
  DBI::dbExecute(conn, "INSERT INTO studies VALUES
    ('study_001', 'FEV1', 'Forced Expiratory Volume in 1 second', NULL, 'UK Biobank', 'European', 320000, 2),
    ('study_002', 'FVC', 'Forced Vital Capacity', NULL, 'SpiroMeta', 'European', 280000, 1)
  ")

  # Loci
  DBI::dbExecute(conn, "INSERT INTO loci VALUES
    ('locus_001', 'study_001', 'chr4_HHIP', 'chr4', 145500000, 145700000, '4:145567058:C:T', 'rs12504628', 1e-50, 'curated', 1, 2),
    ('locus_002', 'study_001', 'chr4_GSTCD', 'chr4', 106500000, 106700000, '4:106530476:G:A', 'rs10516526', 1e-20, 'curated', 1, 1),
    ('locus_003', 'study_002', 'chr6_AGER', 'chr6', 32100000, 32200000, '6:32180466:A:G', 'rs2070600', 1e-30, 'curated', 1, 2)
  ")

  # Locus-gene evidence (~12 rows)
  DBI::dbExecute(conn, "INSERT INTO locus_gene_evidence
    (locus_id, gene_symbol, evidence_category, evidence_stream, source_tag, pvalue, score, tissue) VALUES
    ('locus_001', 'HHIP', 'QTL', 'eQTL', 'source_eqtl', 1e-8, 0.95, 'Lung'),
    ('locus_001', 'HHIP', 'COLOC', 'eQTL_coloc', 'source_coloc', NULL, 0.89, 'Lung'),
    ('locus_001', 'HHIP', 'PROX', 'nearest', 'source_proximity', NULL, 1.0, NULL),
    ('locus_001', 'FAM13A', 'QTL', 'eQTL', 'source_eqtl', 1e-4, 0.60, 'Lung'),
    ('locus_001', 'FAM13A', 'PROX', 'within', 'source_proximity', NULL, 0.5, NULL),
    ('locus_002', 'GSTCD', 'QTL', 'eQTL', 'source_eqtl', 1e-6, 0.80, 'Lung'),
    ('locus_002', 'GSTCD', 'COLOC', 'eQTL_coloc', 'source_coloc', NULL, 0.75, 'Lung'),
    ('locus_003', 'AGER', 'QTL', 'pQTL', 'source_eqtl', 1e-10, 0.92, 'Blood'),
    ('locus_003', 'AGER', 'PROX', 'nearest', 'source_proximity', NULL, 1.0, NULL),
    ('locus_003', 'HTR4', 'PROX', 'within', 'source_proximity', NULL, 0.3, NULL)
  ")

  # Gene-level evidence (~8 rows)
  DBI::dbExecute(conn, "INSERT INTO gene_evidence
    (gene_symbol, evidence_category, evidence_type, source_tag, score, tissue) VALUES
    ('HHIP', 'KNOW', 'lung_biology', 'source_literature', 0.90, NULL),
    ('HHIP', 'FUNC', 'mouse_model', 'source_functional', 0.85, 'Lung'),
    ('HHIP', 'LIT', 'pubmed_hits', 'source_literature', 0.70, NULL),
    ('FAM13A', 'KNOW', 'lung_biology', 'source_literature', 0.60, NULL),
    ('GSTCD', 'EXP', 'lung_expression', 'source_expression', 0.55, 'Lung'),
    ('AGER', 'DRUG', 'druggable_target', 'source_druggability', 0.80, NULL),
    ('HTR4', 'LIT', 'pubmed_hits', 'source_literature', 0.40, NULL),
    ('HTR4', 'FUNC', 'airway_model', 'source_functional', 0.50, 'Airway')
  ")

  # Locus-gene scores
  DBI::dbExecute(conn, "INSERT INTO locus_gene_scores VALUES
    ('locus_001', 'HHIP', 5.2, TRUE, TRUE, 'weighted_sum', 0.95, 1, TRUE),
    ('locus_001', 'FAM13A', 120.5, FALSE, FALSE, 'weighted_sum', 0.55, 2, FALSE),
    ('locus_002', 'GSTCD', 0.0, TRUE, TRUE, 'weighted_sum', 0.80, 1, TRUE),
    ('locus_003', 'AGER', 2.1, TRUE, TRUE, 'weighted_sum', 0.92, 1, TRUE),
    ('locus_003', 'HTR4', 85.0, FALSE, FALSE, 'weighted_sum', 0.30, 2, FALSE)
  ")

  # Data sources
  DBI::dbExecute(conn, "INSERT INTO data_sources
    (source_tag, source_name, source_type, evidence_category, is_integrated, version, record_count) VALUES
    ('source_eqtl', 'GTEx v8 eQTL', 'eqtl', 'QTL', TRUE, 'v8', 150),
    ('source_coloc', 'GTEx v8 Colocalization', 'coloc', 'COLOC', TRUE, 'v8', 80)
  ")

  # Wrap as v2f_connection
  structure(
    list(conn = conn, db_path = ":memory:", backend = "duckdb"),
    class = "v2f_connection"
  )
}

#' Create a minimal v2f_connection with only core tables (no PEGASUS)
#' @return A v2f_connection object
create_legacy_test_db <- function() {
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")

  DBI::dbExecute(conn, "
    CREATE TABLE gene_search_index (
      ensembl_gene_id TEXT PRIMARY KEY,
      gene TEXT,
      chromosome TEXT,
      start_position INTEGER,
      end_position INTEGER,
      searchable_text TEXT
    )
  ")

  DBI::dbExecute(conn, "INSERT INTO gene_search_index VALUES
    ('ENSG00000164161', 'HHIP', 'chr4', 145567058, 145662865, 'HHIP Hedgehog Interacting Protein')
  ")

  DBI::dbExecute(conn, "
    CREATE TABLE source_metadata (
      table_name TEXT PRIMARY KEY,
      display_name TEXT,
      description TEXT,
      data_type TEXT,
      source_type TEXT,
      gene_column TEXT,
      unique_per_gene BOOLEAN,
      include_in_search BOOLEAN,
      last_updated TIMESTAMP
    )
  ")

  DBI::dbExecute(conn, "INSERT INTO source_metadata VALUES
    ('eqtl_data', 'eQTL Data', 'eQTL associations', 'eqtl', 'googlesheets', 'gene', TRUE, TRUE, NULL)
  ")

  structure(
    list(conn = conn, db_path = ":memory:", backend = "duckdb"),
    class = "v2f_connection"
  )
}
