# PEGASUS V2F — CLI

Command-line tool and pipeline engine for building PEGASUS gene prioritization databases. Loads heterogeneous data sources (files, Google Sheets, Excel, URLs), maps them into a GWAS-anchored evidence model, scores candidate genes, and exports standardized deliverables.

## Install

```bash
uv pip install -e cli/
```

This installs the `v2f` command on your PATH. Includes DuckDB, PostgreSQL, and Google Sheets support out of the box.

## Features

### GWAS-anchored evidence model

The database is organized around GWAS studies. A study defines traits, traits define loci (genomic regions with significant associations), and loci contain candidate genes with supporting evidence. This mirrors the PEGASUS standard from EBI SPOT.

**Database tables:** `studies`, `loci`, `genes`, `variants`, `locus_gene_evidence`, `gene_evidence`, `locus_gene_scores`, `data_sources`

### Multi-source data loading

Sources are declared in `v2f.yaml` and can be:
- **Local files** (TSV, CSV, gzipped)
- **Excel workbooks** (specific sheets)
- **Google Sheets** (by spreadsheet ID)
- **URLs** (remote TSV/CSV)

Each source can have inline transformations (rename columns, filter rows, compute derived values) applied before loading.

### Evidence routing

Sources with an `evidence:` block are routed into the PEGASUS evidence model instead of being loaded as raw tables:

| Evidence type | Target table | Use case |
|---|---|---|
| `role: locus_definition` | studies + loci | Curated GWAS loci from manual curation |
| `role: gwas_sumstats` | variants + loci | Auto-clumped loci from summary statistics |
| `centric: gene` | gene_evidence | Gene-level annotations (expression, protein data, etc.) |
| `centric: variant` | locus_gene_evidence | Variant-level evidence matched to loci by position |

Sources without an `evidence:` block load as raw exploration tables.

### Integration scoring

After evidence is loaded, the scoring engine evaluates each locus-gene pair:
- Distance to lead variant
- Nearest gene detection
- Configurable criteria counting (QTL signals, colocalization, expression, etc.)
- Ranking within each locus
- Predicted effector gene flagging

Scoring runs automatically when sources are added and can be re-run with `v2f materialize`.

### Integration wizard

Raw exploration tables can be interactively mapped to PEGASUS evidence categories:

```bash
v2f integrate my_raw_source
```

The wizard detects column names and types, suggests mappings, and writes the `evidence:` block into `v2f.yaml`. The raw table is then re-loaded through evidence routing, scored, and dropped.

### PEGASUS export

Exports the three standard PEGASUS deliverables:
- **Evidence Matrix** (TSV) — locus-gene evidence pivot table
- **Metadata** (YAML) — study, loci, genes, sources, criteria, scores
- **PEG List** (TSV) — rank-1 predicted effector genes per locus

### Dual database backends

DuckDB for local development (zero-config, single file). PostgreSQL for shared/deployed databases. The same CLI commands work with both.

### Config layering

Three config layers merge in order:
1. `v2f.yaml` — shared config (tracked in git)
2. `.v2f/local.yaml` — local overrides (gitignored)
3. Environment variables / CLI flags — final overrides

### Embedded config

The resolved config is stored inside the database (`_pegasus_meta` table), making databases self-describing. You can rebuild a database from itself with `v2f build --from-db existing.duckdb`.

## Commands

### Project management

```bash
v2f init                     # Initialize a new project (creates v2f.yaml)
v2f init <git-url>           # Initialize from a git repo (clone + init)
v2f status                   # Show project, database, and sync status
v2f sync                     # Pull latest config from remote
v2f sync --push -m "msg"     # Push config changes
v2f sync --build             # Pull and auto-rebuild
```

### Build and sources

```bash
v2f build                    # Build database from v2f.yaml
v2f build --overwrite        # Drop and rebuild
v2f build --from-db old.db   # Rebuild using config from existing DB

v2f add-source <name> \
  --type file \
  --path data/my_data.tsv    # Add a data source
v2f add-source <name> \
  --type googlesheets \
  --url <spreadsheet-url>    # Add from Google Sheets
v2f add-source <name> \
  --no-score                 # Add without auto-scoring (batch mode)

v2f update-source <name>     # Re-fetch and reload a source
v2f remove-source <name>     # Drop a source (cleans evidence + re-scores)
v2f sources                  # List all data sources
```

### Evidence and scoring

```bash
v2f materialize              # Re-run integration scoring

v2f integrate <source>       # Interactive: map raw table to evidence
v2f integrate <source> \
  --category COLOC \
  --centric variant \
  --source-tag my_coloc      # Non-interactive mode
```

### Query and inspect

```bash
v2f tables                   # List tables with row counts
v2f query "SELECT ..."       # Run ad-hoc SQL
v2f query "SELECT ..." \
  --format csv               # Output as CSV (also: json, table)
```

### Config management

```bash
v2f config show              # Show resolved config (all layers merged)
v2f config edit              # Open v2f.yaml in $EDITOR
v2f config edit --local      # Open .v2f/local.yaml
v2f config diff              # Show what local.yaml overrides
v2f config validate          # Check config structure
```

### Export

```bash
v2f export csv <table>       # Export a table as CSV
v2f export csv <table> \
  -o output.csv              # Write to file

v2f export pegasus <study_id>          # Export PEGASUS deliverables
v2f export pegasus <study_id> -o dir/  # Write to specific directory
```

### Server

```bash
v2f serve                    # Start API server on localhost:8000
v2f serve --port 3000        # Custom port
v2f serve --reload           # Auto-reload for development
```

### Global flags

```bash
v2f --db path/to/db.duckdb <command>   # Override database path
v2f --db postgresql://... <command>     # Use PostgreSQL
v2f --project /path/to/project <cmd>   # Override project root
v2f --json <command>                    # JSON output
v2f --quiet <command>                   # Suppress progress
v2f --version                           # Show version
```

## Config format

### Minimal config (raw sources only)

```yaml
version: 1

database:
  backend: duckdb
  name: gene.duckdb

data_sources:
  - name: my_genes
    source_type: file
    path: data/genes.tsv
```

### Full PEGASUS config

```yaml
version: 1

pegasus:
  study:
    id_prefix: my_study_2024
    gwas_source: "PMID:12345678"
    ancestry: European
    genome_build: GRCh38
    traits:
      - FEV1
      - FVC

  locus_definition:
    window_kb: 1000
    merge_distance_kb: 500

  integration:
    method: criteria_count_v1
    effector_threshold: 0.25
    criteria:
      - name: nearest_gene
        type: computed
      - name: coloc
        category: COLOC
        threshold_field: score
        threshold: 0.8

database:
  backend: duckdb
  name: gene.duckdb

data_sources:
  # Curated loci from manual curation
  - name: trait_loci
    source_type: googlesheets
    url: https://docs.google.com/spreadsheets/d/...
    evidence:
      role: locus_definition
      source_tag: my_study_2024
      fields:
        gene: gene
        trait: trait
        chromosome: chr
        position: pos
        pvalue: minP

  # Gene-level annotation
  - name: secretome
    source_type: file
    path: data/raw/secretome.tsv.gz
    evidence:
      category: KNOW
      centric: gene
      evidence_type: secretome
      source_tag: hpa_secretome
      fields:
        gene: Gene

  # Variant-level evidence
  - name: coloc_results
    source_type: excel
    path: data/raw/coloc.xlsx
    evidence:
      category: COLOC
      centric: variant
      source_tag: coloc_v1
      fields:
        gene: gene
        chromosome: chr
        position: pos

  # Raw table (exploration, not scored)
  - name: extra_data
    source_type: file
    path: data/raw/extra.tsv
```

## Tutorial

### 1. Set up a new project

```bash
mkdir lung-gwas && cd lung-gwas
v2f init
```

This creates `v2f.yaml` (config), `.v2f/` (local state), and `.gitignore`.

### 2. Add data sources

Load your data as raw tables first — no YAML editing needed:

```bash
# From a file
v2f add-source secretome --type file --path data/raw/secretome.tsv.gz

# From Google Sheets
v2f add-source trait_loci --type googlesheets --url "https://docs.google.com/..."

# From Excel
v2f add-source coloc_results --type excel --path data/raw/coloc.xlsx
```

### 3. Inspect what you loaded

```bash
v2f tables
v2f query "SELECT * FROM secretome LIMIT 5"
```

### 4. Map sources to PEGASUS evidence with the wizard

The integration wizard walks you through mapping each raw table to the PEGASUS evidence model interactively:

```bash
v2f integrate secretome
```

The wizard will:
1. Show the table's columns, types, and sample values
2. Auto-suggest a PEGASUS category and field mappings based on column names
3. Let you confirm or adjust each mapping
4. Write the `evidence:` block into `v2f.yaml` for you
5. Re-load through evidence routing, drop the raw table, and score

Repeat for each source. Or if you prefer non-interactive mode:

```bash
v2f integrate secretome --category KNOW --centric gene --source-tag hpa_secretome
```

You can also edit `v2f.yaml` directly for more granular control — useful for advanced configuration or when working with an AI assistant that can generate the evidence mappings for you. See the [config format](#config-format) section below for the full schema.

### 5. Build the database

```bash
v2f build
```

This loads all sources, routes evidence, annotates genes, runs scoring, and creates the search index.

### 6. Explore results

```bash
# What tables do we have?
v2f tables

# Top-scoring genes
v2f query "SELECT * FROM locus_gene_scores ORDER BY integration_rank LIMIT 20"

# Evidence for a specific gene
v2f query "SELECT * FROM locus_gene_evidence WHERE gene_symbol = 'HHIP'"

# All predicted effector genes
v2f query "SELECT * FROM locus_gene_scores WHERE is_predicted_effector = TRUE"
```

### 7. Export PEGASUS deliverables

```bash
v2f export pegasus my_study_2024_fev1 -o pegasus-output/
```

Produces three files:
- `evidence_matrix.tsv` — pivot table of all evidence per locus-gene pair
- `metadata.yaml` — study, loci, genes, sources, criteria, scores
- `peg_list.tsv` — rank-1 predicted effector gene per locus

### 8. Serve the web UI

```bash
v2f serve
# Open http://localhost:8000
```

### 9. Batch workflows

When adding many sources, skip auto-scoring with `--no-score` and score once at the end:

```bash
v2f add-source source1 --type file --path data/s1.tsv --no-score
v2f add-source source2 --type file --path data/s2.tsv --no-score
v2f add-source source3 --type file --path data/s3.tsv --no-score
v2f materialize   # Score all at once
```

### 10. Share via git

The project directory (with `v2f.yaml` and data files) can be shared via git. Team members clone and build their own local database:

```bash
git clone <project-repo> && cd <project>
v2f init
v2f build
```

Or use sync for ongoing collaboration:

```bash
v2f sync          # Pull latest config
v2f sync --build  # Pull and rebuild
```

## Testing

```bash
cd cli
uv run pytest -v          # Run all tests
uv run pytest -k scoring  # Run scoring tests only
```
