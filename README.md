# PEGASUS V2F

Gene prioritization pipeline and database engine implementing the [PEGASUS standard](https://www.ebi.ac.uk/spot/pegasus/) (EBI SPOT). Loads heterogeneous genomic data sources, maps them to a GWAS-anchored evidence model, scores candidate genes at each locus, and exports standardized deliverables.

## Monorepo structure

```
pegasus-v2f/
  cli/        Python CLI and pipeline engine
  api/        FastAPI REST API server (serves the UI)
  r-client/   R package for analysts (planned)
  ui/         React frontend (planned)
  schema/     PEGASUS schema reference (planned)
```

Managed as a [uv workspace](https://docs.astral.sh/uv/concepts/workspaces/) with a single lockfile. The CLI package is the engine; all other components depend on it.

## Components

### `cli/` — Pipeline engine

The `v2f` command-line tool. Builds databases from a declarative `v2f.yaml` config, manages data sources, runs PEGASUS scoring, and exports results. Supports DuckDB (local) and PostgreSQL (shared) backends.

Install: `uv pip install -e cli/`

See [cli/README.md](cli/README.md) for commands, config format, and usage examples.

### `api/` — REST API server

FastAPI application serving the database over HTTP. Provides gene search, evidence queries, GWAS trait/locus browsing, and data import endpoints. Also serves the React UI from `ui/` as static files.

Install: `uv pip install -e api/`

See [api/README.md](api/README.md) for endpoints and usage.

### `r-client/` — R package (planned)

Domain-first R package for analysts. Thin read-only interface returning data frames — no SQL required. Functions like `search_genes()`, `gene_evidence()`, `trait_genes()`.

See [r-client/README.md](r-client/README.md).

### `ui/` — React frontend (planned)

Web interface for browsing genes, evidence, and PEGASUS results. Served by the API server.

See [ui/README.md](ui/README.md).

### `schema/` — Schema reference (planned)

Documentation of the PEGASUS database schema and controlled vocabularies.

See [schema/README.md](schema/README.md).

## Quick start

```bash
# Clone and install
git clone https://github.com/LungGenomics/pegasus-v2f.git && cd pegasus-v2f
uv pip install -e cli/ -e api/

# Initialize a project
mkdir my-study && cd my-study
v2f init

# Edit v2f.yaml to declare your GWAS study, loci, and data sources
# Then build:
v2f build

# Explore
v2f tables
v2f query "SELECT * FROM locus_gene_scores ORDER BY integration_rank LIMIT 10"

# Export PEGASUS deliverables
v2f export pegasus my_study_fev1 -o pegasus-output/

# Start the web server
v2f serve
```

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) for package management

## Development

```bash
# Install with dev dependencies
uv pip install -e "cli/[dev]" -e api/

# Run tests
cd cli && uv run pytest -v

# Run API server with auto-reload
v2f serve --reload
```
