# PEGASUS V2F — API

FastAPI server providing REST access to PEGASUS V2F databases. Serves gene search, evidence queries, GWAS trait/locus browsing, data import, and the React web UI.

## Install

```bash
uv pip install -e cli/ -e api/
```

The API package depends on the CLI package (shared database and config modules).

## Start the server

```bash
# Via the CLI (recommended)
v2f serve                          # localhost:8000
v2f serve --port 3000              # custom port
v2f serve --host 0.0.0.0           # bind to all interfaces
v2f serve --reload                 # auto-reload for development

# Or directly with uvicorn
uvicorn pegasus_v2f_api.app:app --reload
```

The server requires a built database. Run `v2f build` first.

## Features

### Gene search

Full-text search across the gene search index. Returns gene symbols, Ensembl IDs, evidence categories, scoring data, and locus associations.

### PEGASUS evidence browsing

Browse the evidence model hierarchically: studies/traits, loci within a study, and scored genes with evidence at each locus. Supports the full PEGASUS workflow from GWAS hits to prioritized effector genes.

### Data import

Import data sources via the API (Google Sheets fetch, direct data upload). Used by the web UI for interactive source management.

### Database queries

Execute ad-hoc SQL queries against the database for custom analysis.

## Endpoints

### Gene search

```
GET /genes?search=HHIP&limit=10
```

Search genes by symbol or name. Returns matching rows from `gene_search_index` with evidence categories, scores, and locus counts.

| Param | Default | Description |
|---|---|---|
| `search` | `""` | Search string (empty returns all) |
| `limit` | `100` | Max results |

### PEGASUS evidence

```
GET /evidence/{gene}
```

All evidence for a gene across both evidence tables. Each row includes an `evidence_level` field (`"locus"` or `"gene"`) indicating which table it came from.

Returns: `locus_id`, `gene_symbol`, `evidence_category`, `evidence_stream`, `source_tag`, `pvalue`, `effect_size`, `score`, `tissue`, `cell_type`, `evidence_level`.

```
GET /traits
```

List all GWAS studies/traits. Returns: `study_id`, `trait`, `trait_description`, `gwas_source`, `ancestry`, `sample_size`, `n_loci`.

```
GET /traits/{study_id}/loci
```

List loci for a study. Returns: `locus_id`, `locus_name`, `chromosome`, `start_position`, `end_position`, `lead_variant_id`, `lead_rsid`, `lead_pvalue`, `locus_source`, `n_candidate_genes`.

```
GET /loci/{locus_id}/genes
```

Evidence matrix for a single locus. Returns scored genes with nested evidence arrays. Each gene object includes:
- Score fields: `distance_to_lead_kb`, `is_nearest_gene`, `is_within_locus`, `integration_score`, `integration_rank`, `is_predicted_effector`
- `evidence`: array of `{evidence_category, evidence_stream, source_tag, pvalue, score}`

### Database

```
GET /tables
```

List all tables with row counts.

```
GET /sources
```

List data sources from stored config.

```
GET /config
```

Read stored config from database.

```
POST /db/query
Body: {"query": "SELECT ..."}
```

Execute an SQL query. Returns array of row objects.

```
POST /db/update_metadata
Body: {"table_name": "...", "description": "...", "display_name": "...", "data_type": "..."}
```

Update metadata for a data source.

```
POST /db/delete_table
Body: {"table_name": "..."}
```

Delete a table and remove from config.

### Data import

```
POST /import/fetch_google
Body: {"ss": "<spreadsheet_id>", "sheet": "<sheet_name>", "skip": 0}
```

Fetch data from a Google Sheet (preview, up to 100 rows).

```
POST /import/import_data
Body: {
  "name": "table_name",
  "source_type": "file",
  "display_name": "...",
  "description": "...",
  "gene_column": "gene",
  "include_in_search": false,
  "data": [{"col": "val"}, ...]
}
```

Import data into the database.

## Tutorial

### Browse GWAS results

```bash
# List studies
curl http://localhost:8000/traits | python -m json.tool

# List loci for a study
curl http://localhost:8000/traits/my_study_fev1/loci | python -m json.tool

# Get scored genes at a locus
curl http://localhost:8000/loci/HHIP_FEV1/genes | python -m json.tool
```

### Search genes

```bash
# Search by gene symbol
curl "http://localhost:8000/genes?search=HHIP" | python -m json.tool

# Get all evidence for a gene
curl http://localhost:8000/evidence/HHIP | python -m json.tool
```

### Query the database

```bash
curl -X POST http://localhost:8000/db/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM locus_gene_scores WHERE is_predicted_effector = TRUE"}'
```

## Architecture

```
api/src/pegasus_v2f_api/
  app.py      Application factory, CORS config, database connection
  routes.py   All REST endpoint definitions
  static.py   React UI static file serving
```

The API opens a read-only database connection on startup (path resolved from project config or `--db` flag). All endpoints query this shared connection.
